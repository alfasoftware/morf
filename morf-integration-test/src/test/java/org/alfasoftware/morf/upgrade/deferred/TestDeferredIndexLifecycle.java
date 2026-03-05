/* Copyright 2026 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationColumnTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.AddSecondDeferredIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * End-to-end lifecycle integration tests for the deferred index mechanism.
 * Exercises upgrade → restart → execute cycles through the real
 * {@link Upgrade#performUpgrade} path, verifying both Mode 1
 * (force-build on restart) and Mode 2 (background build) behaviour.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexLifecycle {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private UpgradeConfigAndContext upgradeConfigAndContext;

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      deferredIndexOperationTable(),
      deferredIndexOperationColumnTable(),
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );


  /** Create a fresh schema before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // =========================================================================
  // Happy path
  // =========================================================================

  /** Upgrade defers index, execute builds it, restart finds schema correct. */
  @Test
  public void testHappyPath_upgradeExecuteRestart() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));

    executeDeferred();
    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");

    // Restart — same steps, nothing new to do
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    // Should pass without error
  }


  // =========================================================================
  // Mode 1 — force build on restart (default)
  // =========================================================================

  /** Mode 1: restart without execute force-builds deferred indexes. */
  @Test
  public void testMode1_restartWithoutExecute_forceBuilds() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Restart without calling execute — Mode 1 should force-build
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    assertIndexExists("Product", "Product_Name_1");
  }


  /** Mode 1: crashed IN_PROGRESS ops are found and force-built on restart. */
  @Test
  public void testMode1_crashedOpsAreForceBuilt() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    setOperationStatus("Product_Name_1", "IN_PROGRESS");

    // Restart — Mode 1 should reset IN_PROGRESS → PENDING and force-build
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Mode 2 — background build
  // =========================================================================

  /** Mode 2: restart without execute passes schema check, index built later. */
  @Test
  public void testMode2_restartWithoutExecute_backgroundBuild() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Restart in Mode 2 — schema augmented, no force-build
    upgradeConfigAndContext.setForceDeferredIndexBuildOnRestart(false);
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    // Index should NOT exist yet — Mode 2 does not force-build
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Execute builds it in the background
    executeDeferred();
    assertIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
  }


  /** Mode 2: no-upgrade restart, execute picks up leftovers. */
  @Test
  public void testMode2_noUpgradeRestart_executeBuildsInBackground() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Restart in Mode 2
    upgradeConfigAndContext.setForceDeferredIndexBuildOnRestart(false);
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    // Execute picks up the pending op
    executeDeferred();
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Mode 2: crashed IN_PROGRESS ops are augmented in schema and built by execute. */
  @Test
  public void testMode2_crashedOpsBuiltInBackground() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    setOperationStatus("Product_Name_1", "IN_PROGRESS");

    // Restart in Mode 2 — schema augmented with IN_PROGRESS op
    upgradeConfigAndContext.setForceDeferredIndexBuildOnRestart(false);
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    // Execute resets IN_PROGRESS → PENDING and builds
    executeDeferred();
    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Crash recovery via executor
  // =========================================================================

  /** Executor resets IN_PROGRESS ops to PENDING and builds them. */
  @Test
  public void testCrashRecovery_inProgressResetToPending() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    setOperationStatus("Product_Name_1", "IN_PROGRESS");

    // Execute should reset and build
    executeDeferred();
    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Executor handles index already built before crash — marks COMPLETED. */
  @Test
  public void testCrashRecovery_indexAlreadyBuilt() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    // Simulate: DB finished building the index before the crash
    buildIndexManually("Product", "Product_Name_1", "name");
    setOperationStatus("Product_Name_1", "IN_PROGRESS");

    // Execute resets to PENDING, tries CREATE INDEX, fails (exists), marks COMPLETED
    executeDeferred();
    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Two sequential upgrades
  // =========================================================================

  /** Two upgrades, both executed — third restart passes. */
  @Test
  public void testTwoSequentialUpgrades() {
    // First upgrade
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    executeDeferred();
    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));

    // Second upgrade adds another deferred index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));
    executeDeferred();
    assertEquals("COMPLETED", queryOperationStatus("Product_IdName_1"));

    // Third restart — everything clean
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));
  }


  /** Two upgrades, first index not built — Mode 1 force-builds before second upgrade. */
  @Test
  public void testTwoUpgrades_firstIndexNotBuilt_mode1() {
    // First upgrade — don't execute
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Second upgrade (Mode 1) — readiness check should force-build first index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));
    assertIndexExists("Product", "Product_Name_1");

    // Execute builds second index
    executeDeferred();
    assertIndexExists("Product", "Product_IdName_1");
  }


  /** Two upgrades, first index not built — Mode 2 augments and builds both in background. */
  @Test
  public void testTwoUpgrades_firstIndexNotBuilt_mode2() {
    // First upgrade — don't execute
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Second upgrade (Mode 2) — schema augmented
    upgradeConfigAndContext.setForceDeferredIndexBuildOnRestart(false);
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));

    // Execute builds both
    executeDeferred();
    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  // =========================================================================
  // Helpers
  // =========================================================================

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> step) {
    performUpgradeWithSteps(targetSchema, Collections.singletonList(step));
  }


  private void performUpgradeWithSteps(Schema targetSchema,
                                        List<Class<? extends UpgradeStep>> steps) {
    Upgrade.performUpgrade(targetSchema, steps, connectionResources,
        upgradeConfigAndContext, viewDeploymentValidator);
  }


  private void executeDeferred() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    config.setMaxRetries(1);
    DeferredIndexOperationDAO dao = new DeferredIndexOperationDAOImpl(
        new SqlScriptExecutorProvider(connectionResources), connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        dao, connectionResources, new SqlScriptExecutorProvider(connectionResources),
        config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();
  }


  private Schema schemaWithFirstIndex() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
  }


  private Schema schemaWithBothIndexes() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );
  }


  private String queryOperationStatus(String indexName) {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("status"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("indexName").eq(indexName))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }


  private void setOperationStatus(String indexName, String status) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            update(tableRef(DEFERRED_INDEX_OPERATION_NAME))
                .set(literal(status).as("status"))
                .where(field("indexName").eq(indexName))
        )
    );
  }


  private void buildIndexManually(String tableName, String indexName, String columnName) {
    sqlScriptExecutorProvider.get().execute(
        List.of("CREATE INDEX " + indexName + " ON " + tableName + " (" + columnName + ")")
    );
  }


  private void assertIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private void assertIndexDoesNotExist(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("Index " + indexName + " should not exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
