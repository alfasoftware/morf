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
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;

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
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * End-to-end integration tests for the deferred index lifecycle using the
 * comments-based model. Exercises the full upgrade framework path: upgrade
 * step adds a deferred index (recorded in table comments), the executor
 * scans for unbuilt deferred indexes, and builds them.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexIntegration {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private final UpgradeConfigAndContext upgradeConfigAndContext = new UpgradeConfigAndContext();
  { upgradeConfigAndContext.setDeferredIndexCreationEnabled(true); }

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
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
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * After upgrade, the deferred index should appear in the schema as a
   * virtual index (isDeferred=true) but not yet physically built. After
   * the executor runs, the physical index should exist.
   */
  @Test
  public void testDeferredIndexLifecycle() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    assertDeferredIndexPending("Product", "Product_Name_1");

    executeDeferred();

    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * A deferred unique index should preserve the unique constraint
   * through the full pipeline.
   */
  @Test
  public void testDeferredUniqueIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredUniqueIndex.class);

    executeDeferred();

    assertPhysicalIndexExists("Product", "Product_Name_UQ");
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index should be unique",
          sr.getTable("Product").indexes().stream()
              .filter(idx -> "Product_Name_UQ".equalsIgnoreCase(idx.getName()))
              .findFirst().get().isUnique());
    }
  }


  /**
   * A deferred multi-column index should preserve column ordering.
   */
  @Test
  public void testDeferredMultiColumnIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );
    performUpgrade(targetSchema, AddDeferredMultiColumnIndex.class);

    executeDeferred();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = sr.getTable("Product").indexes().stream()
          .filter(i -> "Product_IdName_1".equalsIgnoreCase(i.getName()))
          .findFirst().orElseThrow(() -> new AssertionError("Index not found"));
      assertTrue("First column should be id", idx.columnNames().get(0).equalsIgnoreCase("id"));
      assertTrue("Second column should be name", idx.columnNames().get(1).equalsIgnoreCase("name"));
    }
  }


  /**
   * Creating a new table and deferring an index on it in the same
   * upgrade step should work end-to-end.
   */
  @Test
  public void testNewTableWithDeferredIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ),
        table("Category").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 50)
        ).indexes(index("Category_Label_1").columns("label"))
    );
    performUpgrade(targetSchema, AddTableWithDeferredIndex.class);

    assertDeferredIndexPending("Category", "Category_Label_1");

    executeDeferred();

    assertPhysicalIndexExists("Category", "Category_Label_1");
  }


  /**
   * Deferring an index on a table that already contains rows should
   * build the index correctly over existing data.
   */
  @Test
  public void testDeferredIndexOnPopulatedTable() {
    insertProductRow(1L, "Widget");
    insertProductRow(2L, "Gadget");
    insertProductRow(3L, "Doohickey");

    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    executeDeferred();

    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Deferring two indexes in a single upgrade step should queue both
   * and the executor should build them both.
   */
  @Test
  public void testMultipleIndexesDeferredInOneStep() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );
    performUpgrade(targetSchema, AddTwoDeferredIndexes.class);

    executeDeferred();

    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
  }


  /**
   * Running the executor a second time on an already-built set of
   * indexes should be a safe no-op.
   */
  @Test
  public void testExecutorIdempotencyOnBuiltIndexes() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // Second run -- should be a no-op
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * When forceImmediateIndexes is configured for an index name, the
   * deferred index should be built immediately during the upgrade step
   * (not deferred to the executor).
   */
  @Test
  public void testForceImmediateIndexBypassesDeferral() {
    upgradeConfigAndContext.setForceImmediateIndexes(Set.of("Product_Name_1"));
    try {
      performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

      // Index should exist immediately -- no executor needed
      assertPhysicalIndexExists("Product", "Product_Name_1");
    } finally {
      upgradeConfigAndContext.setForceImmediateIndexes(Set.of());
    }
  }


  /**
   * When forceDeferredIndexes is configured for an index name, addIndex()
   * should defer the index instead of building it immediately.
   */
  @Test
  public void testForceDeferredIndexOverridesImmediateCreation() {
    upgradeConfigAndContext.setForceDeferredIndexes(Set.of("Product_Name_1"));
    try {
      performUpgrade(schemaWithIndex(), AddImmediateIndex.class);

      // Index should NOT be physically built yet -- it was deferred
      assertDeferredIndexPending("Product", "Product_Name_1");

      // Executor should build it
      executeDeferred();
      assertPhysicalIndexExists("Product", "Product_Name_1");
    } finally {
      upgradeConfigAndContext.setForceDeferredIndexes(Set.of());
    }
  }


  /**
   * When deferredIndexCreationEnabled is false (the default), a deferred
   * index should be built immediately during the upgrade step.
   */
  @Test
  public void testDisabledFeatureBuildsDeferredIndexImmediately() {
    UpgradeConfigAndContext disabledConfig = new UpgradeConfigAndContext();
    // deferredIndexCreationEnabled defaults to false

    Upgrade.performUpgrade(schemaWithIndex(), Collections.singletonList(AddDeferredIndex.class),
        connectionResources, disabledConfig, viewDeploymentValidator);

    // Index should exist immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * When the dialect does not support deferred index creation,
   * the deferred index should be built immediately.
   */
  @Test
  public void testUnsupportedDialectFallsBackToImmediateIndex() {
    org.alfasoftware.morf.jdbc.SqlDialect realDialect = connectionResources.sqlDialect();
    org.alfasoftware.morf.jdbc.SqlDialect spyDialect = org.mockito.Mockito.spy(realDialect);
    org.mockito.Mockito.when(spyDialect.supportsDeferredIndexCreation()).thenReturn(false);

    ConnectionResources spyConn = org.mockito.Mockito.spy(connectionResources);
    org.mockito.Mockito.when(spyConn.sqlDialect()).thenReturn(spyDialect);

    Upgrade.performUpgrade(schemaWithIndex(), Collections.singletonList(AddDeferredIndex.class),
        spyConn, upgradeConfigAndContext, viewDeploymentValidator);

    // Index should exist immediately -- built during upgrade, not deferred
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> upgradeStep) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(upgradeStep),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  private void executeDeferred() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();
  }


  private Schema schemaWithIndex() {
    return schema(
        deployedViewsTable(),
        upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
  }


  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private void assertDeferredIndexPending(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Deferred index " + indexName + " should be present with isDeferred()=true on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName()) && idx.isDeferred()));
    }
  }


  private void insertProductRow(long id, String name) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            insert().into(tableRef("Product"))
                .values(literal(id).as("id"), literal(name).as("name"))
        )
    );
  }
}
