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
import static org.junit.Assert.assertTrue;

import java.util.Collections;

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
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for the {@link DeferredIndexService} facade, verifying
 * the full lifecycle through a real database: upgrade step queues deferred
 * index operations, then the service recovers stale entries, executes
 * pending builds, and reports the results.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexService {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private final UpgradeConfigAndContext upgradeConfigAndContext = new UpgradeConfigAndContext();

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
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * Verify that execute() recovers, builds the index, marks it COMPLETED,
   * and the index exists in the schema.
   */
  @Test
  public void testExecuteBuildsIndexEndToEnd() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexService service = createService(config);
    service.execute();
    service.awaitCompletion(60L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify that execute() handles multiple deferred indexes in a single run.
   */
  @Test
  public void testExecuteBuildsMultipleIndexes() {
    Schema targetSchema = schema(
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
    performUpgrade(targetSchema, AddTwoDeferredIndexes.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexService service = createService(config);
    service.execute();
    service.awaitCompletion(60L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertEquals("COMPLETED", queryOperationStatus("Product_IdName_1"));
    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  /**
   * Verify that execute() with an empty queue completes immediately with no error.
   */
  @Test
  public void testExecuteWithEmptyQueue() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexService service = createService(config);
    service.execute();

    // awaitCompletion should return true immediately on an empty queue
    assertTrue("Should complete immediately on empty queue", service.awaitCompletion(5L));
  }


  /**
   * Verify that execute() recovers a stale IN_PROGRESS operation before
   * executing it.
   */
  @Test
  public void testExecuteRecoversStaleAndCompletes() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // Simulate a crashed executor — mark the operation as stale IN_PROGRESS
    setOperationToStaleInProgress("Product_Name_1");
    assertEquals("IN_PROGRESS", queryOperationStatus("Product_Name_1"));

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    config.setStaleThresholdSeconds(1L);
    DeferredIndexService service = createService(config);
    service.execute();
    service.awaitCompletion(60L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify that awaitCompletion() returns true immediately when the
   * queue is empty.
   */
  @Test
  public void testAwaitCompletionReturnsTrueWhenEmpty() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexService service = createService(config);
    assertTrue("Should return true on empty queue", service.awaitCompletion(5L));
  }


  /**
   * Verify that awaitCompletion() returns true when all operations are
   * already COMPLETED.
   */
  @Test
  public void testAwaitCompletionReturnsTrueWhenAllCompleted() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // Build the index first
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexService firstService = createService(config);
    firstService.execute();
    firstService.awaitCompletion(60L);

    // Now await on a new service should return immediately
    DeferredIndexService service = createService(config);
    assertTrue("Should return true when all completed", service.awaitCompletion(5L));
  }


  /**
   * Verify that execute() is idempotent — calling it a second time on an
   * already-completed queue is a safe no-op.
   */
  @Test
  public void testExecuteIdempotent() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexService service = createService(config);

    service.execute();
    service.awaitCompletion(60L);
    assertEquals("First run should complete", "COMPLETED", queryOperationStatus("Product_Name_1"));

    // Second execute on a fresh service — should be a no-op
    DeferredIndexService service2 = createService(config);
    service2.execute();
    service2.awaitCompletion(60L);
    assertEquals("Should still be COMPLETED after second run", "COMPLETED", queryOperationStatus("Product_Name_1"));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> upgradeStep) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(upgradeStep),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  private Schema schemaWithIndex() {
    return schema(
        deployedViewsTable(),
        upgradeAuditTable(),
        deferredIndexOperationTable(),
        deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
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


  private void assertIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private DeferredIndexService createService(DeferredIndexConfig config) {
    DeferredIndexOperationDAO dao = new DeferredIndexOperationDAOImpl(connectionResources);
    DeferredIndexRecoveryService recovery = new DeferredIndexRecoveryServiceImpl(dao, connectionResources, config);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, connectionResources, config);
    return new DeferredIndexServiceImpl(recovery, executor, config);
  }


  private void setOperationToStaleInProgress(String indexName) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            update(tableRef(DEFERRED_INDEX_OPERATION_NAME))
                .set(
                    literal("IN_PROGRESS").as("status"),
                    literal(1_000_000_000L).as("startedTime")
                )
                .where(field("indexName").eq(indexName))
        )
    );
  }
}
