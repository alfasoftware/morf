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
import static org.alfasoftware.morf.sql.SqlUtils.insert;
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
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRemove;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredUniqueIndex;
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
 * End-to-end integration tests for the deferred index lifecycle (Stage 12).
 * Exercises the full upgrade framework path: upgrade step execution,
 * deferred operation queueing, executor completion, and schema verification.
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
   * Verify that running an upgrade step with addIndexDeferred() inserts
   * a PENDING row into the DeferredIndexOperation table.
   */
  @Test
  public void testDeferredAddCreatesPendingRow() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));
    assertEquals("Row count", 1, countOperations());
  }


  /**
   * Verify that running the executor after the upgrade step completes
   * the build, marks the row COMPLETED, and the index exists in the schema.
   */
  @Test
  public void testExecutorCompletesAndIndexExistsInSchema() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutor(connectionResources, config);
    executor.executeAndWait(60_000L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify that addIndexDeferred() followed immediately by removeIndex()
   * in the same step auto-cancels the deferred operation.
   */
  @Test
  public void testAutoCancelDeferredAddFollowedByRemove() {
    Schema targetSchema = schema(INITIAL_SCHEMA);
    performUpgrade(targetSchema, AddDeferredIndexThenRemove.class);

    assertEquals("No deferred operations should remain", 0, countOperations());
    assertIndexDoesNotExist("Product", "Product_Name_1");
  }


  /**
   * Verify that a deferred unique index is built correctly with
   * the unique constraint preserved through the full pipeline.
   */
  @Test
  public void testDeferredUniqueIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredUniqueIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, config).executeAndWait(60_000L);

    assertIndexExists("Product", "Product_Name_UQ");
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index should be unique",
          sr.getTable("Product").indexes().stream()
              .filter(idx -> "Product_Name_UQ".equalsIgnoreCase(idx.getName()))
              .findFirst().get().isUnique());
    }
  }


  /**
   * Verify that a deferred multi-column index preserves column ordering
   * through the full pipeline.
   */
  @Test
  public void testDeferredMultiColumnIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );
    performUpgrade(targetSchema, AddDeferredMultiColumnIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, config).executeAndWait(60_000L);

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = sr.getTable("Product").indexes().stream()
          .filter(i -> "Product_IdName_1".equalsIgnoreCase(i.getName()))
          .findFirst().orElseThrow(() -> new AssertionError("Index not found"));
      assertEquals("Column count", 2, idx.columnNames().size());
      assertEquals("First column", "id", idx.columnNames().get(0).toLowerCase());
      assertEquals("Second column", "name", idx.columnNames().get(1).toLowerCase());
    }
  }


  /**
   * Verify that creating a new table and deferring an index on it
   * in the same upgrade step works end-to-end.
   */
  @Test
  public void testNewTableWithDeferredIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
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

    assertEquals("PENDING", queryOperationStatus("Category_Label_1"));

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, config).executeAndWait(60_000L);

    assertEquals("COMPLETED", queryOperationStatus("Category_Label_1"));
    assertIndexExists("Category", "Category_Label_1");
  }


  /**
   * Verify that deferring an index on a table that already contains rows
   * builds the index correctly over existing data.
   */
  @Test
  public void testDeferredIndexOnPopulatedTable() {
    insertProductRow(1L, "Widget");
    insertProductRow(2L, "Gadget");
    insertProductRow(3L, "Doohickey");

    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, config).executeAndWait(60_000L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify that deferring two indexes in a single upgrade step queues
   * both and the executor builds them both to completion.
   */
  @Test
  public void testMultipleIndexesDeferredInOneStep() {
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

    assertEquals("Row count", 2, countOperations());
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));
    assertEquals("PENDING", queryOperationStatus("Product_IdName_1"));

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, config).executeAndWait(60_000L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertEquals("COMPLETED", queryOperationStatus("Product_IdName_1"));
    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  /**
   * Verify that running the executor a second time on an already-completed
   * queue is a safe no-op with no errors.
   */
  @Test
  public void testExecutorIdempotencyOnCompletedQueue() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutor(connectionResources, config);

    DeferredIndexExecutor.ExecutionResult firstRun = executor.executeAndWait(60_000L);
    assertEquals("First run completed", 1, firstRun.getCompletedCount());
    assertEquals("First run failed", 0, firstRun.getFailedCount());

    DeferredIndexExecutor.ExecutionResult secondRun = executor.executeAndWait(60_000L);
    assertEquals("Second run completed", 0, secondRun.getCompletedCount());
    assertEquals("Second run failed", 0, secondRun.getFailedCount());

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify the full recovery-to-execution pipeline: a stale IN_PROGRESS
   * operation is reset to PENDING by the recovery service, then the executor
   * picks it up and completes the index build.
   */
  @Test
  public void testRecoveryResetsStaleOperationThenExecutorCompletes() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // Simulate a crashed executor by marking the operation IN_PROGRESS
    // with a timestamp far in the past
    setOperationToStaleInProgress("Product_Name_1");

    assertEquals("IN_PROGRESS", queryOperationStatus("Product_Name_1"));

    // Recovery with a 1-second stale threshold should reset it to PENDING
    DeferredIndexConfig recoveryConfig = new DeferredIndexConfig();
    recoveryConfig.setStaleThresholdSeconds(1L);
    new DeferredIndexRecoveryService(connectionResources, recoveryConfig).recoverStaleOperations();

    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));

    // Now the executor should pick it up and complete the build
    DeferredIndexConfig execConfig = new DeferredIndexConfig();
    execConfig.setRetryBaseDelayMs(10L);
    new DeferredIndexExecutor(connectionResources, execConfig).executeAndWait(60_000L);

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


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


  private int countOperations() {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("operationId"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      int count = 0;
      while (rs.next()) count++;
      return count;
    });
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


  private void insertProductRow(long id, String name) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            insert().into(tableRef("Product"))
                .values(literal(id).as("id"), literal(name).as("name"))
        )
    );
  }


  private void setOperationToStaleInProgress(String indexName) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            update(tableRef(DEFERRED_INDEX_OPERATION_NAME))
                .set(
                    literal("IN_PROGRESS").as("status"),
                    literal(20250101120000L).as("startedTime")
                )
                .where(field("indexName").eq(indexName))
        )
    );
  }
}
