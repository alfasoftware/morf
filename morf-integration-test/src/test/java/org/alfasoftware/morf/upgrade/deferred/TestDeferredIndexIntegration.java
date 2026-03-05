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
import java.util.List;
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
import org.alfasoftware.morf.upgrade.upgrade.CreateDeferredIndexOperationTables;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenChange;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRemove;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRename;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRenameColumnThenRemove;
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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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
   * Verify that addIndexDeferred() followed by changeIndex() in the same
   * step cancels the deferred operation and creates the new index immediately.
   */
  @Test
  public void testDeferredAddFollowedByChangeIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_2").columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredIndexThenChange.class);

    assertEquals("No deferred operations should remain", 0, countOperations());
    assertIndexDoesNotExist("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_Name_2");
  }


  /**
   * Verify that addIndexDeferred() followed by renameIndex() in the same
   * step updates the deferred operation's index name in the queue.
   */
  @Test
  public void testDeferredAddFollowedByRenameIndex() {
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredIndexThenRename.class);

    assertEquals("PENDING", queryOperationStatus("Product_Name_Renamed"));
    assertEquals("Row count", 1, countOperations());

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_Renamed"));
    assertIndexExists("Product", "Product_Name_Renamed");
  }


  /**
   * Verify that addIndexDeferred() followed by changeColumn() (rename) and
   * then removeColumn() by the new name cancels the deferred operation, even
   * though the column name changed between deferral and removal.
   */
  @Test
  public void testDeferredAddFollowedByRenameColumnThenRemove() {
    // Initial schema has an extra "description" column for this test
    Schema initialWithDesc = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100),
            column("description", DataType.STRING, 200)
        )
    );
    schemaManager.mutateToSupportSchema(initialWithDesc, TruncationBehavior.ALWAYS);

    // After the step: description renamed to summary then removed; index cancelled
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        deferredIndexOperationTable(), deferredIndexOperationColumnTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        )
    );
    performUpgrade(targetSchema, AddDeferredIndexThenRenameColumnThenRemove.class);

    assertEquals("Deferred operation should be cancelled", 0, countOperations());
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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

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

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);

    // First run: build the index
    DeferredIndexExecutor executor1 = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor1.execute().join();

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");

    // Second run: should be a no-op
    DeferredIndexExecutor executor2 = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor2.execute().join();

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify crash recovery: a stale IN_PROGRESS operation is reset to PENDING
   * by the executor, then picked up and completed.
   */
  @Test
  public void testExecutorResetsInProgressAndCompletes() {
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // Simulate a crashed executor by marking the operation IN_PROGRESS
    setOperationToStaleInProgress("Product_Name_1");
    assertEquals("IN_PROGRESS", queryOperationStatus("Product_Name_1"));

    // Executor should reset IN_PROGRESS → PENDING and build
    DeferredIndexExecutionConfig execConfig = new DeferredIndexExecutionConfig();
    execConfig.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), execConfig, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");
  }


  /**
   * Verify that when forceImmediateIndexes is configured for an index name,
   * addIndexDeferred() builds the index immediately during the upgrade step
   * and does not queue a deferred operation.
   */
  @Test
  public void testForceImmediateIndexBypassesDeferral() {
    upgradeConfigAndContext.setForceImmediateIndexes(Set.of("Product_Name_1"));

    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // Index should exist immediately — no executor needed
    assertIndexExists("Product", "Product_Name_1");
    // No deferred operation should have been queued
    assertEquals("No deferred operations expected", 0, countOperations());

    // Clean up config for other tests
    upgradeConfigAndContext.setForceImmediateIndexes(Set.of());
  }


  /**
   * Verify that when forceDeferredIndexes is configured for an index name,
   * addIndex() queues a deferred operation instead of building the index
   * immediately, and the executor can then complete it.
   */
  @Test
  public void testForceDeferredIndexOverridesImmediateCreation() {
    upgradeConfigAndContext.setForceDeferredIndexes(Set.of("Product_Name_1"));

    performUpgrade(schemaWithIndex(), AddImmediateIndex.class);

    // Index should NOT exist yet — it was deferred
    assertIndexDoesNotExist("Product", "Product_Name_1");
    // A PENDING deferred operation should have been queued
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));

    // Executor should complete the build
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources), connectionResources, new SqlScriptExecutorProvider(connectionResources), config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    assertEquals("COMPLETED", queryOperationStatus("Product_Name_1"));
    assertIndexExists("Product", "Product_Name_1");

    // Clean up config for other tests
    upgradeConfigAndContext.setForceDeferredIndexes(Set.of());
  }


  /**
   * Verify that on a fresh database without deferred index tables,
   * running both {@code CreateDeferredIndexOperationTables} and a step
   * using {@code addIndexDeferred()} in the same upgrade batch succeeds.
   * This exercises the {@code @ExclusiveExecution @Sequence(1)} guarantee
   * that the infrastructure tables are created before any INSERT into them.
   */
  @Test
  public void testFreshDatabaseWithDeferredIndexInSameBatch() {
    // Start from a schema WITHOUT the deferred index tables
    Schema schemaWithoutDeferredTables = schema(
        deployedViewsTable(),
        upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        )
    );
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(schemaWithoutDeferredTables, TruncationBehavior.ALWAYS);

    // Run upgrade with both the table-creation step and a deferred index step
    Upgrade.performUpgrade(schemaWithIndex(),
        List.of(CreateDeferredIndexOperationTables.class, AddDeferredIndex.class),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);

    // The INSERT from AddDeferredIndex must have succeeded — the table existed
    assertEquals("PENDING", queryOperationStatus("Product_Name_1"));
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
        select(field("id"))
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
                    literal(1_000_000_000L).as("startedTime")
                )
                .where(field("indexName").eq(indexName))
        )
    );
  }
}
