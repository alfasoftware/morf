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
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationColumnTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeferredIndexExecutor} (Stages 7 and 8).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexExecutor {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;

  private static final Schema TEST_SCHEMA = schema(
      deferredIndexOperationTable(),
      deferredIndexOperationColumnTable(),
      table("Apple").columns(
          column("pips", DataType.STRING, 10).nullable(),
          column("color", DataType.STRING, 20).nullable()
      )
  );

  private DeferredIndexConfig config;


  /**
   * Create a fresh schema and a default config before each test.
   */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(TEST_SCHEMA, TruncationBehavior.ALWAYS);
    config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L); // fast retries for tests
  }


  /**
   * Invalidate the schema manager cache after each test.
   */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // -------------------------------------------------------------------------
  // Stage 7: execution tests
  // -------------------------------------------------------------------------

  /**
   * A PENDING operation should transition to COMPLETED and the index should
   * exist in the database schema after executeAndWait returns.
   */
  @Test
  public void testPendingTransitionsToCompleted() {
    config.setMaxRetries(0);
    insertPendingRow("Apple", "Apple_1", false, "pips");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 1, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());

    try (SchemaResource schema = connectionResources.openSchemaResource()) {
      assertTrue("Apple_1 should exist in schema",
          schema.getTable("Apple").indexes().stream().anyMatch(idx -> "Apple_1".equalsIgnoreCase(idx.getName())));
    }
  }


  /**
   * With maxRetries=0 an operation that targets a non-existent table should be
   * marked FAILED in a single attempt with no retries.
   */
  @Test
  public void testFailedAfterMaxRetriesWithNoRetries() {
    config.setMaxRetries(0);
    insertPendingRow("NoSuchTable", "NoSuchTable_1", false, "col");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("failedCount", 1, result.getFailedCount());
    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("status should be FAILED", DeferredIndexStatus.FAILED.name(), queryStatus("NoSuchTable_1"));
    assertEquals("retryCount should be 1", 1, queryRetryCount("NoSuchTable_1"));
  }


  /**
   * With maxRetries=1 a failing operation should be retried once before being
   * permanently marked FAILED with retryCount=2.
   */
  @Test
  public void testRetryOnFailure() {
    config.setMaxRetries(1);
    insertPendingRow("NoSuchTable", "NoSuchTable_1", false, "col");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("failedCount", 1, result.getFailedCount());
    assertEquals("status should be FAILED", DeferredIndexStatus.FAILED.name(), queryStatus("NoSuchTable_1"));
    assertEquals("retryCount should be 2 (initial + 1 retry)", 2, queryRetryCount("NoSuchTable_1"));
  }


  /**
   * executeAndWait on an empty queue should return an ExecutionResult with
   * zeroed counts and complete immediately.
   */
  @Test
  public void testEmptyQueueReturnsImmediately() {
    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /**
   * A unique index should be built with the UNIQUE constraint applied.
   */
  @Test
  public void testUniqueIndexCreated() {
    config.setMaxRetries(0);
    insertPendingRow("Apple", "Apple_Unique_1", true, "pips");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    executor.executeAndWait(60_000L);

    try (SchemaResource schema = connectionResources.openSchemaResource()) {
      assertTrue("Apple_Unique_1 should be unique",
          schema.getTable("Apple").indexes().stream()
              .filter(idx -> "Apple_Unique_1".equalsIgnoreCase(idx.getName()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Index not found"))
              .isUnique());
    }
  }


  /**
   * A multi-column index should be built with columns in the correct order.
   */
  @Test
  public void testMultiColumnIndexCreated() {
    config.setMaxRetries(0);
    insertPendingRow("Apple", "Apple_Multi_1", false, "pips", "color");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 1, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());

    try (SchemaResource schema = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = schema.getTable("Apple").indexes().stream()
          .filter(i -> "Apple_Multi_1".equalsIgnoreCase(i.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Multi-column index not found"));
      assertEquals("column count", 2, idx.columnNames().size());
      assertEquals("first column", "pips", idx.columnNames().get(0).toUpperCase().equals("PIPS") ? "pips" : idx.columnNames().get(0));
    }
  }


  /**
   * getStatus should reflect accurate counts after executeAndWait completes.
   * This exercises the same AtomicInteger counters that the progress logger reads.
   */
  @Test
  public void testGetStatusReflectsCompletedExecution() {
    config.setMaxRetries(0);
    insertPendingRow("Apple", "Apple_S1", false, "pips");
    insertPendingRow("NoSuchTable", "NoSuchTable_S2", false, "col");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    executor.executeAndWait(60_000L);

    DeferredIndexExecutor.ExecutionStatus status = executor.getStatus();
    assertEquals("totalCount", 2, status.getTotalCount());
    assertEquals("completedCount", 1, status.getCompletedCount());
    assertEquals("failedCount", 1, status.getFailedCount());
    assertEquals("inProgressCount", 0, status.getInProgressCount());
  }


  // -------------------------------------------------------------------------
  // Stage 8: awaitCompletion tests
  // -------------------------------------------------------------------------

  /**
   * awaitCompletion should return true immediately when no operations are queued.
   */
  @Test
  public void testAwaitCompletionReturnsTrueWhenQueueEmpty() {
    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    assertTrue("should return true for empty queue", executor.awaitCompletion(10L));
  }


  /**
   * awaitCompletion should return false when a PENDING operation exists and the
   * timeout expires before execution starts.
   */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    insertPendingRow("Apple", "Apple_2", false, "pips");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    // Timeout of 1 second; no executor is running so PENDING row never becomes COMPLETED
    assertFalse("should return false on timeout", executor.awaitCompletion(1L));
  }


  /**
   * awaitCompletion should return true immediately when all operations are
   * already in a terminal state (COMPLETED).
   */
  @Test
  public void testAwaitCompletionReturnsTrueAfterExecution() {
    config.setMaxRetries(0);
    insertPendingRow("Apple", "Apple_3", false, "pips");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(new DeferredIndexOperationDAOImpl(connectionResources), connectionResources, config);
    executor.executeAndWait(60_000L); // completes the operation

    // All operations are now COMPLETED; awaitCompletion should return true at once
    assertTrue("should return true when all operations are terminal", executor.awaitCompletion(5L));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void insertPendingRow(String tableName, String indexName,
                                 boolean unique, String... columns) {
    long operationId = Math.abs(UUID.randomUUID().getMostSignificantBits());
    List<String> sql = new ArrayList<>();
    sql.addAll(connectionResources.sqlDialect().convertStatementToSQL(
        insert().into(tableRef(DEFERRED_INDEX_OPERATION_NAME)).values(
            literal(operationId).as("id"),
            literal("test-upgrade-uuid").as("upgradeUUID"),
            literal(tableName).as("tableName"),
            literal(indexName).as("indexName"),
            literal(DeferredIndexOperationType.ADD.name()).as("operationType"),
            literal(unique ? 1 : 0).as("indexUnique"),
            literal(DeferredIndexStatus.PENDING.name()).as("status"),
            literal(0).as("retryCount"),
            literal(System.currentTimeMillis()).as("createdTime")
        )
    ));
    for (int i = 0; i < columns.length; i++) {
      sql.addAll(connectionResources.sqlDialect().convertStatementToSQL(
          insert().into(tableRef(DEFERRED_INDEX_OPERATION_COLUMN_NAME)).values(
              literal(Math.abs(UUID.randomUUID().getMostSignificantBits())).as("id"),
              literal(operationId).as("operationId"),
              literal(columns[i]).as("columnName"),
              literal(i).as("columnSequence")
          )
      ));
    }
    sqlScriptExecutorProvider.get().execute(sql);
  }


  private String queryStatus(String indexName) {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("status"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("indexName").eq(indexName))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }


  private int queryRetryCount(String indexName) {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("retryCount"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("indexName").eq(indexName))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getInt(1) : 0);
  }
}
