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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationColumnTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
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
 * Integration tests for {@link DeferredIndexRecoveryService} (Stage 9).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexRecoveryService {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;

  /** Very old timestamp guaranteed to be stale under any positive stale threshold. */
  private static final long STALE_STARTED_TIME = 20_200_101_000_000L;

  private static final Schema BASE_SCHEMA = schema(
      deferredIndexOperationTable(),
      deferredIndexOperationColumnTable(),
      table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
  );

  private DeferredIndexConfig config;


  /**
   * Drop all tables, recreate the required schema, and reset config before each test.
   */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(BASE_SCHEMA, TruncationBehavior.ALWAYS);
    config = new DeferredIndexConfig();
    config.setStaleThresholdSeconds(1L); // any positive value: our stale row is far in the past
  }


  /**
   * Invalidate the schema manager cache after each test.
   */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * A stale IN_PROGRESS operation whose index does not yet exist in the database
   * should be reset to PENDING so the executor will rebuild it.
   */
  @Test
  public void testStaleOperationWithNoIndexIsResetToPending() {
    insertInProgressRow("op-r1", "Apple", "Apple_Missing", false, STALE_STARTED_TIME, "pips");

    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations();

    assertEquals("status should be PENDING", DeferredIndexStatus.PENDING.name(), queryStatus("op-r1"));
  }


  /**
   * A stale IN_PROGRESS operation whose index already exists in the database
   * should be marked COMPLETED.
   */
  @Test
  public void testStaleOperationWithExistingIndexIsMarkedCompleted() {
    // Build the schema so the Apple table has the index already
    Schema schemaWithIndex = schema(
        deferredIndexOperationTable(),
        deferredIndexOperationColumnTable(),
        table("Apple")
            .columns(column("pips", DataType.STRING, 10).nullable())
            .indexes(index("Apple_Existing").columns("pips"))
    );
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(schemaWithIndex, TruncationBehavior.ALWAYS);

    insertInProgressRow("op-r2", "Apple", "Apple_Existing", false, STALE_STARTED_TIME, "pips");

    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations();

    assertEquals("status should be COMPLETED", DeferredIndexStatus.COMPLETED.name(), queryStatus("op-r2"));
  }


  /**
   * A non-stale (recently started) IN_PROGRESS operation must not be touched by
   * the recovery service.
   */
  @Test
  public void testNonStaleOperationIsLeftUntouched() {
    // Use current timestamp as startedTime; with staleThreshold=1s and timestamp=now it is NOT stale
    long recentStarted = DeferredIndexRecoveryService.currentTimestamp();
    insertInProgressRow("op-r3", "Apple", "Apple_Active", false, recentStarted, "pips");

    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations();

    assertEquals("status should still be IN_PROGRESS",
        DeferredIndexStatus.IN_PROGRESS.name(), queryStatus("op-r3"));
  }


  /**
   * recoverStaleOperations should complete without error when there are no
   * IN_PROGRESS operations at all.
   */
  @Test
  public void testNoStaleOperationsIsANoOp() {
    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations(); // should not throw
  }


  /**
   * A stale IN_PROGRESS operation referencing a table that no longer exists
   * should be reset to PENDING (table absence implies index absence).
   */
  @Test
  public void testStaleOperationWithDroppedTableIsResetToPending() {
    insertInProgressRow("op-r4", "DroppedTable", "DroppedTable_1", false, STALE_STARTED_TIME, "col");

    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations();

    assertEquals("status should be PENDING", DeferredIndexStatus.PENDING.name(), queryStatus("op-r4"));
  }


  /**
   * Multiple stale operations with mixed outcomes: one whose index exists in
   * the database (should become COMPLETED) and one whose index is absent
   * (should become PENDING).
   */
  @Test
  public void testMixedOutcomeRecovery() {
    // Rebuild schema with an index that matches one of the operations
    Schema schemaWithIndex = schema(
        deferredIndexOperationTable(),
        deferredIndexOperationColumnTable(),
        table("Apple")
            .columns(column("pips", DataType.STRING, 10).nullable())
            .indexes(index("Apple_Present").columns("pips"))
    );
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(schemaWithIndex, TruncationBehavior.ALWAYS);

    insertInProgressRow("op-r5", "Apple", "Apple_Present", false, STALE_STARTED_TIME, "pips");
    insertInProgressRow("op-r6", "Apple", "Apple_Absent", false, STALE_STARTED_TIME, "pips");

    DeferredIndexRecoveryService service = new DeferredIndexRecoveryService(connectionResources, config);
    service.recoverStaleOperations();

    assertEquals("existing index should be COMPLETED", DeferredIndexStatus.COMPLETED.name(), queryStatus("op-r5"));
    assertEquals("missing index should be PENDING", DeferredIndexStatus.PENDING.name(), queryStatus("op-r6"));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void insertInProgressRow(String operationId, String tableName, String indexName,
                                    boolean unique, long startedTime, String... columns) {
    List<String> sql = new ArrayList<>();
    sql.addAll(connectionResources.sqlDialect().convertStatementToSQL(
        insert().into(tableRef(DEFERRED_INDEX_OPERATION_NAME)).values(
            literal(operationId).as("operationId"),
            literal("test-upgrade-uuid").as("upgradeUUID"),
            literal(tableName).as("tableName"),
            literal(indexName).as("indexName"),
            literal(DeferredIndexOperationType.ADD.name()).as("operationType"),
            literal(unique ? 1 : 0).as("indexUnique"),
            literal(DeferredIndexStatus.IN_PROGRESS.name()).as("status"),
            literal(0).as("retryCount"),
            literal(System.currentTimeMillis()).as("createdTime"),
            literal(startedTime).as("startedTime")
        )
    ));
    for (int i = 0; i < columns.length; i++) {
      sql.addAll(connectionResources.sqlDialect().convertStatementToSQL(
          insert().into(tableRef(DEFERRED_INDEX_OPERATION_COLUMN_NAME)).values(
              literal(operationId).as("operationId"),
              literal(columns[i]).as("columnName"),
              literal(i).as("columnSequence")
          )
      ));
    }
    sqlScriptExecutorProvider.get().execute(sql);
  }


  private String queryStatus(String operationId) {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("status"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("operationId").eq(operationId))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }
}
