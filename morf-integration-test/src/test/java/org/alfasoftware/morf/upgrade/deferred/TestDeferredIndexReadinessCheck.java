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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
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
 * Integration tests for {@link DeferredIndexReadinessCheckImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexReadinessCheck {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;

  private static final Schema TEST_SCHEMA = schema(
      deferredIndexOperationTable(),
      table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
  );

  private UpgradeConfigAndContext config;


  /**
   * Drop and recreate the required schema before each test.
   */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(TEST_SCHEMA, TruncationBehavior.ALWAYS);
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    config.setDeferredIndexRetryBaseDelayMs(10L);
  }


  /**
   * Invalidate the schema manager cache after each test.
   */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * forceBuildAllPending() should be a no-op when the queue is empty — no exception thrown
   * and no operations executed.
   */
  @Test
  public void testValidateWithEmptyQueueIsNoOp() {
    DeferredIndexReadinessCheck validator = createValidator(config);
    validator.forceBuildAllPending(); // must not throw
  }


  /**
   * When PENDING operations exist, forceBuildAllPending() must execute them before returning:
   * the index should exist in the schema and the row should be COMPLETED
   * (not PENDING) when the call returns.
   */
  @Test
  public void testPendingOperationsAreExecutedBeforeReturning() {
    insertPendingRow("Apple", "Apple_V1", false, "pips");

    DeferredIndexReadinessCheck validator = createValidator(config);
    validator.forceBuildAllPending();

    // Verify no PENDING rows remain
    assertFalse("no non-terminal operations should remain after validate",
        hasPendingOperations());

    // Verify the index actually exists in the database
    try (var schema = connectionResources.openSchemaResource()) {
      assertTrue("Apple_V1 index should exist",
          schema.getTable("Apple").indexes().stream().anyMatch(idx -> "Apple_V1".equalsIgnoreCase(idx.getName())));
    }
  }


  /**
   * When multiple PENDING operations exist they should all be executed before
   * forceBuildAllPending() returns.
   */
  @Test
  public void testMultiplePendingOperationsAllExecuted() {
    insertPendingRow("Apple", "Apple_V2", false, "pips");
    insertPendingRow("Apple", "Apple_V3", true, "pips");

    DeferredIndexReadinessCheck validator = createValidator(config);
    validator.forceBuildAllPending();

    assertFalse("no non-terminal operations should remain", hasPendingOperations());
  }


  /**
   * When a PENDING operation targets a non-existent table, forceBuildAllPending() should
   * throw because the forced execution fails.
   */
  @Test
  public void testFailedForcedExecutionThrows() {
    insertPendingRow("NoSuchTable", "NoSuchTable_V4", false, "col");

    DeferredIndexReadinessCheck validator = createValidator(config);
    try {
      validator.forceBuildAllPending();
      fail("Expected IllegalStateException for failed forced execution");
    } catch (IllegalStateException e) {
      assertTrue("exception message should mention failed count",
          e.getMessage().contains("1 index operation(s) could not be built"));
    }

    // The operation should be FAILED, not PENDING
    assertEquals("status should be FAILED after forced execution",
        DeferredIndexStatus.FAILED.name(), queryStatus("NoSuchTable_V4"));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void insertPendingRow(String tableName, String indexName,
                                 boolean unique, String... columns) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            insert().into(tableRef(DEFERRED_INDEX_OPERATION_NAME)).values(
                literal(Math.abs(UUID.randomUUID().getMostSignificantBits())).as("id"),
                literal("test-upgrade-uuid").as("upgradeUUID"),
                literal(tableName).as("tableName"),
                literal(indexName).as("indexName"),
                literal(unique ? 1 : 0).as("indexUnique"),
                literal(String.join(",", columns)).as("indexColumns"),
                literal(DeferredIndexStatus.PENDING.name()).as("status"),
                literal(0).as("retryCount"),
                literal(System.currentTimeMillis()).as("createdTime")
            )
        )
    );
  }


  private String queryStatus(String indexName) {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("status"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("indexName").eq(indexName))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }


  private DeferredIndexReadinessCheck createValidator(UpgradeConfigAndContext validatorConfig) {
    DeferredIndexOperationDAO dao = new DeferredIndexOperationDAOImpl(new SqlScriptExecutorProvider(connectionResources), connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, connectionResources, new SqlScriptExecutorProvider(connectionResources), validatorConfig, new DeferredIndexExecutorServiceFactory.Default());
    return new DeferredIndexReadinessCheckImpl(dao, executor, validatorConfig, connectionResources);
  }


  private boolean hasPendingOperations() {
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        select(field("id"))
            .from(tableRef(DEFERRED_INDEX_OPERATION_NAME))
            .where(field("status").eq(DeferredIndexStatus.PENDING.name()))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next());
  }
}
