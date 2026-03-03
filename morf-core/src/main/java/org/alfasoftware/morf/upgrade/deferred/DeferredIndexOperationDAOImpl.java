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

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexOperationDAO}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexOperationDAOImpl implements DeferredIndexOperationDAO {

  private static final Log log = LogFactory.getLog(DeferredIndexOperationDAOImpl.class);

  private static final String OPERATION_TABLE        = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
  private static final String OPERATION_COLUMN_TABLE = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME;

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * Construct with explicit dependencies.
   *
   * @param sqlScriptExecutorProvider provider for SQL executors.
   * @param sqlDialect the SQL dialect to use for statement conversion.
   */
  DeferredIndexOperationDAOImpl(SqlScriptExecutorProvider sqlScriptExecutorProvider, SqlDialect sqlDialect) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = sqlDialect;
  }


  /**
   * Construct from {@link ConnectionResources}.
   *
   * @param connectionResources the connection resources to use.
   */
  @Inject
  DeferredIndexOperationDAOImpl(ConnectionResources connectionResources) {
    this(new SqlScriptExecutorProvider(connectionResources.getDataSource(), connectionResources.sqlDialect()),
         connectionResources.sqlDialect());
  }


  /**
   * Inserts a new operation row together with its column rows.
   *
   * @param op the operation to insert.
   */
  @Override
  public void insertOperation(DeferredIndexOperation op) {
    if (log.isDebugEnabled()) {
      log.debug("Inserting deferred index operation [" + op.getId() + "]: table=" + op.getTableName()
          + ", index=" + op.getIndexName() + ", columns=" + op.getColumnNames());
    }
    List<String> statements = new ArrayList<>();

    statements.addAll(sqlDialect.convertStatementToSQL(
      insert().into(tableRef(OPERATION_TABLE))
        .values(
          literal(op.getId()).as("id"),
          literal(op.getUpgradeUUID()).as("upgradeUUID"),
          literal(op.getTableName()).as("tableName"),
          literal(op.getIndexName()).as("indexName"),
          literal(op.getOperationType().name()).as("operationType"),
          literal(op.isIndexUnique()).as("indexUnique"),
          literal(op.getStatus().name()).as("status"),
          literal(op.getRetryCount()).as("retryCount"),
          literal(op.getCreatedTime()).as("createdTime")
        )
    ));

    List<String> columnNames = op.getColumnNames();
    for (int seq = 0; seq < columnNames.size(); seq++) {
      statements.addAll(sqlDialect.convertStatementToSQL(
        insert().into(tableRef(OPERATION_COLUMN_TABLE))
          .values(
            literal(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE).as("id"),
            literal(op.getId()).as("operationId"),
            literal(columnNames.get(seq)).as("columnName"),
            literal(seq).as("columnSequence")
          )
      ));
    }

    sqlScriptExecutorProvider.get().execute(statements);
  }


  /**
   * Returns all {@link DeferredIndexOperation#STATUS_PENDING} operations with
   * their ordered column names populated.
   *
   * @return list of pending operations.
   */
  @Override
  public List<DeferredIndexOperation> findPendingOperations() {
    return findOperationsByStatus(DeferredIndexStatus.PENDING);
  }


  /**
   * Returns all {@link DeferredIndexOperation#STATUS_IN_PROGRESS} operations
   * whose {@code startedTime} is strictly less than the supplied threshold,
   * indicating a stale or abandoned build.
   *
   * @param startedBefore upper bound on {@code startedTime} (yyyyMMddHHmmss).
   * @return list of stale in-progress operations.
   */
  @Override
  public List<DeferredIndexOperation> findStaleInProgressOperations(long startedBefore) {
    TableReference op = tableRef(OPERATION_TABLE);
    TableReference col = tableRef(OPERATION_COLUMN_TABLE);

    SelectStatement select = select(
        op.field("id"), op.field("upgradeUUID"), op.field("tableName"),
        op.field("indexName"), op.field("operationType"), op.field("indexUnique"),
        op.field("status"), op.field("retryCount"), op.field("createdTime"),
        op.field("startedTime"), op.field("completedTime"), op.field("errorMessage"),
        col.field("columnName"), col.field("columnSequence")
      ).from(op)
       .leftOuterJoin(col, op.field("id").eq(col.field("operationId")))
       .where(and(
         op.field("status").eq(DeferredIndexStatus.IN_PROGRESS.name()),
         op.field("startedTime").lessThan(literal(startedBefore))
       ))
       .orderBy(op.field("id"), col.field("columnSequence"));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperationsWithColumns);
  }


  /**
   * Returns {@code true} if any record for the given table name and index name
   * exists in the queue (regardless of status). Used by
   * {@link org.alfasoftware.morf.upgrade.deferred.DeferredAddIndex#isApplied} to
   * detect whether the upgrade step has already been processed.
   *
   * @param tableName the name of the table.
   * @param indexName the name of the index.
   * @return {@code true} if a matching record exists.
   */
  @Override
  public boolean existsByTableNameAndIndexName(String tableName, String indexName) {
    SelectStatement select = select(field("id"))
      .from(tableRef(OPERATION_TABLE))
      .where(and(
        field("tableName").eq(tableName),
        field("indexName").eq(indexName)
      ));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, ResultSet::next);
  }


  /**
   * Transitions the operation to {@link DeferredIndexOperation#STATUS_IN_PROGRESS}
   * and records its start time.
   *
   * @param operationId the operation to update.
   * @param startedTime start timestamp (yyyyMMddHHmmss).
   */
  @Override
  public void markStarted(long id, long startedTime) {
    if (log.isDebugEnabled()) log.debug("Marking operation [" + id + "] as IN_PROGRESS");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.IN_PROGRESS.name()).as("status"),
            literal(startedTime).as("startedTime")
          )
          .where(field("id").eq(id))
      )
    );
  }


  /**
   * Transitions the operation to {@link DeferredIndexOperation#STATUS_COMPLETED}
   * and records its completion time.
   *
   * @param operationId   the operation to update.
   * @param completedTime completion timestamp (yyyyMMddHHmmss).
   */
  @Override
  public void markCompleted(long id, long completedTime) {
    if (log.isDebugEnabled()) log.debug("Marking operation [" + id + "] as COMPLETED");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.COMPLETED.name()).as("status"),
            literal(completedTime).as("completedTime")
          )
          .where(field("id").eq(id))
      )
    );
  }


  /**
   * Transitions the operation to {@link DeferredIndexOperation#STATUS_FAILED},
   * records the error message, and stores the updated retry count.
   *
   * @param operationId   the operation to update.
   * @param errorMessage  the error message.
   * @param newRetryCount the new retry count value.
   */
  @Override
  public void markFailed(long id, String errorMessage, int newRetryCount) {
    if (log.isDebugEnabled()) log.debug("Marking operation [" + id + "] as FAILED (retryCount=" + newRetryCount + ")");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.FAILED.name()).as("status"),
            literal(errorMessage).as("errorMessage"),
            literal(newRetryCount).as("retryCount")
          )
          .where(field("id").eq(id))
      )
    );
  }


  /**
   * Resets a {@link DeferredIndexOperation#STATUS_FAILED} operation back to
   * {@link DeferredIndexOperation#STATUS_PENDING} so it will be retried.
   *
   * @param operationId the operation to reset.
   */
  @Override
  public void resetToPending(long id) {
    if (log.isDebugEnabled()) log.debug("Resetting operation [" + id + "] to PENDING");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(literal(DeferredIndexStatus.PENDING.name()).as("status"))
          .where(field("id").eq(id))
      )
    );
  }


  /**
   * Updates the status of an operation to the supplied value.
   *
   * @param operationId the operation to update.
   * @param newStatus   the new status value.
   */
  @Override
  public void updateStatus(long id, DeferredIndexStatus newStatus) {
    if (log.isDebugEnabled()) log.debug("Updating operation [" + id + "] status to " + newStatus);
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(literal(newStatus.name()).as("status"))
          .where(field("id").eq(id))
      )
    );
  }


  /**
   * Returns {@code true} if there is at least one PENDING or IN_PROGRESS operation.
   *
   * @return {@code true} if any non-terminal operations exist.
   */
  @Override
  public boolean hasNonTerminalOperations() {
    SelectStatement select = select(field("id"))
      .from(tableRef(OPERATION_TABLE))
      .where(or(
        field("status").eq(DeferredIndexStatus.PENDING.name()),
        field("status").eq(DeferredIndexStatus.IN_PROGRESS.name())
      ));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, ResultSet::next);
  }


  private List<DeferredIndexOperation> findOperationsByStatus(DeferredIndexStatus status) {
    TableReference op = tableRef(OPERATION_TABLE);
    TableReference col = tableRef(OPERATION_COLUMN_TABLE);

    SelectStatement select = select(
        op.field("id"), op.field("upgradeUUID"), op.field("tableName"),
        op.field("indexName"), op.field("operationType"), op.field("indexUnique"),
        op.field("status"), op.field("retryCount"), op.field("createdTime"),
        op.field("startedTime"), op.field("completedTime"), op.field("errorMessage"),
        col.field("columnName"), col.field("columnSequence")
      ).from(op)
       .leftOuterJoin(col, op.field("id").eq(col.field("operationId")))
       .where(op.field("status").eq(status.name()))
       .orderBy(op.field("id"), col.field("columnSequence"));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperationsWithColumns);
  }


  /**
   * Maps a joined result set (operation + column rows) into a list of
   * {@link DeferredIndexOperation} instances with column names populated.
   * Consecutive rows with the same {@code id} are collapsed into a single
   * operation object.
   */
  private List<DeferredIndexOperation> mapOperationsWithColumns(ResultSet rs) throws SQLException {
    Map<Long, DeferredIndexOperation> byId = new LinkedHashMap<>();

    while (rs.next()) {
      long id = rs.getLong("id");
      DeferredIndexOperation op = byId.get(id);

      if (op == null) {
        op = new DeferredIndexOperation();
        op.setId(id);
        op.setUpgradeUUID(rs.getString("upgradeUUID"));
        op.setTableName(rs.getString("tableName"));
        op.setIndexName(rs.getString("indexName"));
        op.setOperationType(DeferredIndexOperationType.valueOf(rs.getString("operationType")));
        op.setIndexUnique(rs.getBoolean("indexUnique"));
        op.setStatus(DeferredIndexStatus.valueOf(rs.getString("status")));
        op.setRetryCount(rs.getInt("retryCount"));
        op.setCreatedTime(rs.getLong("createdTime"));
        long startedTime = rs.getLong("startedTime");
        op.setStartedTime(rs.wasNull() ? null : startedTime);
        long completedTime = rs.getLong("completedTime");
        op.setCompletedTime(rs.wasNull() ? null : completedTime);
        op.setErrorMessage(rs.getString("errorMessage"));
        op.setColumnNames(new ArrayList<>());
        byId.put(id, op);
      }

      String columnName = rs.getString("columnName");
      if (columnName != null) {
        op.getColumnNames().add(columnName);
      }
    }

    return new ArrayList<>(byId.values());
  }
}
