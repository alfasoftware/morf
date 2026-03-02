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
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.Inject;

/**
 * Default implementation of {@link DeferredIndexOperationDAO}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
class DeferredIndexOperationDAOImpl implements DeferredIndexOperationDAO {

  private static final String OPERATION_TABLE        = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
  private static final String OPERATION_COLUMN_TABLE = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME;

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * DI constructor.
   *
   * @param sqlScriptExecutorProvider provider for SQL executors.
   * @param sqlDialect the SQL dialect to use for statement conversion.
   */
  @Inject
  DeferredIndexOperationDAOImpl(SqlScriptExecutorProvider sqlScriptExecutorProvider, SqlDialect sqlDialect) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = sqlDialect;
  }


  /**
   * Constructor for use without Guice.
   *
   * @param connectionResources the connection resources to use.
   */
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
    List<String> statements = new ArrayList<>();

    statements.addAll(sqlDialect.convertStatementToSQL(
      insert().into(tableRef(OPERATION_TABLE))
        .values(
          literal(op.getOperationId()).as("operationId"),
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
            literal(op.getOperationId()).as("operationId"),
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
    SelectStatement select = select(
        field("operationId"), field("upgradeUUID"), field("tableName"),
        field("indexName"), field("operationType"), field("indexUnique"),
        field("status"), field("retryCount"), field("createdTime"),
        field("startedTime"), field("completedTime"), field("errorMessage")
      ).from(tableRef(OPERATION_TABLE))
       .where(and(
         field("status").eq(DeferredIndexStatus.IN_PROGRESS.name()),
         field("startedTime").lessThan(literal(startedBefore))
       ));

    String sql = sqlDialect.convertStatementToSQL(select);
    List<DeferredIndexOperation> ops = sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperations);
    return loadColumnNamesForAll(ops);
  }


  /**
   * Returns {@code true} if a record for the given upgrade UUID and index name
   * already exists in the queue (regardless of status).
   *
   * @param upgradeUUID the UUID of the upgrade step.
   * @param indexName   the name of the index.
   * @return {@code true} if a matching record exists.
   */
  @Override
  public boolean existsByUpgradeUUIDAndIndexName(String upgradeUUID, String indexName) {
    SelectStatement select = select(field("operationId"))
      .from(tableRef(OPERATION_TABLE))
      .where(and(
        field("upgradeUUID").eq(upgradeUUID),
        field("indexName").eq(indexName)
      ));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, ResultSet::next);
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
    SelectStatement select = select(field("operationId"))
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
  public void markStarted(String operationId, long startedTime) {
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.IN_PROGRESS.name()).as("status"),
            literal(startedTime).as("startedTime")
          )
          .where(field("operationId").eq(operationId))
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
  public void markCompleted(String operationId, long completedTime) {
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.COMPLETED.name()).as("status"),
            literal(completedTime).as("completedTime")
          )
          .where(field("operationId").eq(operationId))
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
  public void markFailed(String operationId, String errorMessage, int newRetryCount) {
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(
            literal(DeferredIndexStatus.FAILED.name()).as("status"),
            literal(errorMessage).as("errorMessage"),
            literal(newRetryCount).as("retryCount")
          )
          .where(field("operationId").eq(operationId))
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
  public void resetToPending(String operationId) {
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(literal(DeferredIndexStatus.PENDING.name()).as("status"))
          .where(field("operationId").eq(operationId))
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
  public void updateStatus(String operationId, DeferredIndexStatus newStatus) {
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(OPERATION_TABLE))
          .set(literal(newStatus.name()).as("status"))
          .where(field("operationId").eq(operationId))
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
    SelectStatement select = select(field("operationId"))
      .from(tableRef(OPERATION_TABLE))
      .where(or(
        field("status").eq(DeferredIndexStatus.PENDING.name()),
        field("status").eq(DeferredIndexStatus.IN_PROGRESS.name())
      ));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, ResultSet::next);
  }


  private List<DeferredIndexOperation> findOperationsByStatus(DeferredIndexStatus status) {
    SelectStatement select = select(
        field("operationId"), field("upgradeUUID"), field("tableName"),
        field("indexName"), field("operationType"), field("indexUnique"),
        field("status"), field("retryCount"), field("createdTime"),
        field("startedTime"), field("completedTime"), field("errorMessage")
      ).from(tableRef(OPERATION_TABLE))
       .where(field("status").eq(status.name()));

    String sql = sqlDialect.convertStatementToSQL(select);
    List<DeferredIndexOperation> ops = sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperations);
    return loadColumnNamesForAll(ops);
  }


  private List<DeferredIndexOperation> loadColumnNamesForAll(List<DeferredIndexOperation> ops) {
    for (DeferredIndexOperation op : ops) {
      op.setColumnNames(loadColumnNames(op.getOperationId()));
    }
    return ops;
  }


  private List<String> loadColumnNames(String operationId) {
    SelectStatement select = select(field("columnName"))
      .from(tableRef(OPERATION_COLUMN_TABLE))
      .where(field("operationId").eq(operationId))
      .orderBy(field("columnSequence"));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      List<String> names = new ArrayList<>();
      while (rs.next()) {
        names.add(rs.getString(1));
      }
      return names;
    });
  }


  private List<DeferredIndexOperation> mapOperations(ResultSet rs) throws SQLException {
    List<DeferredIndexOperation> result = new ArrayList<>();
    while (rs.next()) {
      DeferredIndexOperation op = new DeferredIndexOperation();
      op.setOperationId(rs.getString("operationId"));
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
      result.add(op);
    }
    return result;
  }
}
