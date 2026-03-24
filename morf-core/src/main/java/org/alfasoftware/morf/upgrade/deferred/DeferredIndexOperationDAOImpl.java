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
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.sql.SelectStatement;
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

  private static final String DEFERRED_INDEX_OP_TABLE = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;

  // Column name constants
  private static final String COL_ID = "id";
  private static final String COL_UPGRADE_UUID = "upgradeUUID";
  private static final String COL_TABLE_NAME = "tableName";
  private static final String COL_INDEX_NAME = "indexName";
  private static final String COL_INDEX_UNIQUE = "indexUnique";
  private static final String COL_INDEX_COLUMNS = "indexColumns";
  private static final String COL_STATUS = "status";
  private static final String COL_RETRY_COUNT = "retryCount";
  private static final String COL_CREATED_TIME = "createdTime";
  private static final String COL_STARTED_TIME = "startedTime";
  private static final String COL_COMPLETED_TIME = "completedTime";
  private static final String COL_ERROR_MESSAGE = "errorMessage";

  private static final String LOG_MARKING_OP = "Marking operation [";

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * Constructs the DAO with injected dependencies.
   *
   * @param sqlScriptExecutorProvider provider for SQL executors.
   * @param connectionResources      database connection resources.
   */
  @Inject
  DeferredIndexOperationDAOImpl(SqlScriptExecutorProvider sqlScriptExecutorProvider, ConnectionResources connectionResources) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = connectionResources.sqlDialect();
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
   * Transitions the operation to {@link DeferredIndexOperation#STATUS_IN_PROGRESS}
   * and records its start time.
   *
   * @param operationId the operation to update.
   * @param startedTime start timestamp (epoch milliseconds).
   */
  @Override
  public void markStarted(long id, long startedTime) {
    if (log.isDebugEnabled()) log.debug(LOG_MARKING_OP + id + "] as IN_PROGRESS");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(DEFERRED_INDEX_OP_TABLE))
          .set(
            literal(DeferredIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
            literal(startedTime).as(COL_STARTED_TIME)
          )
          .where(field(COL_ID).eq(id))
      )
    );
  }


  /**
   * Transitions the operation to {@link DeferredIndexOperation#STATUS_COMPLETED}
   * and records its completion time.
   *
   * @param operationId   the operation to update.
   * @param completedTime completion timestamp (epoch milliseconds).
   */
  @Override
  public void markCompleted(long id, long completedTime) {
    if (log.isDebugEnabled()) log.debug(LOG_MARKING_OP + id + "] as COMPLETED");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(DEFERRED_INDEX_OP_TABLE))
          .set(
            literal(DeferredIndexStatus.COMPLETED.name()).as(COL_STATUS),
            literal(completedTime).as(COL_COMPLETED_TIME)
          )
          .where(field(COL_ID).eq(id))
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
    if (log.isDebugEnabled()) log.debug(LOG_MARKING_OP + id + "] as FAILED (retryCount=" + newRetryCount + ")");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(DEFERRED_INDEX_OP_TABLE))
          .set(
            literal(DeferredIndexStatus.FAILED.name()).as(COL_STATUS),
            literal(errorMessage).as(COL_ERROR_MESSAGE),
            literal(newRetryCount).as(COL_RETRY_COUNT)
          )
          .where(field(COL_ID).eq(id))
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
        update(tableRef(DEFERRED_INDEX_OP_TABLE))
          .set(literal(DeferredIndexStatus.PENDING.name()).as(COL_STATUS))
          .where(field(COL_ID).eq(id))
      )
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexOperationDAO#resetAllInProgressToPending()
   */
  @Override
  public void resetAllInProgressToPending() {
    log.info("Resetting any IN_PROGRESS deferred index operations to PENDING");
    sqlScriptExecutorProvider.get().execute(
      sqlDialect.convertStatementToSQL(
        update(tableRef(DEFERRED_INDEX_OP_TABLE))
          .set(literal(DeferredIndexStatus.PENDING.name()).as(COL_STATUS))
          .where(field(COL_STATUS).eq(DeferredIndexStatus.IN_PROGRESS.name()))
      )
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexOperationDAO#findNonTerminalOperations()
   */
  @Override
  public List<DeferredIndexOperation> findNonTerminalOperations() {
    SelectStatement select = select(
        field(COL_ID), field(COL_UPGRADE_UUID), field(COL_TABLE_NAME),
        field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
        field(COL_STATUS), field(COL_RETRY_COUNT), field(COL_CREATED_TIME),
        field(COL_STARTED_TIME), field(COL_COMPLETED_TIME), field(COL_ERROR_MESSAGE)
      ).from(tableRef(DEFERRED_INDEX_OP_TABLE))
       .where(or(
         field(COL_STATUS).eq(DeferredIndexStatus.PENDING.name()),
         field(COL_STATUS).eq(DeferredIndexStatus.IN_PROGRESS.name()),
         field(COL_STATUS).eq(DeferredIndexStatus.FAILED.name())
       ))
       .orderBy(field(COL_ID));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperations);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexOperationDAO#countAllByStatus()
   */
  @Override
  public Map<DeferredIndexStatus, Integer> countAllByStatus() {
    SelectStatement select = select(field(COL_STATUS))
      .from(tableRef(DEFERRED_INDEX_OP_TABLE));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      Map<DeferredIndexStatus, Integer> counts = new EnumMap<>(DeferredIndexStatus.class);
      for (DeferredIndexStatus s : DeferredIndexStatus.values()) {
        counts.put(s, 0);
      }
      while (rs.next()) {
        String statusValue = rs.getString(1);
        try {
          DeferredIndexStatus status = DeferredIndexStatus.valueOf(statusValue);
          counts.merge(status, 1, Integer::sum);
        } catch (IllegalArgumentException e) {
          log.warn("Ignoring unrecognised deferred index status value: " + statusValue);
        }
      }
      return counts;
    });
  }


  /**
   * Returns all operations with the given status, with column names populated.
   *
   * @param status the status to filter by.
   * @return list of matching operations.
   */
  private List<DeferredIndexOperation> findOperationsByStatus(DeferredIndexStatus status) {
    SelectStatement select = select(
        field(COL_ID), field(COL_UPGRADE_UUID), field(COL_TABLE_NAME),
        field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
        field(COL_STATUS), field(COL_RETRY_COUNT), field(COL_CREATED_TIME),
        field(COL_STARTED_TIME), field(COL_COMPLETED_TIME), field(COL_ERROR_MESSAGE)
      ).from(tableRef(DEFERRED_INDEX_OP_TABLE))
       .where(field(COL_STATUS).eq(status.name()))
       .orderBy(field(COL_ID));

    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapOperations);
  }


  /**
   * Maps a result set into a list of {@link DeferredIndexOperation} instances.
   * Each row maps directly to one operation.
   */
  private List<DeferredIndexOperation> mapOperations(ResultSet rs) throws SQLException {
    List<DeferredIndexOperation> result = new ArrayList<>();

    while (rs.next()) {
      DeferredIndexOperation op = new DeferredIndexOperation();
      op.setId(rs.getLong(COL_ID));
      op.setUpgradeUUID(rs.getString(COL_UPGRADE_UUID));
      op.setTableName(rs.getString(COL_TABLE_NAME));
      op.setIndexName(rs.getString(COL_INDEX_NAME));
      op.setIndexUnique(rs.getBoolean(COL_INDEX_UNIQUE));
      op.setColumnNames(Arrays.asList(rs.getString(COL_INDEX_COLUMNS).split(",")));
      op.setStatus(DeferredIndexStatus.valueOf(rs.getString(COL_STATUS)));
      op.setRetryCount(rs.getInt(COL_RETRY_COUNT));
      op.setCreatedTime(rs.getLong(COL_CREATED_TIME));
      long startedTime = rs.getLong(COL_STARTED_TIME);
      op.setStartedTime(rs.wasNull() ? null : startedTime);
      long completedTime = rs.getLong(COL_COMPLETED_TIME);
      op.setCompletedTime(rs.wasNull() ? null : completedTime);
      op.setErrorMessage(rs.getString(COL_ERROR_MESSAGE));
      result.add(op);
    }

    return result;
  }
}
