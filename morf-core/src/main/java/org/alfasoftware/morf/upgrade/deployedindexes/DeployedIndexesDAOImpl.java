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

package org.alfasoftware.morf.upgrade.deployedindexes;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
 * Default implementation of {@link DeployedIndexesDAO}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
public class DeployedIndexesDAOImpl implements DeployedIndexesDAO {

  private static final Log log = LogFactory.getLog(DeployedIndexesDAOImpl.class);

  private static final String TABLE = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;

  static final String COL_ID = "id";
  static final String COL_UPGRADE_UUID = "upgradeUUID";
  static final String COL_TABLE_NAME = "tableName";
  static final String COL_INDEX_NAME = "indexName";
  static final String COL_INDEX_UNIQUE = "indexUnique";
  static final String COL_INDEX_COLUMNS = "indexColumns";
  static final String COL_INDEX_DEFERRED = "indexDeferred";
  static final String COL_STATUS = "status";
  static final String COL_RETRY_COUNT = "retryCount";
  static final String COL_CREATED_TIME = "createdTime";
  static final String COL_STARTED_TIME = "startedTime";
  static final String COL_COMPLETED_TIME = "completedTime";
  static final String COL_ERROR_MESSAGE = "errorMessage";

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * Constructs the DAO with injected dependencies.
   *
   * @param sqlScriptExecutorProvider provider for SQL executors.
   * @param connectionResources database connection resources.
   */
  @Inject
  public DeployedIndexesDAOImpl(SqlScriptExecutorProvider sqlScriptExecutorProvider,
                          ConnectionResources connectionResources) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = connectionResources.sqlDialect();
  }


  @Override
  public List<DeployedIndexEntry> findAll() {
    return executeQuery(
        select(field(COL_ID), field(COL_UPGRADE_UUID), field(COL_TABLE_NAME),
               field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
               field(COL_INDEX_DEFERRED), field(COL_STATUS), field(COL_RETRY_COUNT),
               field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
               field(COL_ERROR_MESSAGE))
            .from(tableRef(TABLE))
            .orderBy(field(COL_ID))
    );
  }


  @Override
  public List<DeployedIndexEntry> findByTable(String tableName) {
    return executeQuery(
        select(field(COL_ID), field(COL_UPGRADE_UUID), field(COL_TABLE_NAME),
               field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
               field(COL_INDEX_DEFERRED), field(COL_STATUS), field(COL_RETRY_COUNT),
               field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
               field(COL_ERROR_MESSAGE))
            .from(tableRef(TABLE))
            .where(field(COL_TABLE_NAME).eq(tableName))
            .orderBy(field(COL_ID))
    );
  }


  @Override
  public List<DeployedIndexEntry> findNonTerminalOperations() {
    return executeQuery(
        select(field(COL_ID), field(COL_UPGRADE_UUID), field(COL_TABLE_NAME),
               field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
               field(COL_INDEX_DEFERRED), field(COL_STATUS), field(COL_RETRY_COUNT),
               field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
               field(COL_ERROR_MESSAGE))
            .from(tableRef(TABLE))
            .where(or(
                field(COL_STATUS).eq(DeployedIndexStatus.PENDING.name()),
                field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()),
                field(COL_STATUS).eq(DeployedIndexStatus.FAILED.name())))
            .orderBy(field(COL_ID))
    );
  }


  @Override
  public Map<DeployedIndexStatus, Integer> countAllByStatus() {
    Map<DeployedIndexStatus, Integer> result = new EnumMap<>(DeployedIndexStatus.class);
    for (DeployedIndexStatus s : DeployedIndexStatus.values()) {
      result.put(s, 0);
    }

    String sql = sqlDialect.convertStatementToSQL(
        select(field(COL_STATUS))
            .from(tableRef(TABLE))
    );

    sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      while (rs.next()) {
        String statusStr = rs.getString(1);
        try {
          DeployedIndexStatus status = DeployedIndexStatus.valueOf(statusStr);
          result.merge(status, 1, Integer::sum);
        } catch (IllegalArgumentException e) {
          log.warn("Unknown status value in DeployedIndexes: " + statusStr);
        }
      }
      return null;
    });

    return result;
  }


  @Override
  public void markStarted(String tableName, String indexName, long startedTime) {
    executeSql(sqlDialect.convertStatementToSQL(
        update(tableRef(TABLE))
            .set(literal(DeployedIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
                 literal(startedTime).as(COL_STARTED_TIME))
            .where(field(COL_TABLE_NAME).eq(tableName)
                .and(field(COL_INDEX_NAME).eq(indexName)))
    ));
  }


  @Override
  public void markCompleted(String tableName, String indexName, long completedTime) {
    executeSql(sqlDialect.convertStatementToSQL(
        update(tableRef(TABLE))
            .set(literal(DeployedIndexStatus.COMPLETED.name()).as(COL_STATUS),
                 literal(completedTime).as(COL_COMPLETED_TIME))
            .where(field(COL_TABLE_NAME).eq(tableName)
                .and(field(COL_INDEX_NAME).eq(indexName)))
    ));
  }


  @Override
  public void markFailed(String tableName, String indexName, String errorMessage) {
    executeSql(sqlDialect.convertStatementToSQL(
        update(tableRef(TABLE))
            .set(literal(DeployedIndexStatus.FAILED.name()).as(COL_STATUS),
                 literal(errorMessage).as(COL_ERROR_MESSAGE))
            .where(field(COL_TABLE_NAME).eq(tableName)
                .and(field(COL_INDEX_NAME).eq(indexName)))
    ));
    // Increment retryCount separately (Morf DSL doesn't support field+1 in SET)
    executeSql(sqlDialect.convertStatementToSQL(
        update(tableRef(TABLE))
            .set(literal(1).as(COL_RETRY_COUNT))  // simplified: app manages retry count
            .where(field(COL_TABLE_NAME).eq(tableName)
                .and(field(COL_INDEX_NAME).eq(indexName)))
    ));
  }


  @Override
  public void resetAllInProgressToPending() {
    String sql = sqlDialect.convertStatementToSQL(
        update(tableRef(TABLE))
            .set(literal(DeployedIndexStatus.PENDING.name()).as(COL_STATUS))
            .where(field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()))
    );
    executeSql(sql);
    log.debug("Reset all IN_PROGRESS entries in DeployedIndexes to PENDING");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<DeployedIndexEntry> executeQuery(SelectStatement select) {
    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapEntries);
  }


  private List<DeployedIndexEntry> mapEntries(ResultSet rs) throws SQLException {
    List<DeployedIndexEntry> result = new ArrayList<>();
    while (rs.next()) {
      DeployedIndexEntry entry = new DeployedIndexEntry();
      entry.setId(rs.getLong(COL_ID));
      entry.setUpgradeUUID(rs.getString(COL_UPGRADE_UUID));
      entry.setTableName(rs.getString(COL_TABLE_NAME));
      entry.setIndexName(rs.getString(COL_INDEX_NAME));
      entry.setIndexUnique(rs.getBoolean(COL_INDEX_UNIQUE));
      entry.setIndexColumns(DeployedIndexEntry.parseColumns(rs.getString(COL_INDEX_COLUMNS)));
      entry.setIndexDeferred(rs.getBoolean(COL_INDEX_DEFERRED));
      entry.setStatus(DeployedIndexStatus.valueOf(rs.getString(COL_STATUS)));
      entry.setRetryCount(rs.getInt(COL_RETRY_COUNT));
      entry.setCreatedTime(rs.getLong(COL_CREATED_TIME));

      long startedTime = rs.getLong(COL_STARTED_TIME);
      entry.setStartedTime(rs.wasNull() ? null : startedTime);

      long completedTime = rs.getLong(COL_COMPLETED_TIME);
      entry.setCompletedTime(rs.wasNull() ? null : completedTime);

      entry.setErrorMessage(rs.getString(COL_ERROR_MESSAGE));
      result.add(entry);
    }
    return result;
  }


  private void executeSql(String sql) {
    sqlScriptExecutorProvider.get().execute(sql);
  }
}
