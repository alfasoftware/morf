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
import org.alfasoftware.morf.sql.UpdateStatement;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeployedIndexesDAO}. A thin executor:
 * every method asks {@link DeployedIndexesStatementFactory} for the DSL,
 * converts to SQL via the dialect, and executes.
 *
 * <p>The only logic that lives here (and not in the factory) is
 * {@code ResultSet} mapping — that's post-execution handling, not DSL.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeployedIndexesDAOImpl implements DeployedIndexesDAO {

  private static final Log log = LogFactory.getLog(DeployedIndexesDAOImpl.class);

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;
  private final DeployedIndexesStatementFactory factory;


  /**
   * Constructs the DAO with injected dependencies.
   *
   * @param sqlScriptExecutorProvider provider for SQL script execution.
   * @param connectionResources connection resources (supplies the dialect).
   * @param factory statement factory — Guice-injected; tests supply a stub.
   */
  @Inject
  DeployedIndexesDAOImpl(SqlScriptExecutorProvider sqlScriptExecutorProvider,
                          ConnectionResources connectionResources,
                          DeployedIndexesStatementFactory factory) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = connectionResources.sqlDialect();
    this.factory = factory;
  }


  @Override
  public List<DeployedIndex> findAll() {
    return executeQuery(factory.statementToFindAll());
  }


  @Override
  public List<DeployedIndex> findByTable(String tableName) {
    return executeQuery(factory.statementToFindByTable(tableName));
  }


  @Override
  public List<DeployedIndex> findNonTerminalOperations() {
    return executeQuery(factory.statementToFindNonTerminalOperations());
  }


  @Override
  public Map<DeployedIndexStatus, Integer> countAllByStatus() {
    Map<DeployedIndexStatus, Integer> result = new EnumMap<>(DeployedIndexStatus.class);
    for (DeployedIndexStatus s : DeployedIndexStatus.values()) {
      result.put(s, 0);
    }

    String sql = sqlDialect.convertStatementToSQL(factory.statementToSelectStatusColumn());
    sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      while (rs.next()) {
        String statusStr = rs.getString(1);
        try {
          result.merge(DeployedIndexStatus.valueOf(statusStr), 1, Integer::sum);
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
    executeUpdate(factory.statementToMarkStarted(tableName, indexName, startedTime));
  }


  @Override
  public void markCompleted(String tableName, String indexName, long completedTime) {
    executeUpdate(factory.statementToMarkCompleted(tableName, indexName, completedTime));
  }


  @Override
  public void markFailed(String tableName, String indexName, String errorMessage) {
    executeUpdate(factory.statementToMarkFailed(tableName, indexName, errorMessage));
    executeUpdate(factory.statementToBumpRetryCount(tableName, indexName));
  }


  @Override
  public void resetAllInProgressToPending() {
    executeUpdate(factory.statementToResetInProgress());
    log.debug("Reset all IN_PROGRESS entries in DeployedIndexes to PENDING");
  }


  // -------------------------------------------------------------------------
  // Execution helpers
  // -------------------------------------------------------------------------

  private List<DeployedIndex> executeQuery(SelectStatement select) {
    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, this::mapEntries);
  }


  private void executeUpdate(UpdateStatement update) {
    executeSql(sqlDialect.convertStatementToSQL(update));
  }


  private void executeSql(String sql) {
    sqlScriptExecutorProvider.get().execute(List.of(sql));
  }


  private List<DeployedIndex> mapEntries(ResultSet rs) throws SQLException {
    List<DeployedIndex> result = new ArrayList<>();
    while (rs.next()) {
      DeployedIndex entry = new DeployedIndex();
      entry.setId(rs.getLong(DeployedIndexesStatementFactory.COL_ID));
      entry.setTableName(rs.getString(DeployedIndexesStatementFactory.COL_TABLE_NAME));
      entry.setIndexName(rs.getString(DeployedIndexesStatementFactory.COL_INDEX_NAME));
      entry.setIndexUnique(rs.getBoolean(DeployedIndexesStatementFactory.COL_INDEX_UNIQUE));
      entry.setIndexColumns(Arrays.asList(
          rs.getString(DeployedIndexesStatementFactory.COL_INDEX_COLUMNS).split(",")));
      entry.setStatus(DeployedIndexStatus.valueOf(
          rs.getString(DeployedIndexesStatementFactory.COL_STATUS)));
      entry.setRetryCount(rs.getInt(DeployedIndexesStatementFactory.COL_RETRY_COUNT));
      entry.setCreatedTime(rs.getLong(DeployedIndexesStatementFactory.COL_CREATED_TIME));

      long startedTime = rs.getLong(DeployedIndexesStatementFactory.COL_STARTED_TIME);
      entry.setStartedTime(rs.wasNull() ? null : startedTime);

      long completedTime = rs.getLong(DeployedIndexesStatementFactory.COL_COMPLETED_TIME);
      entry.setCompletedTime(rs.wasNull() ? null : completedTime);

      entry.setErrorMessage(rs.getString(DeployedIndexesStatementFactory.COL_ERROR_MESSAGE));
      result.add(entry);
    }
    return result;
  }
}
