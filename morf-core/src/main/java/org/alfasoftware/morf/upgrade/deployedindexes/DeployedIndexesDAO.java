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
 * Package-private persistence layer for the {@code DeployedIndexes} table.
 * Executes every read and write via {@link SqlScriptExecutorProvider} and
 * {@link SqlDialect}; DSL construction and row mapping live in
 * {@link DeployedIndexesSql}.
 *
 * <p>A concrete class rather than an interface+impl pair — the previous
 * split served no behavioural purpose (every method was a 1-line wrapper)
 * and the class is package-private, so there's no adopter-facing contract
 * to model. Contributors inside this package depend on it directly;
 * {@link DeployedIndexTrackerImpl} delegates here, and
 * {@link DeployedIndexesModelEnricherImpl} injects it for the upgrade-start
 * {@link #findAll()} read.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeployedIndexesDAO {

  private static final Log log = LogFactory.getLog(DeployedIndexesDAO.class);

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * @param sqlScriptExecutorProvider provider for SQL script execution.
   * @param connectionResources connection resources (supplies the dialect).
   */
  @Inject
  DeployedIndexesDAO(SqlScriptExecutorProvider sqlScriptExecutorProvider,
                     ConnectionResources connectionResources) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = connectionResources.sqlDialect();
  }


  /** @return every persisted tracking row, ordered by id. */
  List<DeployedIndex> findAll() {
    return executeQuery(DeployedIndexesSql.selectAll());
  }


  /** @return non-terminal (PENDING/IN_PROGRESS/FAILED) rows, ordered by id. */
  List<DeployedIndex> findNonTerminal() {
    return executeQuery(DeployedIndexesSql.selectNonTerminal());
  }


  /** @return counts of every persisted row grouped by status. */
  Map<DeployedIndexStatus, Integer> getProgressCounts() {
    Map<DeployedIndexStatus, Integer> result = new EnumMap<>(DeployedIndexStatus.class);
    for (DeployedIndexStatus s : DeployedIndexStatus.values()) {
      result.put(s, 0);
    }

    String sql = sqlDialect.convertStatementToSQL(DeployedIndexesSql.selectStatusColumn());
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


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param startedTime epoch ms.
   */
  void markStarted(String tableName, String indexName, long startedTime) {
    executeUpdate(DeployedIndexesSql.markStarted(tableName, indexName, startedTime));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param completedTime epoch ms.
   */
  void markCompleted(String tableName, String indexName, long completedTime) {
    executeUpdate(DeployedIndexesSql.markCompleted(tableName, indexName, completedTime));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param errorMessage the failure message.
   */
  void markFailed(String tableName, String indexName, String errorMessage) {
    executeUpdate(DeployedIndexesSql.markFailed(tableName, indexName, errorMessage));
    executeUpdate(DeployedIndexesSql.bumpRetryCount(tableName, indexName));
  }


  /** Flips every IN_PROGRESS row back to PENDING. */
  void resetInProgress() {
    executeUpdate(DeployedIndexesSql.resetInProgress());
    log.debug("Reset all IN_PROGRESS entries in DeployedIndexes to PENDING");
  }


  // -------------------------------------------------------------------------
  // Execution helpers
  // -------------------------------------------------------------------------

  private List<DeployedIndex> executeQuery(SelectStatement select) {
    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, DeployedIndexesSql::mapAll);
  }


  private void executeUpdate(UpdateStatement update) {
    sqlScriptExecutorProvider.get().execute(List.of(sqlDialect.convertStatementToSQL(update)));
  }
}
