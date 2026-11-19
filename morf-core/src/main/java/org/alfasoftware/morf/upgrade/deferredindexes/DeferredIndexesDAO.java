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

package org.alfasoftware.morf.upgrade.deferredindexes;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
 * Package-private persistence layer for the {@code DeferredIndexes} table.
 * Executes every read and write via {@link SqlScriptExecutorProvider} and
 * {@link SqlDialect}; DSL construction and row mapping live in
 * {@link DeferredIndexesStatements}.
 *
 * <p>A concrete class rather than an interface+impl pair — the previous
 * split served no behavioural purpose (every method was a 1-line wrapper)
 * and the class is package-private, so there's no adopter-facing contract
 * to model. Contributors inside this package depend on it directly;
 * {@link DeferredIndexServiceImpl} fans these reads/writes out to adopter
 * threads via {@link DeferredIndexBuildTaskImpl}, and
 * {@link DeferredIndexesModelEnricherImpl} injects it for the upgrade-start
 * {@link #findAll()} read.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexesDAO {

  private static final Log log = LogFactory.getLog(DeferredIndexesDAO.class);

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;
  private final DeferredIndexesStatements statements;


  /**
   * @param sqlScriptExecutorProvider provider for SQL script execution.
   * @param connectionResources connection resources (supplies the dialect).
   * @param statements DSL + row-mapping helper for the DeferredIndexes table.
   */
  @Inject
  DeferredIndexesDAO(SqlScriptExecutorProvider sqlScriptExecutorProvider,
                     ConnectionResources connectionResources,
                     DeferredIndexesStatements statements) {
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.sqlDialect = connectionResources.sqlDialect();
    this.statements = statements;
  }


  /** @return every persisted registration row, ordered by id. */
  List<DeferredIndex> findAll() {
    return executeQuery(statements.selectAll());
  }


  /** @return non-terminal (PENDING/IN_PROGRESS/FAILED) rows, ordered by id. */
  List<DeferredIndex> findNonTerminal() {
    return executeQuery(statements.selectNonTerminal());
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return the single row matching ({@code tableName}, {@code indexName}),
   *     or empty if none.
   */
  Optional<DeferredIndex> findByTableAndIndex(String tableName, String indexName) {
    List<DeferredIndex> rows = executeQuery(statements.selectByTableAndIndex(tableName, indexName));
    return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
  }


  /** @return counts of every persisted row grouped by status. */
  Map<DeferredIndexStatus, Integer> getProgressCounts() {
    Map<DeferredIndexStatus, Integer> result = new EnumMap<>(DeferredIndexStatus.class);
    for (DeferredIndexStatus s : DeferredIndexStatus.values()) {
      result.put(s, 0);
    }

    String sql = sqlDialect.convertStatementToSQL(statements.selectStatusColumn());
    sqlScriptExecutorProvider.get().executeQuery(sql, rs -> {
      while (rs.next()) {
        String statusStr = rs.getString(1);
        try {
          result.merge(DeferredIndexStatus.valueOf(statusStr), 1, Integer::sum);
        } catch (IllegalArgumentException e) {
          log.warn("Unknown status value in DeferredIndexes: " + statusStr);
        }
      }
      return null;
    });
    return result;
  }


  /**
   * Marks the row IN_PROGRESS, records {@code startedTime}, and writes the
   * supplied {@code newAttemptsCount}. The build task computes
   * {@code newAttemptsCount} as {@code currentAttemptsCount + 1} from the row
   * it just re-fetched.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @param startedTime epoch ms when this attempt began.
   * @param newAttemptsCount the value to write into {@code attemptsCount}.
   */
  void markStarted(String tableName, String indexName, long startedTime, int newAttemptsCount) {
    executeUpdate(statements.markStarted(tableName, indexName, startedTime, newAttemptsCount));
  }


  /**
   * Marks the row COMPLETED, records {@code completedTime}, and clears the
   * recoverable-failure registration columns ({@code attemptsCount=0},
   * {@code errorMessage=NULL}).
   *
   * @param tableName the table.
   * @param indexName the index.
   * @param completedTime epoch ms.
   */
  void markCompleted(String tableName, String indexName, long completedTime) {
    executeUpdate(statements.markCompleted(tableName, indexName, completedTime));
  }


  /**
   * Marks the row FAILED with {@code errorMessage}. Does not touch
   * {@code attemptsCount} — that was already bumped by the matching
   * {@link #markStarted}.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @param errorMessage the failure message.
   */
  void markFailed(String tableName, String indexName, String errorMessage) {
    executeUpdate(statements.markFailed(tableName, indexName, errorMessage));
  }


  // -------------------------------------------------------------------------
  // Execution helpers
  // -------------------------------------------------------------------------

  private List<DeferredIndex> executeQuery(SelectStatement select) {
    String sql = sqlDialect.convertStatementToSQL(select);
    return sqlScriptExecutorProvider.get().executeQuery(sql, statements::mapAll);
  }


  private void executeUpdate(UpdateStatement update) {
    sqlScriptExecutorProvider.get().execute(List.of(sqlDialect.convertStatementToSQL(update)));
  }
}
