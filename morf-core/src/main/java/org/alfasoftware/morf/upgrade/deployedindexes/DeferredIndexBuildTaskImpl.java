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

import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Package-private build task for one deferred index. Each instance is bound
 * to a single ({@code tableName}, {@code indexName}) pair and reconciles
 * that row's tracked state with the physical schema each time {@link #run()}
 * is called.
 *
 * <p>Algorithm:</p>
 * <ol>
 *   <li>Open a JDBC connection (autocommit on — required for PostgreSQL
 *       {@code CREATE INDEX CONCURRENTLY}).</li>
 *   <li>Re-fetch the tracking row (state may have changed since the service
 *       handed out this task).</li>
 *   <li>If the row is missing or {@code COMPLETED}, return — nothing to do.</li>
 *   <li>Read the physical state via
 *       {@link SqlDialect#isIndexValid(Connection, String, String)}.</li>
 *   <li>Dispatch:
 *     <ul>
 *       <li>{@code VALID} → mark COMPLETED.</li>
 *       <li>{@code ABSENT} → mark IN_PROGRESS, run CREATE INDEX, mark
 *           COMPLETED on success / FAILED on SQL error.</li>
 *       <li>{@code INVALID} → mark IN_PROGRESS, optionally
 *           {@link SqlDialect#setLockTimeoutSql} (PostgreSQL only), DROP, then
 *           CREATE. On DROP failure, mark FAILED with an explanatory prefix
 *           and stop — the next pass retries.</li>
 *     </ul>
 *   </li>
 *   <li>Close the connection.</li>
 * </ol>
 *
 * <p>Expected SQL outcomes (lock timeouts, unique-constraint violations, etc.)
 * are caught inside the task and persisted to {@code status} +
 * {@code errorMessage}. Unexpected runtime errors propagate as
 * {@link RuntimeException} for the adopter's executor to handle.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
class DeferredIndexBuildTaskImpl implements DeferredIndexBuildTask {

  private static final Log log = LogFactory.getLog(DeferredIndexBuildTaskImpl.class);

  /**
   * Bound on how long {@code DROP INDEX} waits for an interfering lock on a
   * dialect that supports a session lock timeout (PostgreSQL). Short enough to
   * fail-fast when an in-flight build still holds the index, long enough that
   * routine momentary contention isn't mistaken for a stuck build.
   */
  static final Duration LOCK_TIMEOUT = Duration.ofSeconds(10);

  private final String tableName;
  private final String indexName;
  private final ConnectionResources connectionResources;
  private final DeployedIndexesDAO dao;


  DeferredIndexBuildTaskImpl(String tableName,
                             String indexName,
                             ConnectionResources connectionResources,
                             DeployedIndexesDAO dao) {
    this.tableName = tableName;
    this.indexName = indexName;
    this.connectionResources = connectionResources;
    this.dao = dao;
  }


  @Override
  public String getTableName() {
    return tableName;
  }


  @Override
  public String getIndexName() {
    return indexName;
  }


  @Override
  public void run() {
    SqlDialect dialect = connectionResources.sqlDialect();
    DataSource dataSource = connectionResources.getDataSource();

    try (Connection connection = dataSource.getConnection()) {
      // PG CREATE INDEX CONCURRENTLY can't run in a transaction block.
      boolean priorAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(true);
      try {
        reconcile(connection, dialect);
      } finally {
        connection.setAutoCommit(priorAutoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException(
          "Error reconciling deferred index [" + tableName + "." + indexName + "]", e);
    }
  }


  private void reconcile(Connection connection, SqlDialect dialect) {
    Optional<DeployedIndex> rowOpt = dao.findByTableAndIndex(tableName, indexName);
    if (rowOpt.isEmpty()) {
      log.debug("No tracking row for [" + tableName + "." + indexName + "] — nothing to reconcile");
      return;
    }
    DeployedIndex row = rowOpt.get();
    if (row.getStatus() == DeployedIndexStatus.COMPLETED) {
      return;
    }

    Optional<Boolean> validity = dialect.isIndexValid(connection, tableName, indexName);
    if (validity.isEmpty()) {
      buildAbsent(connection, dialect, row);
    } else if (Boolean.TRUE.equals(validity.get())) {
      // Physical index already in place — declare success and reset attempts.
      dao.markCompleted(tableName, indexName, System.currentTimeMillis());
    } else {
      rebuildInvalid(connection, dialect, row);
    }
  }


  private void buildAbsent(Connection connection, SqlDialect dialect, DeployedIndex row) {
    dao.markStarted(tableName, indexName, System.currentTimeMillis(), row.getAttemptsCount() + 1);
    Table table = table(tableName);
    Index index = row.toIndex();
    try {
      executeAll(connection, dialect.deferredIndexDeploymentStatements(table, index));
      dao.markCompleted(tableName, indexName, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("CREATE INDEX failed for [" + tableName + "." + indexName + "]: " + e.getMessage());
      dao.markFailed(tableName, indexName, e.getMessage());
    }
  }


  private void rebuildInvalid(Connection connection, SqlDialect dialect, DeployedIndex row) {
    dao.markStarted(tableName, indexName, System.currentTimeMillis(), row.getAttemptsCount() + 1);
    Table table = table(tableName);
    Index index = row.toIndex();

    Optional<String> lockTimeoutSql = dialect.setLockTimeoutSql(LOCK_TIMEOUT);
    boolean lockTimeoutSet = false;
    if (lockTimeoutSql.isPresent()) {
      try {
        executeOne(connection, lockTimeoutSql.get());
        lockTimeoutSet = true;
      } catch (SQLException e) {
        // Fail-fast safety net is best-effort; proceed with dialect default.
        log.debug("Could not set lock_timeout for [" + tableName + "." + indexName + "]: " + e.getMessage());
      }
    }

    try {
      try {
        executeAll(connection, dialect.indexDropStatements(table, index));
      } catch (SQLException e) {
        log.warn("DROP INDEX failed for invalid leftover [" + tableName + "." + indexName + "]: " + e.getMessage());
        dao.markFailed(tableName, indexName, "could not drop invalid leftover: " + e.getMessage());
        return;
      }

      try {
        executeAll(connection, dialect.deferredIndexDeploymentStatements(table, index));
        dao.markCompleted(tableName, indexName, System.currentTimeMillis());
      } catch (SQLException e) {
        log.warn("CREATE INDEX failed for [" + tableName + "." + indexName + "]: " + e.getMessage());
        dao.markFailed(tableName, indexName, e.getMessage());
      }
    } finally {
      // Clear the session-scoped lock_timeout so it doesn't bleed back into pooled
      // connections — the next caller borrowing this connection would otherwise
      // inherit the 10s timeout we set above.
      if (lockTimeoutSet) {
        dialect.resetLockTimeoutSql().ifPresent(reset -> {
          try {
            executeOne(connection, reset);
          } catch (SQLException e) {
            log.warn("Could not reset lock_timeout on connection for [" + tableName + "." + indexName
                + "]: " + e.getMessage() + " — connection will be discarded by the pool");
          }
        });
      }
    }
  }


  private static void executeAll(Connection connection, Iterable<String> sqlList) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      for (String sql : sqlList) {
        stmt.execute(sql);
      }
    }
  }


  private static void executeOne(Connection connection, String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }
}
