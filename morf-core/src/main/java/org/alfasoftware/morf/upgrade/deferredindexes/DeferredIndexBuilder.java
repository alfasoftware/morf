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

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Stateless service that performs one-pass reconciliation of a deferred-index
 * registration row against the physical schema. Holds no per-task state -- a
 * single instance is shared across every task fan-out.
 *
 * <p>Algorithm in {@link #build(DeferredIndex)}:</p>
 * <ol>
 *   <li>Open a JDBC connection (autocommit on for PostgreSQL CREATE INDEX
 *       CONCURRENTLY; left alone otherwise).</li>
 *   <li>Re-fetch the registration row -- live state may have advanced since
 *       the snapshot was captured by the service.</li>
 *   <li>If the row is missing or {@code COMPLETED}, return.</li>
 *   <li>Read the physical state via
 *       {@link SqlDialect#isIndexValid(Connection, String, String)}.</li>
 *   <li>Dispatch to {@link #buildAbsent} (CREATE) or {@link #rebuildInvalid}
 *       (DROP+CREATE), or just {@code markCompleted} when the physical is
 *       already valid.</li>
 * </ol>
 *
 * <p>Expected SQL outcomes (lock timeouts, unique-constraint violations) are
 * caught and persisted to {@code status} + {@code errorMessage}. Unexpected
 * runtime errors propagate as {@link RuntimeException} for the adopter's
 * executor to handle.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexBuilder {

  private static final Log log = LogFactory.getLog(DeferredIndexBuilder.class);

  /**
   * Bound on how long {@code DROP INDEX} waits for an interfering lock on a
   * dialect that supports a session lock timeout (PostgreSQL). Short enough to
   * fail-fast when an in-flight build still holds the index, long enough that
   * routine momentary contention isn't mistaken for a stuck build.
   */
  static final Duration LOCK_TIMEOUT = Duration.ofSeconds(10);

  private final ConnectionResources connectionResources;
  private final DeferredIndexesDAO dao;


  /**
   * @param connectionResources opens connections for {@link #build}.
   * @param dao persists the row state transitions.
   */
  @Inject
  DeferredIndexBuilder(ConnectionResources connectionResources, DeferredIndexesDAO dao) {
    this.connectionResources = connectionResources;
    this.dao = dao;
  }


  /**
   * Performs one reconciliation pass for the supplied registration row. Opens
   * its own connection (autocommit-gated by dialect), re-fetches the row state,
   * observes the physical index, and reconciles.
   *
   * @param snapshot the row state captured at task-creation time. This method
   *     re-fetches the row before acting; the snapshot is only used for the
   *     ({@code tableName}, {@code indexName}) pair.
   */
  void build(DeferredIndex snapshot) {
    SqlDialect dialect = connectionResources.sqlDialect();
    DataSource dataSource = connectionResources.getDataSource();
    String tableName = snapshot.getTableName();
    String indexName = snapshot.getIndexName();

    try (Connection connection = dataSource.getConnection()) {
      if (dialect.deferredIndexBuildRequiresAutoCommit()) {
        // PG CREATE INDEX CONCURRENTLY can't run in a transaction block.
        boolean priorAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        try {
          buildOnePass(connection, dialect, snapshot);
        } finally {
          connection.setAutoCommit(priorAutoCommit);
        }
      } else {
        buildOnePass(connection, dialect, snapshot);
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException(
          "Error reconciling deferred index [" + tableName + "." + indexName + "]", e);
    }
  }


  /** Re-fetch + observe-and-dispatch loop body, separated from connection management. */
  private void buildOnePass(Connection connection, SqlDialect dialect, DeferredIndex snapshot) {
    String tableName = snapshot.getTableName();
    String indexName = snapshot.getIndexName();
    Optional<DeferredIndex> rowOpt = dao.findByTableAndIndex(tableName, indexName);
    if (rowOpt.isEmpty()) {
      log.debug("No registration row for [" + tableName + "." + indexName + "] — nothing to reconcile");
      return;
    }
    DeferredIndex row = rowOpt.get();
    if (row.getStatus() == DeferredIndexStatus.COMPLETED) {
      return;
    }

    Optional<Boolean> validity = dialect.isIndexValid(connection, tableName, indexName);
    if (validity.isEmpty()) {
      buildAbsent(connection, dialect, row);
    } else if (Boolean.TRUE.equals(validity.get())) {
      // Physical index already in place -- declare success and reset attempts.
      dao.markCompleted(tableName, indexName, System.currentTimeMillis());
    } else {
      rebuildInvalid(connection, dialect, row);
    }
  }


  /** Physical absent path: bump attempts, run CREATE INDEX, persist outcome. */
  private void buildAbsent(Connection connection, SqlDialect dialect, DeferredIndex row) {
    String tableName = row.getTableName();
    String indexName = row.getIndexName();
    dao.markStarted(tableName, indexName, System.currentTimeMillis(), row.getAttemptsCount() + 1);
    Table table = table(tableName);
    Index index = row.toIndex();
    try {
      execute(connection, dialect.deferredIndexDeploymentStatements(table, index));
      dao.markCompleted(tableName, indexName, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("CREATE INDEX failed for [" + tableName + "." + indexName + "]: " + e.getMessage());
      dao.markFailed(tableName, indexName, e.getMessage());
    }
  }


  /**
   * Physical INVALID path: bump attempts, optionally bound DROP wait time
   * (PostgreSQL only), DROP, then CREATE. If DROP fails the next pass retries.
   * The lock_timeout (if set) is reset in a finally so the connection is safe
   * to return to the pool with no leftover session state.
   */
  private void rebuildInvalid(Connection connection, SqlDialect dialect, DeferredIndex row) {
    String tableName = row.getTableName();
    String indexName = row.getIndexName();
    dao.markStarted(tableName, indexName, System.currentTimeMillis(), row.getAttemptsCount() + 1);
    Table table = table(tableName);
    Index index = row.toIndex();

    boolean lockTimeoutSet = setLockTimeout(connection, dialect, tableName, indexName);
    try {
      if (!dropInvalidIndex(connection, dialect, table, index, tableName, indexName)) {
        return;
      }
      createIndex(connection, dialect, table, index, tableName, indexName);
    } finally {
      if (lockTimeoutSet) {
        resetLockTimeout(connection, dialect, tableName, indexName);
      }
    }
  }


  /** Apply the dialect-defined session lock_timeout, if any. Returns true iff it was set. */
  private boolean setLockTimeout(Connection connection, SqlDialect dialect, String tableName, String indexName) {
    Optional<String> lockTimeoutSql = dialect.setLockTimeoutSql(LOCK_TIMEOUT);
    if (lockTimeoutSql.isEmpty()) return false;
    try {
      execute(connection, lockTimeoutSql.get());
      return true;
    } catch (SQLException e) {
      // Best-effort safety net; proceed with dialect default.
      log.debug("Could not set lock_timeout for [" + tableName + "." + indexName + "]: " + e.getMessage());
      return false;
    }
  }


  /**
   * Reset the lock_timeout we set above so the connection is safe to return to
   * the pool with no leftover session state. Best effort -- a failure here is
   * logged but doesn't propagate.
   */
  private void resetLockTimeout(Connection connection, SqlDialect dialect, String tableName, String indexName) {
    dialect.resetLockTimeoutSql().ifPresent(reset -> {
      try {
        execute(connection, reset);
      } catch (SQLException e) {
        log.warn("Could not reset lock_timeout on connection for [" + tableName + "." + indexName
            + "]: " + e.getMessage() + " — connection will be discarded by the pool");
      }
    });
  }


  /**
   * DROP an invalid leftover physical index. Returns false (and persists FAILED
   * with an explanatory prefix) on failure so the caller stops the rebuild.
   */
  private boolean dropInvalidIndex(Connection connection, SqlDialect dialect,
                                   Table table, Index index,
                                   String tableName, String indexName) {
    try {
      execute(connection, dialect.indexDropStatements(table, index));
      return true;
    } catch (SQLException e) {
      log.warn("DROP INDEX failed for invalid leftover [" + tableName + "." + indexName + "]: " + e.getMessage());
      dao.markFailed(tableName, indexName, "could not drop invalid leftover: " + e.getMessage());
      return false;
    }
  }


  /** CREATE the index and mark COMPLETED, or mark FAILED on a SQL error. */
  private void createIndex(Connection connection, SqlDialect dialect,
                           Table table, Index index,
                           String tableName, String indexName) {
    try {
      execute(connection, dialect.deferredIndexDeploymentStatements(table, index));
      dao.markCompleted(tableName, indexName, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("CREATE INDEX failed for [" + tableName + "." + indexName + "]: " + e.getMessage());
      dao.markFailed(tableName, indexName, e.getMessage());
    }
  }


  /** Run a single SQL statement on the supplied connection. */
  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(sql);
    }
  }


  /** Run every SQL statement in {@code sqlList} on the supplied connection in order. */
  private static void execute(Connection connection, Iterable<String> sqlList) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      for (String sql : sqlList) {
        stmt.execute(sql);
      }
    }
  }
}
