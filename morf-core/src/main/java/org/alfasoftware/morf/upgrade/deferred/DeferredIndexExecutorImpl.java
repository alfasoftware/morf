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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.metadata.Table;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexExecutor}.
 *
 * <p>Picks up pending operations, issues the appropriate
 * {@code CREATE INDEX} DDL via
 * {@link SqlDialect#deferredIndexDeploymentStatements(Table, Index)}, and
 * marks each operation as {@link DeferredIndexStatus#COMPLETED} or
 * {@link DeferredIndexStatus#FAILED}.</p>
 *
 * <p>Retry logic uses exponential back-off up to
 * {@link DeferredIndexConfig#getMaxRetries()} additional attempts after the
 * first failure. Progress is logged at INFO level after each operation
 * completes.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexExecutorImpl implements DeferredIndexExecutor {

  private static final Log log = LogFactory.getLog(DeferredIndexExecutorImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final SqlDialect sqlDialect;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final DataSource dataSource;
  private final DeferredIndexConfig config;
  private final DeferredIndexExecutorServiceFactory executorServiceFactory;

  /** The worker thread pool; may be null if execution has not started. */
  private volatile ExecutorService threadPool;


  /**
   * Constructs an executor using the supplied dependencies.
   *
   * @param dao                      DAO for deferred index operations.
   * @param connectionResources      database connection resources.
   * @param sqlScriptExecutorProvider provider for SQL script executors.
   * @param config                   configuration controlling retry, thread-pool, and timeout behaviour.
   * @param executorServiceFactory   factory for creating the worker thread pool.
   */
  @Inject
  DeferredIndexExecutorImpl(DeferredIndexOperationDAO dao, ConnectionResources connectionResources,
                            SqlScriptExecutorProvider sqlScriptExecutorProvider,
                            DeferredIndexConfig config,
                            DeferredIndexExecutorServiceFactory executorServiceFactory) {
    this.dao = dao;
    this.sqlDialect = connectionResources.sqlDialect();
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.dataSource = connectionResources.getDataSource();
    this.config = config;
    this.executorServiceFactory = executorServiceFactory;
  }


  @Override
  public CompletableFuture<Void> execute() {
    List<DeferredIndexOperation> pending = dao.findPendingOperations();

    if (pending.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    threadPool = executorServiceFactory.create(config.getThreadPoolSize());

    CompletableFuture<?>[] futures = pending.stream()
        .map(op -> CompletableFuture.runAsync(() -> {
          executeWithRetry(op);
          logProgress();
        }, threadPool))
        .toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(futures)
        .whenComplete((v, t) -> threadPool.shutdown());
  }


  @Override
  public void shutdown() {
    ExecutorService pool = threadPool;
    if (pool != null) {
      pool.shutdownNow();
    }
  }


  // -------------------------------------------------------------------------
  // Internal execution logic
  // -------------------------------------------------------------------------

  private void executeWithRetry(DeferredIndexOperation op) {
    int maxAttempts = config.getMaxRetries() + 1;

    for (int attempt = op.getRetryCount(); attempt < maxAttempts; attempt++) {
      if (log.isDebugEnabled()) {
        log.debug("Starting deferred index operation [" + op.getId() + "]: table=" + op.getTableName()
            + ", index=" + op.getIndexName() + ", attempt=" + (attempt + 1) + "/" + maxAttempts);
      }
      long startedTime = System.currentTimeMillis();
      dao.markStarted(op.getId(), startedTime);

      try {
        buildIndex(op);
        dao.markCompleted(op.getId(), System.currentTimeMillis());
        if (log.isDebugEnabled()) {
          log.debug("Deferred index operation [" + op.getId() + "] completed: table=" + op.getTableName()
              + ", index=" + op.getIndexName());
        }
        return;

      } catch (Exception e) {
        int newRetryCount = attempt + 1;
        String errorMessage = truncate(e.getMessage(), 2_000);
        dao.markFailed(op.getId(), errorMessage, newRetryCount);

        if (newRetryCount < maxAttempts) {
          if (log.isDebugEnabled()) {
            log.debug("Deferred index operation [" + op.getId() + "] failed (attempt " + newRetryCount
                + "/" + maxAttempts + "), will retry: table=" + op.getTableName()
                + ", index=" + op.getIndexName() + ", error=" + errorMessage);
          }
          dao.resetToPending(op.getId());
          sleepForBackoff(attempt);
        } else {
          log.error("Deferred index operation permanently failed after " + newRetryCount
              + " attempt(s): table=" + op.getTableName() + ", index=" + op.getIndexName(), e);
        }
      }
    }
  }


  private void buildIndex(DeferredIndexOperation op) {
    Index index = reconstructIndex(op);
    Table table = table(op.getTableName());
    Collection<String> statements = sqlDialect.deferredIndexDeploymentStatements(table, index);

    // Execute with autocommit enabled rather than inside a transaction.
    // Some platforms require this — notably PostgreSQL's CREATE INDEX
    // CONCURRENTLY, which cannot run inside a transaction block. Using a
    // dedicated autocommit connection is harmless for platforms that do
    // not have this restriction (Oracle, MySQL, H2, SQL Server).
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(true);
      sqlScriptExecutorProvider.get().execute(statements, connection);
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error building deferred index " + op.getIndexName(), e);
    }
  }


  private static Index reconstructIndex(DeferredIndexOperation op) {
    IndexBuilder builder = index(op.getIndexName());
    if (op.isIndexUnique()) {
      builder = builder.unique();
    }
    return builder.columns(op.getColumnNames().toArray(new String[0]));
  }


  private void sleepForBackoff(int attempt) {
    try {
      long delay = Math.min(config.getRetryBaseDelayMs() * (1L << attempt), config.getRetryMaxDelayMs());
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }


  void logProgress() {
    Map<DeferredIndexStatus, Integer> counts = dao.countAllByStatus();

    log.info("Deferred index progress: completed=" + counts.get(DeferredIndexStatus.COMPLETED)
        + ", in-progress=" + counts.get(DeferredIndexStatus.IN_PROGRESS)
        + ", pending=" + counts.get(DeferredIndexStatus.PENDING)
        + ", failed=" + counts.get(DeferredIndexStatus.FAILED));
  }


  static String truncate(String message, int maxLength) {
    if (message == null) {
      return "";
    }
    return message.length() > maxLength ? message.substring(0, maxLength) : message;
  }
}
