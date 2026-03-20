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

import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexExecutor}.
 *
 * <p>Receives a pre-computed list of {@link DeferredAddIndex} operations,
 * issues the appropriate {@code CREATE INDEX} DDL via
 * {@link org.alfasoftware.morf.jdbc.SqlDialect#deferredIndexDeploymentStatements(Table, Index)},
 * and tracks progress with in-memory counters.</p>
 *
 * <p>Retry logic uses exponential back-off up to
 * {@link DeferredIndexExecutionConfig#getMaxRetries()} additional attempts after the
 * first failure. Progress is logged at INFO level after each operation
 * completes.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexExecutorImpl implements DeferredIndexExecutor {

  private static final Log log = LogFactory.getLog(DeferredIndexExecutorImpl.class);

  private final ConnectionResources connectionResources;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final DeferredIndexExecutionConfig config;
  private final DeferredIndexExecutorServiceFactory executorServiceFactory;

  /** The worker thread pool; may be null if execution has not started. */
  private volatile ExecutorService threadPool;

  /** In-memory progress counters, initialised per execute() call. */
  private final AtomicInteger completedCount = new AtomicInteger();
  private final AtomicInteger failedCount = new AtomicInteger();
  private volatile int totalCount;

  /** Per-operation in-memory retry counts keyed by "tableName/indexName". */
  private final Map<String, AtomicInteger> retryCounts = new ConcurrentHashMap<>();


  /**
   * Constructs an executor using the supplied dependencies.
   *
   * @param connectionResources      database connection resources.
   * @param sqlScriptExecutorProvider provider for SQL script executors.
   * @param config                   configuration controlling retry, thread-pool, and timeout behaviour.
   * @param executorServiceFactory   factory for creating the worker thread pool.
   */
  @Inject
  DeferredIndexExecutorImpl(ConnectionResources connectionResources,
                            SqlScriptExecutorProvider sqlScriptExecutorProvider,
                            DeferredIndexExecutionConfig config,
                            DeferredIndexExecutorServiceFactory executorServiceFactory) {
    this.connectionResources = connectionResources;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.config = config;
    this.executorServiceFactory = executorServiceFactory;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexExecutor#execute(List)
   */
  @Override
  public CompletableFuture<Void> execute(List<DeferredAddIndex> missingIndexes) {
    if (threadPool != null) {
      log.fatal("execute() called more than once on DeferredIndexExecutorImpl");
      throw new IllegalStateException("DeferredIndexExecutor.execute() has already been called");
    }

    if (missingIndexes.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    totalCount = missingIndexes.size();
    completedCount.set(0);
    failedCount.set(0);
    retryCounts.clear();

    threadPool = executorServiceFactory.create(config.getThreadPoolSize());

    CompletableFuture<?>[] futures = missingIndexes.stream()
        .map(dai -> CompletableFuture.runAsync(() -> {
          executeWithRetry(dai);
          logProgress();
        }, threadPool))
        .toArray(CompletableFuture[]::new);

    return CompletableFuture.allOf(futures)
        .whenComplete((v, t) -> {
          threadPool.shutdown();
          threadPool = null;
          logProgress();
          log.info("Deferred index execution complete.");
        });
  }


  /**
   * Returns the current completed count.
   *
   * @return number of indexes successfully built.
   */
  int getCompletedCount() {
    return completedCount.get();
  }


  /**
   * Returns the current failed count.
   *
   * @return number of indexes permanently failed.
   */
  int getFailedCount() {
    return failedCount.get();
  }


  /**
   * Returns the total count for the current execution.
   *
   * @return total number of indexes to build.
   */
  int getTotalCount() {
    return totalCount;
  }


  // -------------------------------------------------------------------------
  // Internal execution logic
  // -------------------------------------------------------------------------

  /**
   * Attempts to build the index for a single deferred add index, retrying with
   * exponential back-off on failure up to {@link DeferredIndexExecutionConfig#getMaxRetries()}
   * times. Tracks progress via in-memory counters.
   *
   * @param dai the deferred add index operation to execute.
   */
  private void executeWithRetry(DeferredAddIndex dai) {
    int maxAttempts = config.getMaxRetries() + 1;
    String key = dai.getTableName() + "/" + dai.getNewIndex().getName();
    AtomicInteger retryCounter = retryCounts.computeIfAbsent(key, k -> new AtomicInteger(0));

    for (int attempt = retryCounter.get(); attempt < maxAttempts; attempt++) {
      log.info("Starting deferred index build: table=" + dai.getTableName()
          + ", index=" + dai.getNewIndex().getName() + ", attempt=" + (attempt + 1) + "/" + maxAttempts);
      long startedTime = System.currentTimeMillis();

      try {
        buildIndex(dai);
        long elapsedSeconds = (System.currentTimeMillis() - startedTime) / 1000;
        completedCount.incrementAndGet();
        log.info("Deferred index build completed in " + elapsedSeconds
            + " s: table=" + dai.getTableName() + ", index=" + dai.getNewIndex().getName());
        return;

      } catch (Exception e) {
        long elapsedSeconds = (System.currentTimeMillis() - startedTime) / 1000;

        // Post-failure check: if the index actually exists in the database
        // (e.g. a previous crashed attempt completed the build), mark completed.
        if (indexExistsInDatabase(dai)) {
          completedCount.incrementAndGet();
          log.info("Deferred index build failed but index exists in database"
              + " — marking completed: table=" + dai.getTableName() + ", index=" + dai.getNewIndex().getName());
          return;
        }

        int newRetryCount = attempt + 1;
        retryCounter.set(newRetryCount);

        if (newRetryCount < maxAttempts) {
          log.error("Deferred index build failed after " + elapsedSeconds
              + " s (attempt " + newRetryCount + "/" + maxAttempts + "), will retry: table="
              + dai.getTableName() + ", index=" + dai.getNewIndex().getName() + ", error=" + e.getMessage());
          sleepForBackoff(attempt);
        } else {
          failedCount.incrementAndGet();
          log.error("Deferred index build permanently failed after " + elapsedSeconds + " s ("
              + newRetryCount + " attempt(s)): table=" + dai.getTableName()
              + ", index=" + dai.getNewIndex().getName(), e);
        }
      }
    }
  }


  /**
   * Executes the {@code CREATE INDEX} DDL for the given operation using an
   * autocommit connection. Autocommit is required for PostgreSQL's
   * {@code CREATE INDEX CONCURRENTLY}.
   *
   * @param dai the deferred add index containing table and index metadata.
   */
  private void buildIndex(DeferredAddIndex dai) {
    Index index = dai.getNewIndex();
    Table table = table(dai.getTableName());
    Collection<String> statements = connectionResources.sqlDialect().deferredIndexDeploymentStatements(table, index);

    try (Connection connection = connectionResources.getDataSource().getConnection()) {
      boolean wasAutoCommit = connection.getAutoCommit();
      try {
        connection.setAutoCommit(true);
        sqlScriptExecutorProvider.get().execute(statements, connection);
      } finally {
        connection.setAutoCommit(wasAutoCommit);
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error building deferred index " + dai.getNewIndex().getName(), e);
    }
  }


  /**
   * Checks whether the index described by the operation exists in the live
   * database schema. Used for post-failure recovery.
   *
   * @param dai the operation to check.
   * @return {@code true} if the index exists.
   */
  private boolean indexExistsInDatabase(DeferredAddIndex dai) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      if (!sr.tableExists(dai.getTableName())) {
        return false;
      }
      return sr.getTable(dai.getTableName()).indexes().stream()
          .anyMatch(idx -> idx.getName().equalsIgnoreCase(dai.getNewIndex().getName()));
    }
  }


  /**
   * Sleeps for an exponentially increasing delay, capped at
   * {@link DeferredIndexExecutionConfig#getRetryMaxDelayMs()}.
   *
   * @param attempt the zero-based attempt number (used to compute the delay).
   */
  private void sleepForBackoff(int attempt) {
    try {
      long delay = Math.min(config.getRetryBaseDelayMs() * (1L << Math.min(attempt, 30)), config.getRetryMaxDelayMs());
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }


  /**
   * Logs current progress at INFO level.
   */
  void logProgress() {
    int completed = completedCount.get();
    int failed = failedCount.get();
    int remaining = totalCount - completed - failed;

    log.info("Deferred index progress: completed=" + completed
        + ", failed=" + failed
        + ", remaining=" + remaining
        + ", total=" + totalCount);
  }
}
