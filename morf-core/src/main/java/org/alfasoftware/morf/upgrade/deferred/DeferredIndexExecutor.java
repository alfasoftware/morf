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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Executes pending deferred index operations queued in the
 * {@code DeferredIndexOperation} table by picking them up, issuing the
 * appropriate {@code CREATE INDEX} DDL via
 * {@link SqlDialect#deferredIndexDeploymentStatements(Table, Index)}, and
 * marking each operation as {@link DeferredIndexStatus#COMPLETED} or
 * {@link DeferredIndexStatus#FAILED}.
 *
 * <p>Retry logic uses exponential back-off up to
 * {@link DeferredIndexConfig#getMaxRetries()} additional attempts after the
 * first failure. Progress is logged at INFO level every 30 seconds (DEBUG
 * additionally logs per-operation details).</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, connectionResources, config);
 * ExecutionResult result = executor.executeAndWait(600_000L);
 * log.info("Completed: " + result.getCompletedCount() + ", failed: " + result.getFailedCount());
 * </pre>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexExecutor {

  private static final Log log = LogFactory.getLog(DeferredIndexExecutor.class);

  /** Progress is logged on this fixed interval. */
  private static final int PROGRESS_LOG_INTERVAL_SECONDS = 30;

  /** Polling interval used by {@link #awaitCompletion(long)}. */
  private static final long AWAIT_POLL_INTERVAL_MS = 5_000L;

  private final DeferredIndexOperationDAO dao;
  private final SqlDialect sqlDialect;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final DataSource dataSource;
  private final DeferredIndexConfig config;

  /** Count of operations completed in the current {@link #executeAndWait} call. */
  private final AtomicInteger completedCount = new AtomicInteger(0);

  /** Count of operations permanently failed in the current {@link #executeAndWait} call. */
  private final AtomicInteger failedCount = new AtomicInteger(0);

  /** Total operations submitted in the current {@link #executeAndWait} call. */
  private final AtomicInteger totalCount = new AtomicInteger(0);

  /**
   * Operations currently executing, keyed by id.
   * Used for progress-log detail at DEBUG level.
   */
  private final ConcurrentHashMap<Long, RunningOperation> runningOperations = new ConcurrentHashMap<>();

  /** The scheduled progress logger; may be null if execution has not started. */
  private volatile ScheduledExecutorService progressLoggerService;


  /**
   * Constructs an executor using the supplied connection and configuration.
   *
   * @param dao                 DAO for deferred index operations.
   * @param connectionResources database connection resources.
   * @param config              configuration controlling retry, thread-pool, and timeout behaviour.
   */
  @Inject
  DeferredIndexExecutor(DeferredIndexOperationDAO dao, ConnectionResources connectionResources,
                         DeferredIndexConfig config) {
    this.dao = dao;
    this.sqlDialect = connectionResources.sqlDialect();
    this.sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
    this.dataSource = connectionResources.getDataSource();
    this.config = config;
  }


  /**
   * Package-private constructor for unit testing with mock dependencies.
   */
  DeferredIndexExecutor(DeferredIndexOperationDAO dao, SqlDialect sqlDialect,
                         SqlScriptExecutorProvider sqlScriptExecutorProvider, DataSource dataSource,
                         DeferredIndexConfig config) {
    this.dao = dao;
    this.sqlDialect = sqlDialect;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.dataSource = dataSource;
    this.config = config;
  }


  /**
   * Picks up all {@link DeferredIndexStatus#PENDING} operations, builds the
   * corresponding indexes, and blocks until all operations reach a terminal
   * state or the timeout elapses.
   *
   * <p>Operations are submitted to a fixed thread pool whose size is governed
   * by {@link DeferredIndexConfig#getThreadPoolSize()}. Each operation is
   * retried up to {@link DeferredIndexConfig#getMaxRetries()} times on failure
   * using exponential back-off.</p>
   *
   * @param timeoutMs maximum time in milliseconds to wait for all operations to
   *                  complete; zero means wait indefinitely.
   * @return summary of how many operations completed and how many failed.
   */
  public ExecutionResult executeAndWait(long timeoutMs) {
    completedCount.set(0);
    failedCount.set(0);
    runningOperations.clear();

    List<DeferredIndexOperation> pending = dao.findPendingOperations();
    totalCount.set(pending.size());

    if (pending.isEmpty()) {
      return new ExecutionResult(0, 0);
    }

    progressLoggerService = startProgressLogger();

    ExecutorService threadPool = Executors.newFixedThreadPool(config.getThreadPoolSize(), r -> {
      Thread t = new Thread(r, "DeferredIndexExecutor");
      t.setDaemon(true);
      return t;
    });

    List<Future<?>> futures = new ArrayList<>(pending.size());
    for (DeferredIndexOperation op : pending) {
      futures.add(threadPool.submit(() -> executeWithRetry(op)));
    }

    awaitFutures(futures, timeoutMs);

    threadPool.shutdownNow();
    progressLoggerService.shutdownNow();

    return new ExecutionResult(completedCount.get(), failedCount.get());
  }


  /**
   * Blocks until all operations in the {@code DeferredIndexOperation} table are
   * in a terminal state ({@link DeferredIndexStatus#COMPLETED} or
   * {@link DeferredIndexStatus#FAILED}), or until the timeout elapses. This
   * method does <em>not</em> start or trigger execution — it is a passive observer
   * intended for multi-instance deployments where other nodes must wait at startup
   * until the index queue is drained.
   *
   * <p>Returns {@code true} immediately if the queue contains no PENDING or
   * IN_PROGRESS operations.</p>
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations reached a terminal state within the
   *         timeout; {@code false} if the timeout elapsed first.
   */
  public boolean awaitCompletion(long timeoutSeconds) {
    long deadline = timeoutSeconds > 0L ? System.currentTimeMillis() + timeoutSeconds * 1_000L : Long.MAX_VALUE;

    while (true) {
      if (!dao.hasNonTerminalOperations()) {
        return true;
      }

      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0L) {
        return false;
      }

      try {
        Thread.sleep(Math.min(AWAIT_POLL_INTERVAL_MS, remaining));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }


  /**
   * Returns a snapshot of the execution progress for the current or most recent
   * {@link #executeAndWait} call.
   *
   * @return current {@link ExecutionStatus}.
   */
  public ExecutionStatus getStatus() {
    int total = totalCount.get();
    int completed = completedCount.get();
    int failed = failedCount.get();
    int inProgress = runningOperations.size();
    return new ExecutionStatus(total, completed, inProgress, failed);
  }


  /**
   * Shuts down any background progress-logger thread started by the most recent
   * {@link #executeAndWait} call.
   */
  public void shutdown() {
    ScheduledExecutorService svc = progressLoggerService;
    if (svc != null) {
      svc.shutdownNow();
    }
  }


  // -------------------------------------------------------------------------
  // Internal execution logic
  // -------------------------------------------------------------------------

  private void executeWithRetry(DeferredIndexOperation op) {
    int maxAttempts = config.getMaxRetries() + 1;

    for (int attempt = op.getRetryCount(); attempt < maxAttempts; attempt++) {
      long startedTime = DeferredIndexTimestamps.currentTimestamp();
      dao.markStarted(op.getId(), startedTime);
      runningOperations.put(op.getId(), new RunningOperation(op, System.currentTimeMillis()));

      try {
        buildIndex(op);
        runningOperations.remove(op.getId());
        dao.markCompleted(op.getId(), DeferredIndexTimestamps.currentTimestamp());
        completedCount.incrementAndGet();
        return;

      } catch (Exception e) {
        runningOperations.remove(op.getId());
        int newRetryCount = attempt + 1;
        String errorMessage = truncate(e.getMessage(), 2_000);
        dao.markFailed(op.getId(), errorMessage, newRetryCount);

        if (newRetryCount < maxAttempts) {
          dao.resetToPending(op.getId());
          sleepForBackoff(attempt);
        } else {
          failedCount.incrementAndGet();
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


  private void awaitFutures(List<Future<?>> futures, long timeoutMs) {
    long deadline = timeoutMs > 0L ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;

    for (Future<?> future : futures) {
      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0L) {
        break;
      }
      try {
        future.get(remaining, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        log.warn("Unexpected error in deferred index executor worker", e.getCause());
      }
    }
  }


  private ScheduledExecutorService startProgressLogger() {
    ScheduledExecutorService svc = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "DeferredIndexProgressLogger");
      t.setDaemon(true);
      return t;
    });
    svc.scheduleAtFixedRate(this::logProgress,
        PROGRESS_LOG_INTERVAL_SECONDS, PROGRESS_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    return svc;
  }


  void logProgress() {
    int total = totalCount.get();
    int completed = completedCount.get();
    int failed = failedCount.get();
    int inProgress = runningOperations.size();
    int pending = total - completed - failed - inProgress;

    log.info("Deferred index progress: total=" + total + ", completed=" + completed
        + ", in-progress=" + inProgress + ", failed=" + failed + ", pending=" + pending);

    if (log.isDebugEnabled()) {
      long now = System.currentTimeMillis();
      for (RunningOperation running : runningOperations.values()) {
        long elapsedMs = now - running.startedAtMs;
        log.debug("  In-progress: table=" + running.op.getTableName()
            + ", index=" + running.op.getIndexName()
            + ", columns=" + running.op.getColumnNames()
            + ", elapsed=" + elapsedMs + "ms");
      }
    }
  }


  static String truncate(String message, int maxLength) {
    if (message == null) {
      return "";
    }
    return message.length() > maxLength ? message.substring(0, maxLength) : message;
  }


  // -------------------------------------------------------------------------
  // Inner types
  // -------------------------------------------------------------------------

  /** Tracks an operation currently being executed, for progress logging. */
  private static final class RunningOperation {
    final DeferredIndexOperation op;
    final long startedAtMs;

    RunningOperation(DeferredIndexOperation op, long startedAtMs) {
      this.op = op;
      this.startedAtMs = startedAtMs;
    }
  }


  /**
   * Summary of the outcome of an {@link DeferredIndexExecutor#executeAndWait} call.
   */
  public static final class ExecutionResult {

    private final int completedCount;
    private final int failedCount;

    ExecutionResult(int completedCount, int failedCount) {
      this.completedCount = completedCount;
      this.failedCount = failedCount;
    }

    /**
     * @return the number of operations that completed successfully.
     */
    public int getCompletedCount() {
      return completedCount;
    }

    /**
     * @return the number of operations that failed permanently.
     */
    public int getFailedCount() {
      return failedCount;
    }
  }


  /**
   * Snapshot of execution progress at a point in time.
   */
  public static final class ExecutionStatus {

    private final int totalCount;
    private final int completedCount;
    private final int inProgressCount;
    private final int failedCount;

    ExecutionStatus(int totalCount, int completedCount, int inProgressCount, int failedCount) {
      this.totalCount = totalCount;
      this.completedCount = completedCount;
      this.inProgressCount = inProgressCount;
      this.failedCount = failedCount;
    }

    /**
     * @return total operations submitted in this execution run.
     */
    public int getTotalCount() {
      return totalCount;
    }

    /**
     * @return operations completed successfully so far.
     */
    public int getCompletedCount() {
      return completedCount;
    }

    /**
     * @return operations currently executing.
     */
    public int getInProgressCount() {
      return inProgressCount;
    }

    /**
     * @return operations permanently failed so far.
     */
    public int getFailedCount() {
      return failedCount;
    }
  }
}
