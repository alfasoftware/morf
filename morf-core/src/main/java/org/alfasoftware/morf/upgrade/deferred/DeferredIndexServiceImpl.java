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

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexService}.
 *
 * <p>Orchestrates recovery, execution, and validation of deferred index
 * operations. All configuration is validated up front in the constructor.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexServiceImpl implements DeferredIndexService {

  private static final Log log = LogFactory.getLog(DeferredIndexServiceImpl.class);

  /** Polling interval used by {@link #awaitCompletion(long)}. */
  static final long AWAIT_POLL_INTERVAL_MS = 5_000L;

  private final DeferredIndexRecoveryService recoveryService;
  private final DeferredIndexExecutor executor;
  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexConfig config;


  /**
   * Constructs the service, validating all configuration parameters.
   *
   * @param recoveryService service for recovering stale operations.
   * @param executor        executor for building deferred indexes.
   * @param dao             DAO for deferred index operations.
   * @param config          configuration for deferred index execution.
   */
  @Inject
  DeferredIndexServiceImpl(DeferredIndexRecoveryService recoveryService,
                            DeferredIndexExecutor executor,
                            DeferredIndexOperationDAO dao,
                            DeferredIndexConfig config) {
    validateConfig(config);
    this.recoveryService = recoveryService;
    this.executor = executor;
    this.dao = dao;
    this.config = config;
  }


  @Override
  public ExecutionResult execute() {
    log.info("Deferred index service: starting recovery of stale operations...");
    recoveryService.recoverStaleOperations();

    log.info("Deferred index service: executing pending operations...");
    long timeoutMs = config.getExecutionTimeoutSeconds() * 1_000L;
    DeferredIndexExecutor.ExecutionResult executorResult = executor.executeAndWait(timeoutMs);

    int completed = executorResult.getCompletedCount();
    int failed = executorResult.getFailedCount();

    log.info("Deferred index service: execution complete — completed=" + completed + ", failed=" + failed);

    if (failed > 0) {
      throw new IllegalStateException("Deferred index execution failed: "
          + failed + " index operation(s) could not be built. "
          + "Resolve the underlying issue before retrying.");
    }

    return new ExecutionResult(completed, failed);
  }


  @Override
  public boolean awaitCompletion(long timeoutSeconds) {
    log.info("Deferred index service: awaiting completion (timeout=" + timeoutSeconds + "s)...");
    long deadline = timeoutSeconds > 0L ? System.currentTimeMillis() + timeoutSeconds * 1_000L : Long.MAX_VALUE;

    while (true) {
      if (!dao.hasNonTerminalOperations()) {
        log.info("Deferred index service: all operations complete.");
        return true;
      }

      long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0L) {
        log.warn("Deferred index service: timed out waiting for operations to complete.");
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


  private static void validateConfig(DeferredIndexConfig config) {
    if (config.getThreadPoolSize() < 1) {
      throw new IllegalArgumentException("threadPoolSize must be >= 1, was " + config.getThreadPoolSize());
    }
    if (config.getMaxRetries() < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0, was " + config.getMaxRetries());
    }
    if (config.getRetryBaseDelayMs() < 0) {
      throw new IllegalArgumentException("retryBaseDelayMs must be >= 0 ms, was " + config.getRetryBaseDelayMs() + " ms");
    }
    if (config.getRetryMaxDelayMs() < config.getRetryBaseDelayMs()) {
      throw new IllegalArgumentException("retryMaxDelayMs (" + config.getRetryMaxDelayMs()
          + " ms) must be >= retryBaseDelayMs (" + config.getRetryBaseDelayMs() + " ms)");
    }
    if (config.getStaleThresholdSeconds() <= 0) {
      throw new IllegalArgumentException(
          "staleThresholdSeconds must be > 0 s, was " + config.getStaleThresholdSeconds() + " s");
    }
    if (config.getExecutionTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "executionTimeoutSeconds must be > 0 s, was " + config.getExecutionTimeoutSeconds() + " s");
    }
  }
}
