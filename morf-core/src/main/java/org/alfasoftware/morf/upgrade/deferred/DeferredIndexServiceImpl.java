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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexService}.
 *
 * <p>Orchestrates execution and validation of deferred index operations.
 * Crash recovery (IN_PROGRESS → PENDING reset) is handled by the executor.
 * Configuration is validated when {@link #execute()} is called.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexServiceImpl implements DeferredIndexService {

  private static final Log log = LogFactory.getLog(DeferredIndexServiceImpl.class);

  private final DeferredIndexExecutor executor;
  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexExecutionConfig config;

  /** Future representing the current execution; {@code null} if not started. */
  private volatile CompletableFuture<Void> executionFuture;


  /**
   * Constructs the service.
   *
   * @param executor executor for building deferred indexes.
   * @param dao      DAO for querying deferred index operation state.
   * @param config   configuration for deferred index execution.
   */
  @Inject
  DeferredIndexServiceImpl(DeferredIndexExecutor executor,
                            DeferredIndexOperationDAO dao,
                            DeferredIndexExecutionConfig config) {
    this.executor = executor;
    this.dao = dao;
    this.config = config;
  }


  @Override
  public void execute() {
    validateConfig(config);

    log.info("Deferred index service: executing pending operations...");
    executionFuture = executor.execute();
  }


  @Override
  public boolean awaitCompletion(long timeoutSeconds) {
    CompletableFuture<Void> future = executionFuture;
    if (future == null) {
      throw new IllegalStateException("awaitCompletion() called before execute()");
    }

    log.info("Deferred index service: awaiting completion (timeout=" + timeoutSeconds + "s)...");

    try {
      if (timeoutSeconds > 0L) {
        future.get(timeoutSeconds, TimeUnit.SECONDS);
      } else {
        future.get();
      }
      log.info("Deferred index service: all operations complete.");
      return true;

    } catch (TimeoutException e) {
      log.warn("Deferred index service: timed out waiting for operations to complete.");
      return false;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;

    } catch (ExecutionException e) {
      throw new IllegalStateException("Deferred index execution failed unexpectedly.", e.getCause());
    }
  }


  @Override
  public Map<DeferredIndexStatus, Integer> getProgress() {
    return dao.countAllByStatus();
  }


  /**
   * Validates that all configuration values are within acceptable ranges.
   *
   * @param config the configuration to validate.
   * @throws IllegalArgumentException if any value is out of range.
   */
  private void validateConfig(DeferredIndexExecutionConfig config) {
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
    if (config.getExecutionTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "executionTimeoutSeconds must be > 0 s, was " + config.getExecutionTimeoutSeconds() + " s");
    }
  }
}
