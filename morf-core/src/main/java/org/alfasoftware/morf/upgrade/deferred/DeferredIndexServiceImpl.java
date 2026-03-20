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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.alfasoftware.morf.upgrade.UpgradeStep;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexService}.
 *
 * <p>Orchestrates replay-based discovery and execution of deferred index
 * operations. Configuration is validated when {@link #execute()} is called.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexServiceImpl implements DeferredIndexService {

  private static final Log log = LogFactory.getLog(DeferredIndexServiceImpl.class);

  private final DeferredIndexExecutor executor;
  private final DeferredIndexExecutionConfig config;
  private final DeferredIndexReadinessCheck readinessCheck;

  /** Upgrade step classes set via {@link #setUpgradeSteps}. */
  private Collection<Class<? extends UpgradeStep>> upgradeSteps;

  /** Future representing the current execution; {@code null} if not started. */
  private CompletableFuture<Void> executionFuture;

  /** Total count of deferred indexes submitted in the last execute() call. */
  private volatile int totalSubmitted;


  /**
   * Constructs the service.
   *
   * @param executor       executor for building deferred indexes.
   * @param config         configuration for deferred index execution.
   * @param readinessCheck readiness check for replay-based discovery.
   */
  @Inject
  DeferredIndexServiceImpl(DeferredIndexExecutor executor,
                            DeferredIndexExecutionConfig config,
                            DeferredIndexReadinessCheck readinessCheck) {
    this.executor = executor;
    this.config = config;
    this.readinessCheck = readinessCheck;
  }


  /**
   * @see DeferredIndexService#setUpgradeSteps(Collection)
   */
  @Override
  public void setUpgradeSteps(Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    this.upgradeSteps = upgradeSteps;
  }


  /**
   * @see DeferredIndexService#execute()
   */
  @Override
  public void execute() {
    if (upgradeSteps == null) {
      throw new IllegalStateException("Upgrade steps not set. Call setUpgradeSteps() before execute().");
    }

    validateConfig(config);

    List<DeferredAddIndex> missing = readinessCheck.findMissingDeferredIndexes(upgradeSteps);
    totalSubmitted = missing.size();

    log.info("Deferred index service: executing " + missing.size() + " pending operation(s)...");
    executionFuture = executor.execute(missing);
  }


  /**
   * @see DeferredIndexService#awaitCompletion(long)
   */
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


  /**
   * @see DeferredIndexService#getProgress()
   */
  @Override
  public DeferredIndexProgress getProgress() {
    if (executor instanceof DeferredIndexExecutorImpl) {
      DeferredIndexExecutorImpl impl = (DeferredIndexExecutorImpl) executor;
      int completed = impl.getCompletedCount();
      int failed = impl.getFailedCount();
      int total = totalSubmitted;
      int remaining = total - completed - failed;
      return new DeferredIndexProgress(total, completed, failed, remaining);
    }
    return new DeferredIndexProgress(0, 0, 0, 0);
  }


  /**
   * Validates that all configuration values are within acceptable ranges.
   *
   * @param cfg the configuration to validate.
   * @throws IllegalArgumentException if any value is out of range.
   */
  private void validateConfig(DeferredIndexExecutionConfig cfg) {
    if (cfg.getThreadPoolSize() < 1) {
      throw new IllegalArgumentException("threadPoolSize must be >= 1, was " + cfg.getThreadPoolSize());
    }
    if (cfg.getMaxRetries() < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0, was " + cfg.getMaxRetries());
    }
    if (cfg.getRetryBaseDelayMs() < 0) {
      throw new IllegalArgumentException("retryBaseDelayMs must be >= 0 ms, was " + cfg.getRetryBaseDelayMs() + " ms");
    }
    if (cfg.getRetryMaxDelayMs() < cfg.getRetryBaseDelayMs()) {
      throw new IllegalArgumentException("retryMaxDelayMs (" + cfg.getRetryMaxDelayMs()
          + " ms) must be >= retryBaseDelayMs (" + cfg.getRetryBaseDelayMs() + " ms)");
    }
    if (cfg.getExecutionTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "executionTimeoutSeconds must be > 0 s, was " + cfg.getExecutionTimeoutSeconds() + " s");
    }
  }
}
