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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.SchemaChangeSequence;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexReadinessCheck}.
 *
 * <p>Uses replay-based discovery: instantiates all upgrade steps, walks their
 * changes with a {@link DeferredIndexTracker} to resolve cascades, then compares
 * surviving deferred indexes against the live database schema to find which
 * ones are missing.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexReadinessCheckImpl implements DeferredIndexReadinessCheck {

  private static final Log log = LogFactory.getLog(DeferredIndexReadinessCheckImpl.class);

  private final DeferredIndexExecutor executor;
  private final DeferredIndexExecutionConfig config;
  private final ConnectionResources connectionResources;


  /**
   * Constructs a readiness check with injected dependencies.
   *
   * @param executor            executor used to force-build pending operations.
   * @param config              configuration used when executing pending operations.
   * @param connectionResources database connection resources.
   */
  @Inject
  DeferredIndexReadinessCheckImpl(DeferredIndexExecutor executor,
                                  DeferredIndexExecutionConfig config,
                                  ConnectionResources connectionResources) {
    this.executor = executor;
    this.config = config;
    this.connectionResources = connectionResources;
  }


  /**
   * @see DeferredIndexReadinessCheck#forceBuildAllPending(Collection)
   */
  @Override
  public void forceBuildAllPending(Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    validateConfig(config);

    List<DeferredAddIndex> missing = findMissingDeferredIndexes(upgradeSteps);
    if (missing.isEmpty()) {
      return;
    }

    log.warn("Found " + missing.size() + " pending deferred index(es) before upgrade. "
        + "Executing immediately before proceeding...");

    CompletableFuture<Void> future = executor.execute(missing);
    awaitCompletion(future);

    log.info("Pre-upgrade deferred index execution complete.");

    // Check for failures — the executor tracks failed count in-memory
    if (executor instanceof DeferredIndexExecutorImpl) {
      int failed = ((DeferredIndexExecutorImpl) executor).getFailedCount();
      if (failed > 0) {
        throw new IllegalStateException("Deferred index force-build failed: "
            + failed + " index operation(s) could not be built. "
            + "Resolve the underlying issue before retrying.");
      }
    }
  }


  /**
   * @see DeferredIndexReadinessCheck#augmentSchemaWithPendingIndexes(Schema, Collection)
   */
  @Override
  public Schema augmentSchemaWithPendingIndexes(Schema sourceSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    List<DeferredAddIndex> missing = findMissingDeferredIndexes(upgradeSteps);
    if (missing.isEmpty()) {
      return sourceSchema;
    }

    log.info("Augmenting schema with " + missing.size() + " deferred index(es) not yet built");

    Schema result = sourceSchema;
    for (DeferredAddIndex dai : missing) {
      if (!result.tableExists(dai.getTableName())) {
        log.warn("Skipping deferred index [" + dai.getNewIndex().getName() + "] — table ["
            + dai.getTableName() + "] does not exist in schema");
        continue;
      }

      Table table = result.getTable(dai.getTableName());
      boolean indexAlreadyInSchema = table.indexes().stream()
          .anyMatch(idx -> idx.getName().equalsIgnoreCase(dai.getNewIndex().getName()));
      if (indexAlreadyInSchema) {
        continue;
      }

      List<String> indexNames = new ArrayList<>();
      for (Index existing : table.indexes()) {
        indexNames.add(existing.getName());
      }
      indexNames.add(dai.getNewIndex().getName());

      log.info("Augmenting schema with deferred index [" + dai.getNewIndex().getName()
          + "] on table [" + dai.getTableName() + "]");

      result = new TableOverrideSchema(result,
          new AlteredTable(table, null, null, indexNames, Arrays.asList(dai.getNewIndex())));
    }

    return result;
  }


  /**
   * @see DeferredIndexReadinessCheck#findMissingDeferredIndexes(Collection)
   */
  @Override
  public List<DeferredAddIndex> findMissingDeferredIndexes(Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    if (upgradeSteps == null || upgradeSteps.isEmpty()) {
      return List.of();
    }

    // Replay all steps to find surviving deferred indexes
    List<UpgradeStep> stepInstances = instantiateSteps(upgradeSteps);
    SchemaChangeSequence sequence = new SchemaChangeSequence(stepInstances);
    List<DeferredAddIndex> surviving = sequence.findSurvivingDeferredIndexes();

    if (surviving.isEmpty()) {
      return List.of();
    }

    // Compare against live database schema
    List<DeferredAddIndex> missing = new ArrayList<>();
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      for (DeferredAddIndex dai : surviving) {
        if (!sr.tableExists(dai.getTableName())) {
          // Table doesn't exist in DB — index is missing
          missing.add(dai);
          continue;
        }
        boolean indexExists = sr.getTable(dai.getTableName()).indexes().stream()
            .anyMatch(idx -> idx.getName().equalsIgnoreCase(dai.getNewIndex().getName()));
        if (!indexExists) {
          missing.add(dai);
        }
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("Replay found " + surviving.size() + " surviving deferred index(es), "
          + missing.size() + " missing from database");
    }

    return missing;
  }


  /**
   * Instantiates all upgrade step classes. Classes that cannot be
   * reflectively instantiated (non-static inner classes, non-public
   * constructors, etc.) are skipped with a debug log.
   */
  private List<UpgradeStep> instantiateSteps(Collection<Class<? extends UpgradeStep>> stepClasses) {
    List<UpgradeStep> steps = new ArrayList<>();
    for (Class<? extends UpgradeStep> stepClass : stepClasses) {
      try {
        java.lang.reflect.Constructor<? extends UpgradeStep> ctor = stepClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        steps.add(ctor.newInstance());
      } catch (NoSuchMethodException e) {
        // Non-static inner class or class without a no-arg constructor — skip
        if (log.isDebugEnabled()) {
          log.debug("Skipping upgrade step without no-arg constructor: " + stepClass.getName());
        }
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Failed to instantiate upgrade step: " + stepClass.getName(), e);
      }
    }
    return steps;
  }


  /**
   * Blocks until the given future completes, with a timeout from config.
   *
   * @param future the future to await.
   * @throws IllegalStateException on timeout, interruption, or execution failure.
   */
  private void awaitCompletion(CompletableFuture<Void> future) {
    long timeoutSeconds = config.getExecutionTimeoutSeconds();
    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new IllegalStateException("Deferred index force-build timed out after "
          + timeoutSeconds + " seconds.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Deferred index force-build interrupted.");
    } catch (ExecutionException e) {
      throw new IllegalStateException("Deferred index force-build failed unexpectedly.", e.getCause());
    }
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
