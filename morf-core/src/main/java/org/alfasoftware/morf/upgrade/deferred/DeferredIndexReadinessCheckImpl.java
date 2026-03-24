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
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexReadinessCheck}.
 *
 * <p>{@link #augmentSchemaWithPendingIndexes(Schema)} is always called to
 * overlay virtual indexes for non-terminal operations into the source schema.
 * {@link #forceBuildAllPending()} is called only when an upgrade with new
 * steps is about to run, to ensure stale indexes from a previous upgrade
 * are built before new changes are applied.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexReadinessCheckImpl implements DeferredIndexReadinessCheck {

  private static final Log log = LogFactory.getLog(DeferredIndexReadinessCheckImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexExecutor executor;
  private final UpgradeConfigAndContext config;
  private final ConnectionResources connectionResources;


  /**
   * Constructs a readiness check with injected dependencies.
   *
   * @param dao                 DAO for deferred index operations.
   * @param executor            executor used to force-build pending operations.
   * @param config              upgrade configuration.
   * @param connectionResources database connection resources.
   */
  @Inject
  DeferredIndexReadinessCheckImpl(DeferredIndexOperationDAO dao, DeferredIndexExecutor executor,
                                  UpgradeConfigAndContext config,
                                  ConnectionResources connectionResources) {
    this.dao = dao;
    this.executor = executor;
    this.config = config;
    this.connectionResources = connectionResources;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexReadinessCheck#forceBuildAllPending()
   */
  @Override
  public void forceBuildAllPending() {
    if (!config.isDeferredIndexCreationEnabled()) {
      log.debug("Deferred index creation is disabled — skipping force-build");
      return;
    }

    if (!deferredIndexTableExists()) {
      log.debug("DeferredIndexOperation table does not exist — skipping readiness check");
      return;
    }

    // Reset any crashed IN_PROGRESS operations so they are picked up
    dao.resetAllInProgressToPending();

    List<DeferredIndexOperation> pending = dao.findPendingOperations();
    if (!pending.isEmpty()) {
      log.warn("Found " + pending.size() + " pending deferred index operation(s) before upgrade. "
          + "Executing immediately before proceeding...");

      awaitCompletion(executor.execute());

      log.info("Pre-upgrade deferred index execution complete.");
    }

    // Check for FAILED operations — whether they existed before this run
    // or were created by the force-build above. An upgrade cannot proceed
    // with permanently failed index operations from a previous upgrade.
    int failedCount = dao.countAllByStatus().getOrDefault(DeferredIndexStatus.FAILED, 0);
    if (failedCount > 0) {
      throw new IllegalStateException("Deferred index force-build failed: "
          + failedCount + " index operation(s) could not be built. "
          + "Resolve the underlying issue before retrying.");
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexReadinessCheck#augmentSchemaWithPendingIndexes(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema augmentSchemaWithPendingIndexes(Schema sourceSchema) {
    if (!config.isDeferredIndexCreationEnabled()) {
      return sourceSchema;
    }
    if (!deferredIndexTableExists()) {
      return sourceSchema;
    }

    List<DeferredIndexOperation> ops = dao.findNonTerminalOperations();
    if (ops.isEmpty()) {
      return sourceSchema;
    }

    log.info("Augmenting schema with " + ops.size() + " deferred index operation(s) not yet built");

    Schema result = sourceSchema;
    for (DeferredIndexOperation op : ops) {
      if (!result.tableExists(op.getTableName())) {
        log.warn("Skipping deferred index [" + op.getIndexName() + "] — table ["
            + op.getTableName() + "] does not exist in schema");
        continue;
      }

      Table table = result.getTable(op.getTableName());
      boolean indexAlreadyExists = table.indexes().stream()
          .anyMatch(idx -> idx.getName().equalsIgnoreCase(op.getIndexName()));
      if (indexAlreadyExists) {
        // The index exists in the database but the operation row is still
        // non-terminal (e.g. the status update failed after CREATE INDEX
        // succeeded). The stale row will be cleaned up when the executor
        // runs: its post-failure indexExistsInDatabase check will mark it
        // COMPLETED. No schema augmentation is needed here.
        log.info("Deferred index [" + op.getIndexName() + "] already exists on table ["
            + op.getTableName() + "] — skipping augmentation; stale row will be resolved by executor");
        continue;
      }

      Index newIndex = op.toIndex();
      List<String> indexNames = new ArrayList<>();
      for (Index existing : table.indexes()) {
        indexNames.add(existing.getName());
      }
      indexNames.add(newIndex.getName());

      log.info("Augmenting schema with deferred index [" + op.getIndexName() + "] on table ["
          + op.getTableName() + "] [" + op.getStatus() + "]");

      result = new TableOverrideSchema(result,
          new AlteredTable(table, null, null, indexNames, Arrays.asList(newIndex)));
    }

    return result;
  }


  /**
   * Blocks until the given future completes, with a timeout from config.
   *
   * @param future the future to await.
   * @throws IllegalStateException on timeout, interruption, or execution failure.
   */
  private void awaitCompletion(CompletableFuture<Void> future) {
    long timeoutSeconds = config.getDeferredIndexForceBuildTimeoutSeconds();
    if (timeoutSeconds <= 0) {
      throw new IllegalArgumentException(
          "deferredIndexForceBuildTimeoutSeconds must be > 0 s, was " + timeoutSeconds + " s");
    }
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
   * Checks whether the DeferredIndexOperation table exists in the database
   * by opening a fresh schema resource.
   *
   * @return {@code true} if the table exists.
   */
  private boolean deferredIndexTableExists() {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      return sr.tableExists(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME);
    }
  }


}
