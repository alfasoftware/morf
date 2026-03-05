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
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.metadata.Table;
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
 * <p>Supports two modes:</p>
 * <ul>
 *   <li><strong>Mode 1 (force-build):</strong> {@link #run()} checks for pending
 *       or crashed operations and force-builds them synchronously before the
 *       upgrade reads the source schema.</li>
 *   <li><strong>Mode 2 (background):</strong> {@link #augmentSchemaWithDeferredIndexes(Schema)}
 *       adds virtual indexes from non-terminal operations into the source schema
 *       so that the schema comparison treats them as present.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexReadinessCheckImpl implements DeferredIndexReadinessCheck {

  private static final Log log = LogFactory.getLog(DeferredIndexReadinessCheckImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexExecutor executor;
  private final DeferredIndexExecutionConfig config;
  private final ConnectionResources connectionResources;


  /**
   * Constructs a readiness check with injected dependencies.
   *
   * @param dao                 DAO for deferred index operations.
   * @param executor            executor used to force-build pending operations.
   * @param config              configuration used when executing pending operations.
   * @param connectionResources database connection resources.
   */
  @Inject
  DeferredIndexReadinessCheckImpl(DeferredIndexOperationDAO dao, DeferredIndexExecutor executor,
                                  DeferredIndexExecutionConfig config,
                                  ConnectionResources connectionResources) {
    this.dao = dao;
    this.executor = executor;
    this.config = config;
    this.connectionResources = connectionResources;
  }


  @Override
  public void run() {
    if (!deferredIndexTableExists()) {
      log.debug("DeferredIndexOperation table does not exist — skipping readiness check");
      return;
    }

    // Reset any crashed IN_PROGRESS operations so they are picked up
    dao.resetAllInProgressToPending();

    List<DeferredIndexOperation> pending = dao.findPendingOperations();
    if (pending.isEmpty()) {
      return;
    }

    log.warn("Found " + pending.size() + " pending deferred index operation(s) before upgrade. "
        + "Executing immediately before proceeding...");

    CompletableFuture<Void> future = executor.execute();

    long timeoutSeconds = config.getExecutionTimeoutSeconds();
    try {
      future.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new IllegalStateException("Pre-upgrade deferred index readiness check timed out after "
          + timeoutSeconds + " seconds.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Pre-upgrade deferred index readiness check interrupted.");
    } catch (ExecutionException e) {
      throw new IllegalStateException("Pre-upgrade deferred index readiness check failed unexpectedly.", e.getCause());
    }

    int failedCount = dao.countAllByStatus().get(DeferredIndexStatus.FAILED);
    if (failedCount > 0) {
      throw new IllegalStateException("Pre-upgrade deferred index readiness check failed: "
          + failedCount + " index operation(s) could not be built. "
          + "Resolve the underlying issue before retrying the upgrade.");
    }

    log.info("Pre-upgrade deferred index execution complete.");
  }


  @Override
  public Schema augmentSchemaWithDeferredIndexes(Schema sourceSchema) {
    if (!deferredIndexTableExists()) {
      return sourceSchema;
    }

    List<DeferredIndexOperation> ops = dao.findNonTerminalOperations();
    if (ops.isEmpty()) {
      return sourceSchema;
    }

    log.info("Augmenting schema with " + ops.size() + " deferred index operation(s) for Mode 2 (background build)");

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
        continue;
      }

      Index newIndex = reconstructIndex(op);
      List<String> indexNames = new ArrayList<>();
      for (Index existing : table.indexes()) {
        indexNames.add(existing.getName());
      }
      indexNames.add(newIndex.getName());

      result = new TableOverrideSchema(result,
          new AlteredTable(table, null, null, indexNames, Arrays.asList(newIndex)));
    }

    return result;
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


  /**
   * Rebuilds an {@link Index} metadata object from the persisted operation state.
   *
   * @param op the operation containing index name, uniqueness, and column names.
   * @return the reconstructed index.
   */
  private static Index reconstructIndex(DeferredIndexOperation op) {
    IndexBuilder builder = index(op.getIndexName());
    if (op.isIndexUnique()) {
      builder = builder.unique();
    }
    return builder.columns(op.getColumnNames().toArray(new String[0]));
  }
}
