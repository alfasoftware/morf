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

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;

import com.google.inject.ImplementedBy;

/**
 * Startup hook that reconciles deferred index operations from a previous
 * run before the upgrade framework begins schema diffing.
 *
 * <p>This check is invoked during application startup by the upgrade
 * framework ({@link org.alfasoftware.morf.upgrade.Upgrade#findPath findPath})
 * for both the sequential and graph-based upgrade paths:</p>
 *
 * <ul>
 *   <li>{@link #augmentSchemaWithPendingIndexes(Schema)} is always called
 *       after the source schema is read, to overlay virtual indexes for
 *       non-terminal operations so the schema comparison treats them as
 *       present.</li>
 *   <li>{@link #forceBuildAllPending()} is called only when an upgrade
 *       with new steps is about to run. It force-builds any pending or
 *       stale operations from a previous upgrade synchronously, ensuring
 *       the schema is clean before new changes are applied.</li>
 * </ul>
 *
 * <p>On a normal restart with no upgrade, pending deferred indexes are
 * left for {@link DeferredIndexService#execute()} to build. After every
 * upgrade, adopters must call {@link DeferredIndexService#execute()} to
 * start building deferred indexes queued by the current upgrade.</p>
 *
 * @see DeferredIndexService
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexReadinessCheckImpl.class)
public interface DeferredIndexReadinessCheck {

  /**
   * Force-builds all pending deferred index operations from a previous
   * upgrade, blocking until complete.
   *
   * <p>Called by the upgrade framework only when an upgrade with new
   * steps is about to run. If the deferred index infrastructure table
   * does not exist (e.g. on the first upgrade), this is a safe no-op.
   * If pending operations are found, they are force-built synchronously
   * before returning. Any stale IN_PROGRESS operations from a crashed
   * process are also reset to PENDING and built.</p>
   *
   * @throws IllegalStateException if any operations failed permanently.
   */
  void forceBuildAllPending();


  /**
   * Augments the given source schema with virtual indexes from non-terminal
   * deferred index operations.
   *
   * <p>Always called after the source schema is read. For each PENDING,
   * IN_PROGRESS, or FAILED operation, the corresponding index is added to
   * the schema so that the schema comparison treats it as present. The
   * actual index will be built by {@link DeferredIndexService#execute()}.</p>
   *
   * @param sourceSchema the current database schema before upgrade.
   * @return the augmented schema with deferred indexes included.
   */
  Schema augmentSchemaWithPendingIndexes(Schema sourceSchema);


  /**
   * Creates a readiness check instance from connection resources, for use
   * in the static upgrade path where Guice is not available.
   *
   * @param connectionResources connection details for constructing services.
   * @return a new readiness check instance.
   */
  static DeferredIndexReadinessCheck create(ConnectionResources connectionResources) {
    return create(connectionResources, new org.alfasoftware.morf.upgrade.UpgradeConfigAndContext());
  }


  /**
   * Creates a readiness check instance from connection resources and config,
   * for use in the static upgrade path where Guice is not available.
   *
   * @param connectionResources connection details for constructing services.
   * @param config              upgrade configuration.
   * @return a new readiness check instance.
   */
  static DeferredIndexReadinessCheck create(ConnectionResources connectionResources,
                                             org.alfasoftware.morf.upgrade.UpgradeConfigAndContext config) {
    SqlScriptExecutorProvider executorProvider = new SqlScriptExecutorProvider(connectionResources);
    DeferredIndexOperationDAO dao = new DeferredIndexOperationDAOImpl(executorProvider, connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, connectionResources,
        executorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    return new DeferredIndexReadinessCheckImpl(dao, executor, config, connectionResources);
  }
}
