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
 * Pre-upgrade safety gate that ensures no deferred index operations remain
 * incomplete before a new upgrade run begins.
 *
 * <p>This check is invoked automatically by the upgrade framework
 * ({@link org.alfasoftware.morf.upgrade.Upgrade#findPath findPath}) before
 * schema diffing begins, for both the sequential and graph-based upgrade
 * paths. If any {@link DeferredIndexStatus#PENDING} or stale
 * {@link DeferredIndexStatus#IN_PROGRESS} operations are found from a
 * previous upgrade, they are force-built synchronously (blocking the
 * upgrade) before proceeding.</p>
 *
 * <p><strong>Important:</strong> this check does <em>not</em> automatically
 * build deferred indexes queued by the current upgrade. After an upgrade
 * completes, adopters must explicitly invoke
 * {@link DeferredIndexService#execute()} to start background index builds.
 * If the adopter forgets, the next upgrade will catch it here.</p>
 *
 * @see DeferredIndexService
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexReadinessCheckImpl.class)
public interface DeferredIndexReadinessCheck {

  /**
   * Ensures all deferred index operations from a previous upgrade are
   * complete before proceeding with a new upgrade.
   *
   * <p>If the deferred index infrastructure table does not exist in the
   * given source schema (e.g. on the first upgrade that introduces the
   * feature), this is a safe no-op. If pending operations are found, they
   * are force-built synchronously (blocking the caller) before returning.</p>
   *
   * @param sourceSchema the current database schema before upgrade.
   * @throws IllegalStateException if any operations failed permanently.
   */
  void run(Schema sourceSchema);


  /**
   * Creates a readiness check instance from connection resources, for use
   * in the static upgrade path where Guice is not available.
   *
   * @param connectionResources connection details for constructing services.
   * @return a new readiness check instance.
   */
  static DeferredIndexReadinessCheck create(ConnectionResources connectionResources) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    SqlScriptExecutorProvider executorProvider = new SqlScriptExecutorProvider(connectionResources);
    DeferredIndexOperationDAO dao = new DeferredIndexOperationDAOImpl(executorProvider, connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, connectionResources,
        executorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    return new DeferredIndexReadinessCheckImpl(dao, executor, config);
  }
}
