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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexReadinessCheck}.
 *
 * <p>If the {@code DeferredIndexOperation} table exists and contains pending
 * operations, they are force-built synchronously via a
 * {@link DeferredIndexExecutor} before returning. This guarantees that
 * subsequent upgrade steps never encounter a missing index that a previous
 * deferred operation was supposed to build.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexReadinessCheckImpl implements DeferredIndexReadinessCheck {

  private static final Log log = LogFactory.getLog(DeferredIndexReadinessCheckImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexExecutor executor;
  private final DeferredIndexExecutionConfig config;


  /**
   * Constructs a readiness check with injected dependencies.
   *
   * @param dao      DAO for deferred index operations.
   * @param executor executor used to force-build pending operations.
   * @param config   configuration used when executing pending operations.
   */
  @Inject
  DeferredIndexReadinessCheckImpl(DeferredIndexOperationDAO dao, DeferredIndexExecutor executor,
                                  DeferredIndexExecutionConfig config) {
    this.dao = dao;
    this.executor = executor;
    this.config = config;
  }


  @Override
  public void run(Schema sourceSchema) {
    if (!sourceSchema.tableExists(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME)) {
      log.debug("DeferredIndexOperation table does not exist — skipping readiness check");
      return;
    }

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
}
