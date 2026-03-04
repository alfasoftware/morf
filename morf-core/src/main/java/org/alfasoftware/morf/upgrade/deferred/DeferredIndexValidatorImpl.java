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

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexValidator}.
 *
 * <p>If pending operations are found, {@link #validateNoPendingOperations()}
 * force-executes them synchronously via a {@link DeferredIndexExecutor} before
 * returning. This guarantees that subsequent upgrade steps never encounter a
 * missing index that a previous deferred operation was supposed to build.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexValidatorImpl implements DeferredIndexValidator {

  private static final Log log = LogFactory.getLog(DeferredIndexValidatorImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final DeferredIndexExecutor executor;
  private final DeferredIndexConfig config;


  /**
   * Constructs a validator with injected dependencies.
   *
   * @param dao      DAO for deferred index operations.
   * @param executor executor used to force-build pending operations.
   * @param config   configuration used when executing pending operations.
   */
  @Inject
  DeferredIndexValidatorImpl(DeferredIndexOperationDAO dao, DeferredIndexExecutor executor,
                             DeferredIndexConfig config) {
    this.dao = dao;
    this.executor = executor;
    this.config = config;
  }


  @Override
  public void validateNoPendingOperations() {
    List<DeferredIndexOperation> pending = dao.findPendingOperations();
    if (pending.isEmpty()) {
      return;
    }

    log.warn("Found " + pending.size() + " pending deferred index operation(s) before upgrade. "
        + "Executing immediately before proceeding...");

    long timeoutMs = config.getExecutionTimeoutSeconds() * 1_000L;
    DeferredIndexExecutionResult result = executor.executeAndWait(timeoutMs);

    log.info("Pre-upgrade deferred index execution complete: completed=" + result.getCompletedCount()
        + ", failed=" + result.getFailedCount());

    if (result.getFailedCount() > 0) {
      throw new IllegalStateException("Pre-upgrade deferred index validation failed: "
          + result.getFailedCount() + " index operation(s) could not be built. "
          + "Resolve the underlying issue before retrying the upgrade.");
    }
  }
}
