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

import org.alfasoftware.morf.jdbc.ConnectionResources;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Pre-upgrade check that ensures no deferred index operations are left
 * {@link DeferredIndexStatus#PENDING} before a new upgrade run begins.
 *
 * <p>If pending operations are found, {@link #validateNoPendingOperations()}
 * force-executes them synchronously via a {@link DeferredIndexExecutor} before
 * returning. This guarantees that subsequent upgrade steps never encounter a
 * missing index that a previous deferred operation was supposed to build.</p>
 *
 * <p>Typical integration point:</p>
 * <pre>
 * DeferredIndexValidator validator = new DeferredIndexValidator(connectionResources, config);
 * validator.validateNoPendingOperations();   // blocks if needed
 * Upgrade.performUpgrade(targetSchema, upgradeSteps, connectionResources, upgradeConfig);
 * </pre>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexValidator {

  private static final Log log = LogFactory.getLog(DeferredIndexValidator.class);

  private final DeferredIndexOperationDAO dao;
  private final ConnectionResources connectionResources;
  private final DeferredIndexConfig config;


  /**
   * Constructs a validator for the supplied database connection.
   *
   * @param connectionResources database connection resources.
   * @param config              configuration used when executing pending operations.
   */
  public DeferredIndexValidator(ConnectionResources connectionResources, DeferredIndexConfig config) {
    if (config.getOperationTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "operationTimeoutSeconds must be > 0 s, was " + config.getOperationTimeoutSeconds() + " s");
    }
    this.connectionResources = connectionResources;
    this.config = config;
    this.dao = new DeferredIndexOperationDAOImpl(connectionResources);
  }


  /**
   * Verifies that no {@link DeferredIndexStatus#PENDING} operations exist. If
   * any are found, executes them immediately (blocking the caller) before
   * returning.
   *
   * <p>The timeout applied to the forced execution is
   * {@link DeferredIndexConfig#getOperationTimeoutSeconds()} converted to
   * milliseconds.</p>
   */
  public void validateNoPendingOperations() {
    List<DeferredIndexOperation> pending = dao.findPendingOperations();
    if (pending.isEmpty()) {
      return;
    }

    log.warn("Found " + pending.size() + " pending deferred index operation(s) before upgrade. "
        + "Executing immediately before proceeding...");

    DeferredIndexExecutor executor = new DeferredIndexExecutor(connectionResources, config);
    long timeoutMs = config.getOperationTimeoutSeconds() * 1_000L;
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(timeoutMs);

    log.info("Pre-upgrade deferred index execution complete: completed=" + result.getCompletedCount()
        + ", failed=" + result.getFailedCount());

    if (result.getFailedCount() > 0) {
      throw new IllegalStateException("Pre-upgrade deferred index validation failed: "
          + result.getFailedCount() + " index operation(s) could not be built. "
          + "Resolve the underlying issue before retrying the upgrade.");
    }
  }
}
