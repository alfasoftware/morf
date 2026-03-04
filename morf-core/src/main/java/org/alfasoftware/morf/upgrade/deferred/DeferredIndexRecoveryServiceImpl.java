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
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexRecoveryService}.
 *
 * <p>For each stale operation the actual database schema is inspected:</p>
 * <ul>
 *   <li>Index already exists &rarr; mark {@link DeferredIndexStatus#COMPLETED}.</li>
 *   <li>Index absent &rarr; reset to {@link DeferredIndexStatus#PENDING} so the
 *       executor will rebuild it.</li>
 * </ul>
 *
 * <p><strong>Note:</strong> Detection of <em>invalid</em> indexes (e.g.
 * PostgreSQL {@code indisvalid=false} after a failed {@code CREATE INDEX
 * CONCURRENTLY}) is not yet implemented. Platform-specific invalid-index
 * handling will be added in Stage 11 (cross-platform dialect support).</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexRecoveryServiceImpl implements DeferredIndexRecoveryService {

  private static final Log log = LogFactory.getLog(DeferredIndexRecoveryServiceImpl.class);

  private final DeferredIndexOperationDAO dao;
  private final ConnectionResources connectionResources;
  private final DeferredIndexConfig config;


  /**
   * Constructs a recovery service for the supplied database connection.
   *
   * @param dao                 DAO for deferred index operations.
   * @param connectionResources database connection resources.
   * @param config              configuration governing the stale-threshold.
   */
  @Inject
  DeferredIndexRecoveryServiceImpl(DeferredIndexOperationDAO dao, ConnectionResources connectionResources,
                                   DeferredIndexConfig config) {
    this.dao = dao;
    this.connectionResources = connectionResources;
    this.config = config;
  }


  @Override
  public void recoverStaleOperations() {
    long threshold = timestampBefore(config.getStaleThresholdSeconds());
    List<DeferredIndexOperation> staleOps = dao.findStaleInProgressOperations(threshold);

    if (staleOps.isEmpty()) {
      return;
    }

    log.info("Recovering " + staleOps.size() + " stale IN_PROGRESS deferred index operation(s)");

    try (SchemaResource schema = connectionResources.openSchemaResource()) {
      for (DeferredIndexOperation op : staleOps) {
        recoverOperation(op, schema);
      }
    }
  }


  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  private void recoverOperation(DeferredIndexOperation op, Schema schema) {
    if (!schema.tableExists(op.getTableName())) {
      log.warn("Stale operation [" + op.getId() + "] — table no longer exists, marking SKIPPED: "
          + op.getTableName() + "." + op.getIndexName());
      dao.updateStatus(op.getId(), DeferredIndexStatus.SKIPPED);
    } else if (indexExistsInSchema(op, schema)) {
      log.info("Stale operation [" + op.getId() + "] — index exists in database, marking COMPLETED: "
          + op.getTableName() + "." + op.getIndexName());
      dao.markCompleted(op.getId(), System.currentTimeMillis());
    } else {
      log.info("Stale operation [" + op.getId() + "] — index absent from database, resetting to PENDING: "
          + op.getTableName() + "." + op.getIndexName());
      dao.resetToPending(op.getId());
    }
  }


  private static boolean indexExistsInSchema(DeferredIndexOperation op, Schema schema) {
    // Caller has already verified that the table exists
    Table table = schema.getTable(op.getTableName());
    return table.indexes().stream()
        .anyMatch(idx -> idx.getName().equalsIgnoreCase(op.getIndexName()));
  }


  private long timestampBefore(long seconds) {
    return System.currentTimeMillis() - java.util.concurrent.TimeUnit.SECONDS.toMillis(seconds);
  }
}
