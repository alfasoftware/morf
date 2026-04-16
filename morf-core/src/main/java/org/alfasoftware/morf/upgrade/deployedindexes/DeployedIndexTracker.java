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

package org.alfasoftware.morf.upgrade.deployedindexes;

import java.util.List;
import java.util.Map;

import com.google.inject.ImplementedBy;

/**
 * Public API for applications to report deferred index execution status
 * back to the DeployedIndexes table. Applications use this alongside
 * {@link org.alfasoftware.morf.upgrade.UpgradePath#getDeferredIndexStatements()}
 * to manage deferred index builds.
 *
 * <p>Typical usage:</p>
 * <pre>
 * List&lt;String&gt; sql = upgradePath.getDeferredIndexStatements();
 * for (String stmt : sql) {
 *     tracker.markStarted(tableName, indexName);
 *     try {
 *         executeSQL(stmt);
 *         tracker.markCompleted(tableName, indexName);
 *     } catch (Exception e) {
 *         tracker.markFailed(tableName, indexName, e.getMessage());
 *     }
 * }
 * </pre>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeployedIndexTrackerImpl.class)
public interface DeployedIndexTracker {

  /**
   * Marks a deferred index as started (IN_PROGRESS).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   */
  void markStarted(String tableName, String indexName);


  /**
   * Marks a deferred index as completed (COMPLETED).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   */
  void markCompleted(String tableName, String indexName);


  /**
   * Marks a deferred index as failed (FAILED) with an error message.
   * The retry count is incremented.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @param errorMessage the error description.
   */
  void markFailed(String tableName, String indexName, String errorMessage);


  /**
   * Returns the current count of deployed index entries grouped by status.
   *
   * @return a map from each {@link DeployedIndexStatus} to its count.
   */
  Map<DeployedIndexStatus, Integer> getProgress();


  /**
   * Returns all deferred index entries that are not yet completed
   * (PENDING, IN_PROGRESS, or FAILED).
   *
   * @return list of non-terminal deferred index entries.
   */
  List<DeployedIndexEntry> getPendingIndexes();
}
