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

package org.alfasoftware.morf.upgrade.deployed;

import java.util.List;
import java.util.Map;

import com.google.inject.ImplementedBy;

/**
 * Data access interface for the DeployedIndexes table. Provides read and
 * write operations for tracking all deployed indexes (deferred and non-deferred).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeployedIndexesDAOImpl.class)
interface DeployedIndexesDAO {

  /**
   * Returns all entries in the DeployedIndexes table.
   *
   * @return all deployed index entries.
   */
  List<DeployedIndexEntry> findAll();


  /**
   * Returns all entries for a given table name.
   *
   * @param tableName the table name.
   * @return entries for that table.
   */
  List<DeployedIndexEntry> findByTable(String tableName);


  /**
   * Returns all entries with status {@link DeployedIndexStatus#PENDING},
   * {@link DeployedIndexStatus#IN_PROGRESS}, or {@link DeployedIndexStatus#FAILED}.
   *
   * @return non-terminal deferred index entries.
   */
  List<DeployedIndexEntry> findNonTerminalOperations();


  /**
   * Returns counts of entries grouped by status.
   *
   * @return map from status to count.
   */
  Map<DeployedIndexStatus, Integer> countAllByStatus();


  /**
   * Marks a deferred index as started (IN_PROGRESS).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @param startedTime epoch milliseconds.
   */
  void markStarted(String tableName, String indexName, long startedTime);


  /**
   * Marks a deferred index as completed.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @param completedTime epoch milliseconds.
   */
  void markCompleted(String tableName, String indexName, long completedTime);


  /**
   * Marks a deferred index as failed with an error message and incremented retry count.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @param errorMessage the error description.
   */
  void markFailed(String tableName, String indexName, String errorMessage);


  /**
   * Resets all IN_PROGRESS entries to PENDING. Used on startup for crash recovery.
   */
  void resetAllInProgressToPending();
}
