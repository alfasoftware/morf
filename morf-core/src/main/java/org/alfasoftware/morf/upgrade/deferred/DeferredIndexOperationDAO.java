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

import com.google.inject.ImplementedBy;

/**
 * DAO for reading and writing {@link DeferredIndexOperation} records,
 * including their associated column-name rows from
 * {@code DeferredIndexOperationColumn}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexOperationDAOImpl.class)
interface DeferredIndexOperationDAO {

  /**
   * Inserts a new operation row together with its column rows.
   *
   * @param op the operation to insert.
   */
  void insertOperation(DeferredIndexOperation op);


  /**
   * Returns all {@link DeferredIndexStatus#PENDING} operations with
   * their ordered column names populated.
   *
   * @return list of pending operations.
   */
  List<DeferredIndexOperation> findPendingOperations();


  /**
   * Returns all {@link DeferredIndexStatus#IN_PROGRESS} operations
   * whose {@code startedTime} is strictly less than the supplied threshold,
   * indicating a stale or abandoned build.
   *
   * @param startedBefore upper bound on {@code startedTime} (yyyyMMddHHmmss).
   * @return list of stale in-progress operations.
   */
  List<DeferredIndexOperation> findStaleInProgressOperations(long startedBefore);


  /**
   * Returns {@code true} if a record for the given upgrade UUID and index name
   * already exists in the queue (regardless of status).
   *
   * @param upgradeUUID the UUID of the upgrade step.
   * @param indexName   the name of the index.
   * @return {@code true} if a matching record exists.
   */
  boolean existsByUpgradeUUIDAndIndexName(String upgradeUUID, String indexName);


  /**
   * Returns {@code true} if any record for the given table name and index name
   * exists in the queue (regardless of status). Used by
   * {@link DeferredAddIndex#isApplied} to detect whether the upgrade step has
   * already been processed.
   *
   * @param tableName the name of the table.
   * @param indexName the name of the index.
   * @return {@code true} if a matching record exists.
   */
  boolean existsByTableNameAndIndexName(String tableName, String indexName);


  /**
   * Transitions the operation to {@link DeferredIndexStatus#IN_PROGRESS}
   * and records its start time.
   *
   * @param id          the operation to update.
   * @param startedTime start timestamp (yyyyMMddHHmmss).
   */
  void markStarted(long id, long startedTime);


  /**
   * Transitions the operation to {@link DeferredIndexStatus#COMPLETED}
   * and records its completion time.
   *
   * @param id            the operation to update.
   * @param completedTime completion timestamp (yyyyMMddHHmmss).
   */
  void markCompleted(long id, long completedTime);


  /**
   * Transitions the operation to {@link DeferredIndexStatus#FAILED},
   * records the error message, and stores the updated retry count.
   *
   * @param id            the operation to update.
   * @param errorMessage  the error message.
   * @param newRetryCount the new retry count value.
   */
  void markFailed(long id, String errorMessage, int newRetryCount);


  /**
   * Resets a {@link DeferredIndexStatus#FAILED} operation back to
   * {@link DeferredIndexStatus#PENDING} so it will be retried.
   *
   * @param id the operation to reset.
   */
  void resetToPending(long id);


  /**
   * Updates the status of an operation to the supplied value.
   *
   * @param id        the operation to update.
   * @param newStatus the new status value.
   */
  void updateStatus(long id, DeferredIndexStatus newStatus);


  /**
   * Returns {@code true} if there is at least one operation in a non-terminal
   * state ({@link DeferredIndexStatus#PENDING} or
   * {@link DeferredIndexStatus#IN_PROGRESS}). Used by
   * {@link DeferredIndexExecutor#awaitCompletion(long)} to poll until the queue
   * is drained.
   *
   * @return {@code true} if any PENDING or IN_PROGRESS operations exist.
   */
  boolean hasNonTerminalOperations();
}
