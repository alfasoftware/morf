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
import java.util.Map;

import com.google.inject.ImplementedBy;

/**
 * DAO for reading and writing {@link DeferredIndexOperation} records.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexOperationDAOImpl.class)
interface DeferredIndexOperationDAO {

  /**
   * Returns all {@link DeferredIndexStatus#PENDING} operations with
   * their ordered column names populated.
   *
   * @return list of pending operations.
   */
  List<DeferredIndexOperation> findPendingOperations();


  /**
   * Transitions the operation to {@link DeferredIndexStatus#IN_PROGRESS}
   * and records its start time.
   *
   * @param id          the operation to update.
   * @param startedTime start timestamp (epoch milliseconds).
   */
  void markStarted(long id, long startedTime);


  /**
   * Transitions the operation to {@link DeferredIndexStatus#COMPLETED}
   * and records its completion time.
   *
   * @param id            the operation to update.
   * @param completedTime completion timestamp (epoch milliseconds).
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
   * Resets all {@link DeferredIndexStatus#IN_PROGRESS} operations to
   * {@link DeferredIndexStatus#PENDING}. Used for crash recovery: any
   * operation that was mid-build when the process died should be retried.
   */
  void resetAllInProgressToPending();


  /**
   * Returns all operations in a non-terminal state
   * ({@link DeferredIndexStatus#PENDING}, {@link DeferredIndexStatus#IN_PROGRESS},
   * or {@link DeferredIndexStatus#FAILED}) with their ordered column names populated.
   *
   * @return list of non-terminal operations.
   */
  List<DeferredIndexOperation> findNonTerminalOperations();


  /**
   * Returns the count of operations grouped by status.
   *
   * @return a map from each {@link DeferredIndexStatus} to its count;
   *         statuses with no operations have a count of zero.
   */
  Map<DeferredIndexStatus, Integer> countAllByStatus();
}
