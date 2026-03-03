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

import com.google.inject.ImplementedBy;

/**
 * Public facade for the deferred index creation mechanism. Adopters inject this
 * interface to manage the lifecycle of background index builds that were queued
 * during upgrade.
 *
 * <p>Typical usage on the <strong>active node</strong> (the one that runs upgrades):</p>
 * <pre>
 * &#064;Inject DeferredIndexService deferredIndexService;
 *
 * // After upgrade completes, build deferred indexes:
 * ExecutionResult result = deferredIndexService.execute();
 * log.info("Built " + result.getCompletedCount() + " indexes");
 * </pre>
 *
 * <p>On <strong>passive nodes</strong> (waiting for another node to finish building):</p>
 * <pre>
 * boolean done = deferredIndexService.awaitCompletion(600);
 * if (!done) {
 *   throw new IllegalStateException("Timed out waiting for deferred indexes");
 * }
 * </pre>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexServiceImpl.class)
public interface DeferredIndexService {

  /**
   * Recovers stale operations, executes all pending deferred index builds,
   * and blocks until they complete or fail.
   *
   * <p>Steps performed:</p>
   * <ol>
   *   <li>Recover stale {@code IN_PROGRESS} operations (crashed executors).</li>
   *   <li>Execute all {@code PENDING} operations using a thread pool.</li>
   *   <li>Block until all operations reach a terminal state or the configured
   *       timeout elapses.</li>
   * </ol>
   *
   * @return summary of completed and failed operation counts.
   * @throws IllegalStateException if any operations failed permanently.
   */
  ExecutionResult execute();


  /**
   * Polls the database until no {@code PENDING} or {@code IN_PROGRESS}
   * operations remain, or until the timeout elapses. This method does
   * <em>not</em> execute any index builds — it is intended for passive nodes
   * in a multi-instance deployment that must wait for another node to finish
   * building indexes.
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations reached a terminal state within the
   *         timeout; {@code false} if the timeout elapsed first.
   */
  boolean awaitCompletion(long timeoutSeconds);


  /**
   * Summary of the outcome of an {@link #execute()} call.
   */
  public static final class ExecutionResult {

    private final int completedCount;
    private final int failedCount;

    /**
     * Constructs an execution result.
     *
     * @param completedCount the number of operations that completed successfully.
     * @param failedCount    the number of operations that failed permanently.
     */
    public ExecutionResult(int completedCount, int failedCount) {
      this.completedCount = completedCount;
      this.failedCount = failedCount;
    }

    /**
     * @return the number of operations that completed successfully.
     */
    public int getCompletedCount() {
      return completedCount;
    }

    /**
     * @return the number of operations that failed permanently.
     */
    public int getFailedCount() {
      return failedCount;
    }
  }
}
