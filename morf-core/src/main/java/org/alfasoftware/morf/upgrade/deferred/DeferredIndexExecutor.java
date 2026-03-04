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
 * Executes pending deferred index operations queued in the
 * {@code DeferredIndexOperation} table by issuing the appropriate
 * {@code CREATE INDEX} DDL and marking each operation as
 * {@link DeferredIndexStatus#COMPLETED} or {@link DeferredIndexStatus#FAILED}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexExecutorImpl.class)
interface DeferredIndexExecutor {

  /**
   * Picks up all {@link DeferredIndexStatus#PENDING} operations, builds the
   * corresponding indexes, and blocks until all operations reach a terminal
   * state or the timeout elapses.
   *
   * @param timeoutMs maximum time in milliseconds to wait for all operations to
   *                  complete; zero means wait indefinitely.
   * @return summary of how many operations completed and how many failed.
   */
  DeferredIndexExecutionResult executeAndWait(long timeoutMs);


  /**
   * Blocks until all operations in the {@code DeferredIndexOperation} table are
   * in a terminal state ({@link DeferredIndexStatus#COMPLETED} or
   * {@link DeferredIndexStatus#FAILED}), or until the timeout elapses. This
   * method does <em>not</em> start or trigger execution.
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations reached a terminal state within the
   *         timeout; {@code false} if the timeout elapsed first.
   */
  boolean awaitCompletion(long timeoutSeconds);


  /**
   * Shuts down any background threads started by the most recent
   * {@link #executeAndWait} call.
   */
  void shutdown();
}
