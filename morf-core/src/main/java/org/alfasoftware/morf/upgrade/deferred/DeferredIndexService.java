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
 * <p>Typical usage:</p>
 * <pre>
 * &#064;Inject DeferredIndexService deferredIndexService;
 *
 * // After upgrade completes, start building deferred indexes:
 * deferredIndexService.execute();
 *
 * // Block until all indexes are built (or time out):
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
   * Recovers stale operations and starts building all pending deferred
   * indexes asynchronously. Returns immediately.
   *
   * <p>Use {@link #awaitCompletion(long)} to block until all operations
   * reach a terminal state.</p>
   */
  void execute();


  /**
   * Polls the database until no {@code PENDING} or {@code IN_PROGRESS}
   * operations remain, or until the timeout elapses.
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations reached a terminal state within the
   *         timeout; {@code false} if the timeout elapsed first.
   */
  boolean awaitCompletion(long timeoutSeconds);
}
