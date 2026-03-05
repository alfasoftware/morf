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

import java.util.Map;

import com.google.inject.ImplementedBy;

/**
 * Public facade for the deferred index creation mechanism. Adopters inject
 * this interface and invoke it <em>after</em> the upgrade completes to start
 * background index builds.
 *
 * <p><strong>Post-upgrade execution is the adopter's responsibility.</strong>
 * The upgrade framework does <em>not</em> automatically run this service.
 * A pre-upgrade {@link DeferredIndexReadinessCheck} is wired into the
 * upgrade pipeline as a safety net: if the adopter forgets to call this
 * service, the next upgrade will force-build any outstanding indexes
 * before proceeding.</p>
 *
 * <p>Typical usage (Guice path):</p>
 * <pre>
 * &#064;Inject DeferredIndexService deferredIndexService;
 *
 * // Run upgrade...
 * upgrade.findPath(targetSchema, steps, exceptionRegexes, dataSource);
 *
 * // Then start building deferred indexes in the background:
 * deferredIndexService.execute();
 *
 * // Optionally block until all indexes are built (or time out):
 * boolean done = deferredIndexService.awaitCompletion(600);
 * if (!done) {
 *   log.warn("Deferred index builds still in progress");
 * }
 * </pre>
 *
 * @see DeferredIndexReadinessCheck
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
   * Blocks until all deferred index operations reach a terminal state
   * ({@code COMPLETED} or {@code FAILED}), or until the timeout elapses.
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations reached a terminal state within the
   *         timeout; {@code false} if the timeout elapsed first.
   * @throws IllegalStateException if called before {@link #execute()}.
   */
  boolean awaitCompletion(long timeoutSeconds);


  /**
   * Returns the current count of deferred index operations grouped by status.
   *
   * <p>Adopters can poll this method on their own schedule (e.g. from a
   * health endpoint or timer) to monitor progress.</p>
   *
   * @return a map from each {@link DeferredIndexStatus} to its count;
   *         statuses with no operations have a count of zero.
   */
  Map<DeferredIndexStatus, Integer> getProgress();
}
