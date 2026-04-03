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
 * Public facade for the deferred index creation mechanism. Adopters inject
 * this interface and invoke it <em>after</em> the upgrade completes to start
 * background index builds.
 *
 * <p>Deferred indexes are declared in table comments by the upgrade framework.
 * The MetaDataProvider reads these comments and exposes unbuilt deferred
 * indexes as virtual indexes with {@code isDeferred()=true}. This service
 * scans for such indexes and builds them asynchronously.</p>
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
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexServiceImpl.class)
public interface DeferredIndexService {

  /**
   * Scans the database schema and starts building all unbuilt deferred
   * indexes asynchronously. Returns immediately.
   *
   * <p>Use {@link #awaitCompletion(long)} to block until all operations
   * reach a terminal state.</p>
   */
  void execute();


  /**
   * Blocks until all deferred index operations reach a terminal state
   * (completed or failed), or until the timeout elapses.
   *
   * <p>A value of zero means "wait indefinitely". This is acceptable here
   * because the caller explicitly opts in to blocking after startup.</p>
   *
   * @param timeoutSeconds maximum time to wait; zero means wait indefinitely.
   * @return {@code true} if all operations completed within the timeout;
   *         {@code false} if the timeout elapsed first.
   * @throws IllegalStateException if called before {@link #execute()}.
   */
  boolean awaitCompletion(long timeoutSeconds);


  /**
   * Returns the SQL statements that would be executed to build all
   * currently unbuilt deferred indexes, without actually executing them.
   *
   * @return a list of SQL statements; empty if there are no deferred
   *         indexes to build.
   */
  List<String> getMissingDeferredIndexStatements();
}
