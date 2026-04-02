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
import java.util.concurrent.CompletableFuture;

import com.google.inject.ImplementedBy;

/**
 * Scans the database schema for deferred indexes that have not yet been
 * physically built and builds them asynchronously using a thread pool.
 *
 * <p>A deferred index is identified by {@code Index.isDeferred() == true}
 * in the schema returned by the MetaDataProvider. The MetaDataProvider
 * merges comment-declared deferred indexes with physical indexes: if a
 * comment declares a deferred index but the physical index does not yet
 * exist, the MetaDataProvider returns a virtual index with
 * {@code isDeferred()=true}. Once physically built, the physical index
 * takes precedence and {@code isDeferred()} returns {@code false}.</p>
 *
 * <p>This is an internal service &mdash; callers should use
 * {@link DeferredIndexService} which provides blocking orchestration
 * on top of this executor.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexExecutorImpl.class)
interface DeferredIndexExecutor {

  /**
   * Scans the database schema for deferred indexes not yet physically
   * built and submits them to a thread pool for asynchronous building.
   * Returns immediately with a future that completes when all submitted
   * operations reach a terminal state.
   *
   * @return a future that completes when all operations are done; completes
   *         immediately if there are no deferred indexes to build.
   */
  CompletableFuture<Void> execute();


  /**
   * Scans the database schema for deferred indexes not yet physically
   * built and returns the SQL statements that would be executed to build
   * them, without actually executing.
   *
   * @return a list of SQL statements; empty if there are no deferred
   *         indexes to build.
   */
  List<String> getMissingDeferredIndexStatements();
}
