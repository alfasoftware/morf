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

import java.util.concurrent.CompletableFuture;

import com.google.inject.ImplementedBy;

/**
 * Picks up {@link DeferredIndexStatus#PENDING} operations and builds them
 * asynchronously using a thread pool. Results are written to the database
 * (each operation is marked {@link DeferredIndexStatus#COMPLETED} or
 * {@link DeferredIndexStatus#FAILED}).
 *
 * <p>This is an internal service — callers should use
 * {@link DeferredIndexService} which provides blocking orchestration
 * on top of this executor.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexExecutorImpl.class)
interface DeferredIndexExecutor {

  /**
   * Picks up all {@link DeferredIndexStatus#PENDING} operations and submits
   * them to a thread pool for asynchronous index building. Returns immediately
   * with a future that completes when all submitted operations reach a terminal
   * state.
   *
   * @return a future that completes when all operations are done; completes
   *         immediately if there are no pending operations.
   */
  CompletableFuture<Void> execute();


  /**
   * Forces immediate shutdown of the thread pool and progress logger.
   * Use for cancellation on timeout; normal completion is handled
   * automatically when the returned future completes.
   */
  void shutdown();
}
