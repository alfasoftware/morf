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
 * Builds deferred indexes asynchronously using a thread pool. Receives
 * a pre-computed list of {@link DeferredAddIndex} operations to build
 * (determined by replaying upgrade steps and comparing against the live
 * database schema).
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
   * Builds the given deferred indexes asynchronously. Returns immediately
   * with a future that completes when all submitted operations reach a
   * terminal state (built or permanently failed).
   *
   * @param missingIndexes deferred indexes not yet built in the database.
   * @return a future that completes when all operations are done; completes
   *         immediately if the list is empty.
   */
  CompletableFuture<Void> execute(List<DeferredAddIndex> missingIndexes);
}
