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

package org.alfasoftware.morf.upgrade.deferredindexes;

import java.util.Optional;

/**
 * Package-private holder paired with one registration row snapshot. Exposes the
 * snapshot via {@link DeferredIndexBuildTask}'s read-only getters and delegates
 * {@link #run()} to the shared {@link DeferredIndexBuilder} -- which carries
 * the actual reconciliation algorithm.
 *
 * <p>Splitting the data (this class) from the behaviour (the builder) means
 * one stateless builder is shared by the whole task fan-out, and each task
 * is just the snapshot bundled with a callback. The algorithm is unit-testable
 * in isolation against the builder; the wrapper is thin enough to need no
 * dedicated test.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
class DeferredIndexBuildTaskImpl implements DeferredIndexBuildTask {

  private final DeferredIndex snapshot;
  private final DeferredIndexBuilder builder;


  /**
   * @param snapshot the registration row state captured at task-creation time;
   *     read-only, exposed via the snapshot getters. The builder re-fetches the
   *     row before acting -- the snapshot is purely advisory for adopter-side
   *     filtering and per-row diagnostics.
   * @param builder the shared, stateless reconciliation algorithm. {@link #run()}
   *     delegates straight to {@link DeferredIndexBuilder#build(DeferredIndex)}.
   */
  DeferredIndexBuildTaskImpl(DeferredIndex snapshot, DeferredIndexBuilder builder) {
    this.snapshot = snapshot;
    this.builder = builder;
  }


  @Override
  public String getTableName() {
    return snapshot.getTableName();
  }


  @Override
  public String getIndexName() {
    return snapshot.getIndexName();
  }


  @Override
  public DeferredIndexStatus getStatus() {
    return snapshot.getStatus();
  }


  @Override
  public int getAttemptsCount() {
    return snapshot.getAttemptsCount();
  }


  @Override
  public Optional<String> getErrorMessage() {
    return Optional.ofNullable(snapshot.getErrorMessage());
  }


  @Override
  public void run() {
    builder.build(snapshot);
  }
}
