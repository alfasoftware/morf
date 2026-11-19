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

import java.util.List;
import java.util.Map;

import com.google.inject.ImplementedBy;

/**
 * Adopter-facing entry point for the background-build flow. The implementation
 * carries no thread of its own — the adopter calls {@link #getBuildTasks()},
 * drains the returned list onto whatever scheduler/executor it owns (serial,
 * a thread pool, CommonJ, etc.), and repeats periodically.
 *
 * <p>Intended adopter loop:</p>
 * <pre>
 * // On boot, and again on a periodic timer:
 * service.getBuildTasks().forEach(Runnable::run);
 * </pre>
 *
 * <p>Each {@link DeferredIndexBuildTask} is self-contained and idempotent: it
 * re-fetches its row, observes physical state, and reconciles. {@code FAILED}
 * is non-terminal — failed rows reappear in {@link #getBuildTasks()} until
 * {@code COMPLETED}.</p>
 *
 * <p><b>Single-node assumption.</b> The adopter must ensure only one process
 * calls {@link #getBuildTasks()} at a time across the cluster. Morf has no
 * runtime detection.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexServiceImpl.class)
public interface DeferredIndexService {

  /**
   * Returns one task per non-{@code COMPLETED} registration row. Each task, when
   * run, performs full reconciliation for its (table, index) — including the
   * {@code isIndexValid} check, status updates, and (where applicable) the
   * {@code DROP INDEX} + {@code CREATE INDEX} pair.
   *
   * <p>The list is a snapshot at call time. Adopter is free to run tasks in
   * any order, in parallel, or via whatever executor.</p>
   *
   * @return one task per non-{@code COMPLETED} registered deferred index.
   */
  List<DeferredIndexBuildTask> getBuildTasks();


  /**
   * Read-only progress summary for monitoring/UI.
   *
   * @return count of registration rows grouped by {@link DeferredIndexStatus}.
   */
  Map<DeferredIndexStatus, Integer> getProgress();
}
