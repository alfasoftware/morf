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
 * One unit of background-build work for a single deferred index. Each task is
 * self-contained: when {@link #run()} executes, it opens its own JDBC
 * connection, observes the physical state of the target index via
 * {@link org.alfasoftware.morf.jdbc.SqlDialect#isIndexValid}, and reconciles
 * the {@code DeferredIndexes} registration row to match — creating, dropping and
 * rebuilding, or simply marking complete as appropriate.
 *
 * <p>{@link #run()} returns when this task's index has reached a steady state
 * for the current pass: either {@code COMPLETED} (success) or {@code FAILED}
 * (a recoverable error has been persisted). Expected SQL outcomes are caught
 * inside the task and persisted to the row's {@code status} +
 * {@code errorMessage} columns. Unexpected runtime errors propagate as
 * {@link RuntimeException}.</p>
 *
 * <p><b>Adopters control parallelism.</b> The interface extends
 * {@link Runnable} so the same instances run via:</p>
 * <pre>
 * // Serial:
 * service.getBuildTasks().forEach(Runnable::run);
 *
 * // Parallel (adopter-owned executor):
 * ExecutorService pool = Executors.newFixedThreadPool(8);
 * service.getBuildTasks().forEach(pool::submit);
 *
 * // CommonJ (adopter wraps each task in a 5-line Work delegate):
 * service.getBuildTasks().forEach(t -&gt; workManager.schedule(new MyWorkAdapter(t)));
 * </pre>
 *
 * <p>Morf spawns no threads.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeferredIndexBuildTask extends Runnable {

  /** @return the table the deferred index belongs to. */
  String getTableName();


  /** @return the deferred index name. */
  String getIndexName();


  /**
   * @return the row's status as observed when {@link DeferredIndexService#getBuildTasks()}
   *     captured the snapshot. Non-{@code COMPLETED} by construction (the service only
   *     hands out tasks for non-terminal rows). Snapshot-only -- live state may have
   *     advanced by the time {@link #run()} executes; the task itself re-fetches the
   *     row before deciding what to do.
   */
  DeferredIndexStatus getStatus();


  /**
   * @return the row's {@code attemptsCount} as observed at snapshot time. Adopters can
   *     filter this list to skip rows that have already retried too many times -- e.g.
   *     {@code service.getBuildTasks().stream().filter(t -> t.getAttemptsCount() < 5)
   *     .forEach(Runnable::run)}.
   *
   *     <p>Snapshot-only. The next {@link #run()} that executes the {@code markStarted}
   *     path will increment the live value -- so the snapshot represents prior attempts,
   *     not including the about-to-start one.</p>
   */
  int getAttemptsCount();


  /**
   * @return the {@code errorMessage} from the most recent failure (if any), as observed
   *     at snapshot time. {@code Optional.empty()} when the row has never failed or when
   *     the most recent {@code markCompleted} cleared it.
   */
  Optional<String> getErrorMessage();
}
