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

package org.alfasoftware.morf.upgrade.deployedindexes;

/**
 * One unit of background-build work for a single deferred index. Each task is
 * self-contained: when {@link #run()} executes, it opens its own JDBC
 * connection, observes the physical state of the target index via
 * {@link org.alfasoftware.morf.jdbc.SqlDialect#isIndexValid}, and reconciles
 * the {@code DeployedIndexes} tracking row to match — creating, dropping and
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
}
