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
 * Status of an index tracked in the DeployedIndexes table.
 *
 * <p>Non-deferred indexes are always {@link #COMPLETED}. Deferred indexes
 * transition through the lifecycle: {@link #PENDING} &rarr;
 * {@link #IN_PROGRESS} &rarr; {@link #COMPLETED} or {@link #FAILED}.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public enum DeployedIndexStatus {

  /** Queued for background creation. Not yet physically built. */
  PENDING,

  /** Currently being built by the application. */
  IN_PROGRESS,

  /** Successfully built and physically present in the database. */
  COMPLETED,

  /** Build failed. {@code retryCount} indicates the number of attempts. */
  FAILED
}
