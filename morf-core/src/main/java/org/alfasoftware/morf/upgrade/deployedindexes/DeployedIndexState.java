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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Runtime-observed state of tracked indexes at the moment an upgrade began.
 * Produced by {@link DeployedIndexesModelEnricher} from the physical catalog
 * plus the {@code DeployedIndexes} table, and consulted by the visitor and
 * the deferred-SQL scan to answer operational questions that have no place
 * on the declarative {@link org.alfasoftware.morf.metadata.Index} model.
 *
 * <p>Separating this state from the schema data keeps the
 * {@link org.alfasoftware.morf.metadata.Index} interface purely declarative
 * — "what the index looks like" — while operational facts — "is it
 * physically there?" — live here.</p>
 *
 * <p>The state is a snapshot: it reflects the database at the start of the
 * upgrade. In-session mutations (indexes added/removed by steps in this
 * run) are tracked by {@link DeployedIndexesChangeService} and composed
 * with this state by the visitor.</p>
 *
 * <p>Every query returns {@link IndexPresence}, a three-valued enum: the
 * tri-state nature of "physical presence" (known present, known absent,
 * or not seen) is explicit in the type. Callers decide what UNKNOWN means
 * in their context by comparing against PRESENT or ABSENT as appropriate,
 * e.g. {@code getPresence(...) != ABSENT} for "present or unknown" and
 * {@code getPresence(...) != PRESENT} for "absent or unknown".</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public final class DeployedIndexState {

  /** Key format: {@code TABLE_UPPER + ':' + INDEX_UPPER}. */
  private final Map<String, IndexPresence> presence;


  DeployedIndexState(Map<String, IndexPresence> presence) {
    this.presence = Collections.unmodifiableMap(new HashMap<>(presence));
  }


  /**
   * @return an empty state (nothing known).
   */
  public static DeployedIndexState empty() {
    return new DeployedIndexState(Collections.emptyMap());
  }


  /**
   * Returns what the enricher recorded for this index.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return {@link IndexPresence#PRESENT} / {@link IndexPresence#ABSENT} if
   *         the enricher recorded it; {@link IndexPresence#UNKNOWN}
   *         otherwise.
   */
  public IndexPresence getPresence(String tableName, String indexName) {
    return presence.getOrDefault(key(tableName, indexName), IndexPresence.UNKNOWN);
  }


  static String key(String tableName, String indexName) {
    return tableName.toUpperCase() + ":" + indexName.toUpperCase();
  }
}
