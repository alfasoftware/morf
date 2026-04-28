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
 * run) are tracked by {@link DeferredIndexSession} and composed
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

  private final Map<IndexKey, IndexPresence> presence;


  DeployedIndexState(Map<IndexKey, IndexPresence> presence) {
    this.presence = Collections.unmodifiableMap(new HashMap<>(presence));
  }


  /**
   * @return an empty state (nothing known).
   */
  public static DeployedIndexState empty() {
    return new DeployedIndexState(Collections.emptyMap());
  }


  /**
   * Test-friendly factory: returns a state containing the single
   * {@code (tableName, indexName) → presence} entry. Compose by calling
   * {@link #with(String, String, IndexPresence)} on the result.
   *
   * @param tableName table name (case-insensitive).
   * @param indexName index name (case-insensitive).
   * @param presence presence to record.
   * @return a state containing the one entry.
   */
  public static DeployedIndexState of(String tableName, String indexName, IndexPresence presence) {
    Map<IndexKey, IndexPresence> map = new HashMap<>();
    map.put(new IndexKey(tableName, indexName), presence);
    return new DeployedIndexState(map);
  }


  /**
   * Test-friendly combinator: returns a new state with one extra entry.
   *
   * @param tableName table name.
   * @param indexName index name.
   * @param p presence to record.
   * @return a new state including this entry plus all existing entries.
   */
  public DeployedIndexState with(String tableName, String indexName, IndexPresence p) {
    Map<IndexKey, IndexPresence> map = new HashMap<>(this.presence);
    map.put(new IndexKey(tableName, indexName), p);
    return new DeployedIndexState(map);
  }


  /**
   * Returns what the enricher recorded for this index. UNKNOWN is the normal
   * result for in-session additions — see {@link IndexPresence#UNKNOWN} for
   * the happy-path-vs-bug distinction and caller interpretations.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return {@link IndexPresence#PRESENT} / {@link IndexPresence#ABSENT} if
   *         the enricher recorded it; {@link IndexPresence#UNKNOWN}
   *         otherwise.
   */
  public IndexPresence getPresence(String tableName, String indexName) {
    return presence.getOrDefault(new IndexKey(tableName, indexName), IndexPresence.UNKNOWN);
  }
}
