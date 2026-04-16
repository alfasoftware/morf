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
 * <p>Presence questions come in two flavours because the right default for
 * unknown entries depends on who is asking:</p>
 * <ul>
 *   <li>{@link #isKnownPhysicallyPresent(String, String)} — only
 *       {@code true} if we explicitly recorded the index as present. Use
 *       for "should we emit a CREATE INDEX for this deferred entry?" —
 *       an unknown entry is a new deferred index from this session and
 *       needs its CREATE INDEX emitted.</li>
 *   <li>{@link #isKnownPhysicallyAbsent(String, String)} — only
 *       {@code true} if we explicitly recorded the index as absent (a
 *       virtual deferred entry from the tracking table). Use for "should
 *       we skip physical DDL for this schema change?" — an unknown entry
 *       is assumed present (either pre-existing or added earlier in this
 *       session).</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public final class DeployedIndexState {

  /** Key format: {@code TABLE_UPPER + ':' + INDEX_UPPER}. */
  private final Map<String, Boolean> physicallyPresent;


  DeployedIndexState(Map<String, Boolean> physicallyPresent) {
    this.physicallyPresent = Collections.unmodifiableMap(new HashMap<>(physicallyPresent));
  }


  /**
   * @return an empty state (nothing known).
   */
  public static DeployedIndexState empty() {
    return new DeployedIndexState(Collections.emptyMap());
  }


  /**
   * Whether the enricher recorded this index as physically present.
   * Returns {@code false} both for indexes known to be absent and for
   * indexes the enricher didn't see at all.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return {@code true} iff the enricher saw this index physically.
   */
  public boolean isKnownPhysicallyPresent(String tableName, String indexName) {
    Boolean known = physicallyPresent.get(key(tableName, indexName));
    return known != null && known;
  }


  /**
   * Whether the enricher recorded this index as physically absent (e.g. a
   * virtual deferred entry from the {@code DeployedIndexes} table with no
   * matching physical index). Returns {@code false} both for indexes known
   * to be present and for indexes the enricher didn't see at all.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return {@code true} iff the enricher recorded this index as absent.
   */
  public boolean isKnownPhysicallyAbsent(String tableName, String indexName) {
    Boolean known = physicallyPresent.get(key(tableName, indexName));
    return known != null && !known;
  }


  static String key(String tableName, String indexName) {
    return tableName.toUpperCase() + ":" + indexName.toUpperCase();
  }
}
