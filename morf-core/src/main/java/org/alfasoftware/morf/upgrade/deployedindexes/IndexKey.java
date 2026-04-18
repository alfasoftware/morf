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

import java.util.Objects;

/**
 * Composite {@code (tableName, indexName)} key used as the lookup key in
 * {@link DeployedIndexState}'s presence map. Case-insensitive: both names
 * are upper-cased on construction so equality and hashing match regardless
 * of the caller's casing.
 *
 * <p>Replaces an earlier string-concat convention (<code>TABLE:INDEX</code>)
 * so callers don't need to know the key format and collisions are
 * impossible.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
final class IndexKey {

  private final String tableUpper;
  private final String indexUpper;


  /**
   * @param tableName table name (non-null, case-insensitive).
   * @param indexName index name (non-null, case-insensitive).
   */
  IndexKey(String tableName, String indexName) {
    this.tableUpper = Objects.requireNonNull(tableName, "tableName").toUpperCase();
    this.indexUpper = Objects.requireNonNull(indexName, "indexName").toUpperCase();
  }


  /** @return true if {@code o} is an {@link IndexKey} with matching upper-cased names. */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IndexKey)) {
      return false;
    }
    IndexKey k = (IndexKey) o;
    return tableUpper.equals(k.tableUpper) && indexUpper.equals(k.indexUpper);
  }


  /** @return hash consistent with {@link #equals(Object)}. */
  @Override
  public int hashCode() {
    return Objects.hash(tableUpper, indexUpper);
  }


  /** @return {@code TABLE_UPPER:INDEX_UPPER} — diagnostic only. */
  @Override
  public String toString() {
    return tableUpper + ":" + indexUpper;
  }
}
