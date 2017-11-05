/* Copyright 2017 Alfa Financial Software
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

package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Represents the {@link SelectStatement#useIndex(org.alfasoftware.morf.sql.element.TableReference, String)}
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public final class UseIndex implements Hint {

  private final TableReference table;
  private final String indexName;

  public UseIndex(TableReference table, String indexName) {
    this.table = table;
    this.indexName = indexName;
  }


  /**
   * @return The table whose index should be used.
   */
  public TableReference getTable() {
    return table;
  }


  /**
   * @return The name of the index to use.
   */
  public String getIndexName() {
    return indexName;
  }


  @Override
  public String toString() {
    return "UseIndex [table=" + table + ", index=" + indexName + "]";
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    UseIndex other = (UseIndex) obj;
    if (indexName == null) {
      if (other.indexName != null)
        return false;
    } else if (!indexName.equals(other.indexName))
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }
}