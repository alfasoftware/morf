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
}