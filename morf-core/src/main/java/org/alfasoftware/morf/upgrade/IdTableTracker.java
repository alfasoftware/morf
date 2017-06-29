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

package org.alfasoftware.morf.upgrade;

/**
 * A very simple {@link TableNameResolver} which only resolves
 * the active name for ID table.
 */
public class IdTableTracker implements TableNameResolver {

  private final String idTableName;

  /**
   * Default constructor.
   * @param idTableName The name for the ID table.
   */
  public IdTableTracker(String idTableName) {
    this.idTableName = idTableName;

  }

  /**
   * @see org.alfasoftware.morf.upgrade.TableNameResolver#activeNameFor(java.lang.String)
   */
  @Override
  public String activeNameFor(String tableName) {
    if ("IDTABLE".equals(tableName))
      return idTableName;
    return tableName;
  }

}
