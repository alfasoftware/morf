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

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Schema change to correct the primary key of a table to a new set of columns regardless of the start state.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class CorrectPrimaryKeyColumns extends ChangePrimaryKeyColumns {

  private final String tableName;
  private final List<String> newPrimaryKeyColumns;
  private List<String> oldPrimaryKeyColumns;

  /**
   * @param tableName The target table
   * @param newPrimaryKeyColumns The new primary key columns
   */
  public CorrectPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns) {
    super(tableName, Collections.<String> emptyList(), newPrimaryKeyColumns);
    this.tableName = tableName;
    this.newPrimaryKeyColumns = newPrimaryKeyColumns;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns#assertExistingPrimaryKey(java.util.List,
   *      org.alfasoftware.morf.metadata.Table)
   */
  @Override
  protected void assertExistingPrimaryKey(List<String> from, Table table) {
    // Can't check the existing state as we don't know what it is.
  }


  /**
   * @see org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    oldPrimaryKeyColumns = namesOfColumns(primaryKeysForTable(schema.getTable(tableName)));
    return super.apply(schema);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    if (oldPrimaryKeyColumns != null) {
      return applyChange(schema, newPrimaryKeyColumns, oldPrimaryKeyColumns);
    } else {
      return schema;
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns#getOldPrimaryKeyColumns()
   */
  @Override
  public List<String> getOldPrimaryKeyColumns() {
    return oldPrimaryKeyColumns == null ? super.getOldPrimaryKeyColumns() : oldPrimaryKeyColumns;
  }
}
