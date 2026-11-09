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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;

/**
 * {@link SchemaChange} which consists of removing an existing column from a
 * table within a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class RemoveColumn implements SchemaChange {

  /** Name of table to remove the column from. */
  private final String tableName;

  /** Definition of column. */
  private final Column columnDefinition;


  /**
   * Construct a {@link RemoveColumn} schema change which can apply the
   * change to metadata.
   *
   * @param tableName name of table to remove the column from.
   * @param column {@link Column} original column definition
   */
  public RemoveColumn(String tableName, Column column) {
    this.tableName = tableName;
    this.columnDefinition = column;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    Table original = schema.getTable(tableName);
    boolean foundColumn = false;
    boolean removedFromPrimaryKey = false;

    List<String> columns = new ArrayList<>();
    for (Column column : original.columns()) {
      // So long as this is not the column we're supposed to be removing
      if (column.getName().equalsIgnoreCase(columnDefinition.getName())) {
        foundColumn = true;
        removedFromPrimaryKey = column.isPrimaryKey();
        continue;
      }

      // Add the column to the schema
      columns.add(column.getName());
    }

    if (!foundColumn) {
      throw new IllegalArgumentException("Cannot remove column [" + columnDefinition.getName() + "] as it does not exist on table [" + tableName + "]");
    }

    Schema result = new TableOverrideSchema(schema, new AlteredTable(original, columns));

    if (removedFromPrimaryKey) {
      Table table = result.getTable(tableName);
      List<String> primaryKeyColumnNames = SchemaUtils.upperCaseNamesOfColumns(SchemaUtils.primaryKeysForTable(table));
      for (Index index : table.indexes()) {
        if (!primaryKeyColumnNames.isEmpty() && SchemaUtils.toUpperCase(index.columnNames()).equals(primaryKeyColumnNames)) {
          throw new IllegalArgumentException("Cannot remove column [" + columnDefinition.getName() + "] from table [" + tableName + "] as the remaining primary key would have the same columns as index [" + index.getName() + "]");
        }
      }
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    // If we can't find the table assume we are not applied. If the table is removed
    // in a subsequent step it is up to that step to mark itself dependent on this one.
    if (!schema.tableExists(tableName)) {
      return false;
    }

    Table table = schema.getTable(tableName);
    SchemaHomology homology = new SchemaHomology();
    for (Column column : table.columns()) {
      if (homology.columnsMatch(column, columnDefinition)) {
        return false;
      }
    }
    return true;

  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    Table original = schema.getTable(tableName);
    List<String> columns = new ArrayList<>();
    for (Column column : original.columns()) {
      columns.add(column.getName());
    }
    columns.add(columnDefinition.getName());

    return new TableOverrideSchema(schema, new AlteredTable(original, columns, Arrays.asList(new Column[] {columnDefinition})));
  }


  /**
   * Gets the definition of the column to be removed.
   *
   * @return the definition of the column
   */
  public Column getColumnDefinition() {
    return columnDefinition;
  }


  /**
   * Gets the name of the table.
   *
   * @return the table's name
   */
  public String getTableName() {
    return tableName;
  }


  @Override
  public String toString() {
    return "RemoveColumn [tableName=" + tableName + ", columnDefinition=" + columnDefinition + "]";
  }

}
