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
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;
import org.apache.commons.lang3.StringUtils;

/**
 * {@link SchemaChange} which consists of adding a new column to an existing
 * table within a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class AddColumn implements SchemaChange {

  /** Name of table to be added. */
  private final String tableName;

  /** Definition of column. */
  private final Column newColumnDefinition;


  /**
   * Construct an {@link AddColumn} schema change which can apply the
   * change to metadata.
   *
   * @param tableName name of table to add column to.
   * @param column {@link Column} new column definition
   */
  public AddColumn(String tableName, Column column) {
    this.tableName = tableName;
    this.newColumnDefinition = column;

    if (column.isPrimaryKey()) {
      throw new IllegalArgumentException("Cannot add primary key column [" + column.getName() + "] to existing table [" + tableName + "]");
    }

    if (!column.isNullable() && StringUtils.isEmpty(column.getDefaultValue())) {
      throw new IllegalArgumentException("Cannot add non-nullable column [" + column.getName() + "] to existing table [" + tableName + "] without a default value");
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   * @return a new {@link Schema} which results from the removal of the relevant table from <var>schema</var>
   */
  @Override
  public Schema reverse(Schema metadata) {
    Table original = metadata.getTable(tableName);
    List<String> columns = new ArrayList<>();
    boolean found = false;
    for (Column column : original.columns()) {
      if (column.getName().equalsIgnoreCase(newColumnDefinition.getName())) {
        found = true;
      } else {
        columns.add(column.getName());
      }
    }

    // Remove the column we are filtering
    if (!found) {
      String columnsInTargetTable = original.columns().toString();
      throw new IllegalStateException(
        "Column [" + newColumnDefinition + "] not found in table [" + tableName + "] so it could not be removed.\n" +
        "Columns in target table [" + tableName + "]:\n" +
        columnsInTargetTable.replace(",", ",\n")
      );
    }

    return new TableOverrideSchema(metadata, new AlteredTable(original, columns));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   * @return a new {@link Schema} which results from the addition of the new column to the relevant table in <var>schema</var>
   */
  @Override
  public Schema apply(Schema schema) {
    Table original = schema.getTable(tableName);
    List<String> columns = new ArrayList<>();

    for (Column column : original.columns()) {
      if (column.getName().equalsIgnoreCase(newColumnDefinition.getName())) {
        throw new IllegalStateException("Column [" + newColumnDefinition.getName() + "] is already present on table [" + tableName + "] so cannot be added.");
      }

      columns.add(column.getName());
    }
    columns.add(newColumnDefinition.getName());

    return new TableOverrideSchema(schema, new AlteredTable(original, columns, Arrays.asList(new Column[] {newColumnDefinition})));
  }


  /**
   * @return the name of the table to add the column to.
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (!schema.tableExists(tableName)) {
      return false;
    }

    Table table = schema.getTable(tableName);
    SchemaHomology homology = new SchemaHomology();
    for (Column column : table.columns()) {
      if (homology.columnsMatch(column, newColumnDefinition)) {
        return true;
      }
    }
    return false;
  }


  /**
   * @return {@link Column} - the definition of the new column.
   */
  public Column getNewColumnDefinition() {
    return newColumnDefinition;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  @Override
  public String toString() {
    return "AddColumn [tableName=" + tableName + ", newColumnDefinition=" + newColumnDefinition + "]";
  }

}
