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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.metadata.SchemaUtils.toUpperCase;

import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * {@link SchemaChange} which consists of changing the primary key columns
 * within a schema
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class ChangePrimaryKeyColumns implements SchemaChange {

  /**
   * Table name in which the primary key columns change is applicable to.
   */
  private final String tableName;

  /**
   * List of column names representing the old column names to change from.
   */
  private final List<String> oldPrimaryKeyColumns;

  /**
   * List of column names representing the new column names to change to.
   */
  private final List<String> newPrimaryKeyColumns;


  /**
   * @param tableName - to change primary key columns on
   * @param oldPrimaryKeyColumns - to change from
   * @param newPrimaryKeyColumns - to change to
   */
  public ChangePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    this.tableName = tableName;
    this.oldPrimaryKeyColumns = oldPrimaryKeyColumns;
    this.newPrimaryKeyColumns = newPrimaryKeyColumns;
  }


  /**
   * Applies the change
   * @param schema The target schema
   * @param from the old primary key
   * @param to the new primary key
   * @return The resulting schema
   */
  protected Schema applyChange(Schema schema, List<String> from, List<String> to) {

    // Construct a list of column names converted to all upper case - this is to satisfy certain databases (H2).
    List<String> newPrimaryKeyColumnsUpperCase = toUpperCase(to);

    // Does the table exist?
    if (!schema.tableExists(tableName)) {
      throw new RuntimeException("The table [" + tableName + "] does not exist.");
    }

    // Prepare a map of the columns for later re-ordering
    Table table = schema.getTable(tableName);
    ImmutableMap<String, Column> columnsMap = Maps.uniqueIndex(table.columns(), new Function<Column, String>() {
      @Override
      public String apply(Column input) {
        return input.getName().toUpperCase();
      }
    });

    assertExistingPrimaryKey(from, table);

    verifyNewPrimaryKeyIsNotIndexed(table, to);

    // Do the "to" primary key columns exist?
    List<String> allColumns = toUpperCase(namesOfColumns(table.columns()));

    // Build up the columns in the correct order
    List<Column> newColumns = new ArrayList<>();

    // Remove primaries from the full list so the non-primaries can be added afterwards
    List<Column> nonPrimaries = Lists.newArrayList(table.columns());

    for (String newPrimaryColumn : newPrimaryKeyColumnsUpperCase) {
      if (allColumns.contains(newPrimaryColumn.toUpperCase())) {
        Column pk = columnsMap.get(newPrimaryColumn);
        newColumns.add(column(pk).primaryKey());
        nonPrimaries.remove(pk);
      } else {
        throw new RuntimeException("The column [" + newPrimaryColumn + "] does not exist on [" + table.getName() + "]");
      }
    }

    // Add in the rest
    for(Column nonpk : nonPrimaries) {
      newColumns.add(column(nonpk).notPrimaryKey());
    }

    return new TableOverrideSchema(schema, new AlteredTable(table, SchemaUtils.namesOfColumns(newColumns), newColumns));
  }


  /**
   * Verify that the "from" position actually matches the schema we have been given
   * @param from The from position
   * @param table The target table
   */
  protected void assertExistingPrimaryKey(List<String> from, Table table) {
    List<String> fromUpperCase = toUpperCase(from);
    List<String> existingUpperCase = toUpperCase(namesOfColumns(primaryKeysForTable(table)));
    if (!fromUpperCase.equals(existingUpperCase)) {
      throw new RuntimeException(String.format("Expected existing primary key columns do not match schema. Expected: %s, schema: %S", fromUpperCase, existingUpperCase));
    }
  }


  /**
   * Verify that the proposed PK does not already exist as an index. Permitting this confuses Oracle.
   */
  private void verifyNewPrimaryKeyIsNotIndexed(Table table, List<String> newPrimaryKeyColumns) {
    List<String> newPrimaryKeyColumnsUpperCase = toUpperCase(newPrimaryKeyColumns);

    for (Index index : table.indexes()) {
      List<String> indexColumnNames = toUpperCase(index.columnNames());

      if (indexColumnNames.equals(newPrimaryKeyColumnsUpperCase)) {
        throw new IllegalArgumentException(
                    "Attempting to change primary key of table [" + table.getName() +
                    "] to " + newPrimaryKeyColumns +
                    " but this combination of columns exists in index [" + index.getName() + "]");
      }
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    return applyChange(schema, oldPrimaryKeyColumns, newPrimaryKeyColumns);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    return applyChange(schema, newPrimaryKeyColumns, oldPrimaryKeyColumns);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(org.alfasoftware.morf.metadata.Schema, org.alfasoftware.morf.jdbc.ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    List<String> actual = namesOfColumns(primaryKeysForTable(schema.getTable(tableName)));
    List<String> expected = newPrimaryKeyColumns;

    return actual.equals(expected);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @return the old primary key column names
   */
  public List<String> getOldPrimaryKeyColumns() {
    return oldPrimaryKeyColumns;
  }


  /**
   * @return the new primary key column names
   */
  public List<String> getNewPrimaryKeyColumns() {
    return newPrimaryKeyColumns;
  }
}
