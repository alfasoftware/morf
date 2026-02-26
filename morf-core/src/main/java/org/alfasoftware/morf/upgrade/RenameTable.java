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

import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;

import com.google.common.collect.Maps;

/**
 * {@link SchemaChange} which consists of changing the table name.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class RenameTable implements SchemaChange {


  /**
   * Existing table name
   */
  private final String oldTableName;

  /**
   * New table name to change to
   */
  private final String newTableName;

  /**
   * @param oldTableName - existing table name
   * @param newTableName - new table name to change to
   */
  public RenameTable(String oldTableName, String newTableName) {
    this.oldTableName = oldTableName;
    this.newTableName = newTableName;
  }


  /**
   * @return the existing table name to change from.
   */
  public String getOldTableName() {
    return oldTableName;
  }


  /**
   * @return the new table name to change to.
   */
  public String getNewTableName() {
    return newTableName;
  }


  /**
   * Applies the renaming.
   */
  private Schema applyChange(Schema schema, String from, String to) {

    if (!schema.tableExists(from)) {
      throw new IllegalArgumentException("Cannot rename table [" + from + "]. It does not exist.");
    }

    if (schema.tableExists(to)) {
      throw new IllegalArgumentException("Cannot rename table [" + from + "]. The new table name [" + to + "] already exists.");
    }

    Map<String, Table> tableMap = Maps.newHashMap();

    for (Table table : schema.tables()) {
      if (table.getName().equalsIgnoreCase(from)) {
        // If this is the table being renamed, add the new renamed table instead
        tableMap.put(to, new RenamedTable(to, schema.getTable(from)));
      } else {
        tableMap.put(table.getName(), table);
      }
    }

    return SchemaUtils.schema(tableMap.values());
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    return applyChange(schema, oldTableName, newTableName);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    return applyChange(schema, newTableName, oldTableName);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(org.alfasoftware.morf.metadata.Schema, org.alfasoftware.morf.jdbc.ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    return schema.tableExists(newTableName);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * A renamed table
   *
   * @author Copyright (c) Alfa Financial Software 2013
   */
  private static class RenamedTable implements Table {
    private final String newName;
    private final Table baseTable;

    /**
     * @param newName The new name for a {@link Table}
     * @param baseTable The {@link Table} to be renamed
     */
    RenamedTable(String newName, Table baseTable) {
      super();
      this.newName = newName;
      this.baseTable = baseTable;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#getName()
     */
    @Override
    public String getName() {
      return newName;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#columns()
     */
    @Override
    public List<Column> columns() {
      return baseTable.columns();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#indexes()
     */
    @Override
    public List<Index> indexes() {
      return baseTable.indexes();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#isTemporary()
     */
    @Override
    public boolean isTemporary() {
      return baseTable.isTemporary();
    }
  }


  @Override
  public String toString() {
    return "RenameTable [oldTableName=" + oldTableName + ", newTableName=" + newTableName + "]";
  }
}