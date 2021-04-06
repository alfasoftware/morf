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

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AugmentedSchema;
import org.alfasoftware.morf.upgrade.adapt.FilteredSchema;

/**
 * A {@link SchemaChange} which consists of removing a table from
 * a database schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class RemoveTable implements SchemaChange {

  /** The {@link Table} to remove from the schema. */
  private final Table tableToBeRemoved;


  /**
   * Construct an {@link RemoveTable} schema change for applying against a schema.
   *
   * @param tableToBeRemoved the table to be removed from schema
   */
  public RemoveTable(Table tableToBeRemoved) {
    this.tableToBeRemoved = tableToBeRemoved;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    if(!schema.tableExists(tableToBeRemoved.getName().toUpperCase())){
      throw new IllegalArgumentException("Cannot remove table [" + tableToBeRemoved.getName() + "] as it does not exist.");
    }

    return new FilteredSchema(schema, tableToBeRemoved.getName());
  };


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    for (String tableName : schema.tableNames()) {
      if (tableName.equalsIgnoreCase(tableToBeRemoved.getName())) {
        return false;
      }
    }

    return true;

  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    if(schema.tableExists(tableToBeRemoved.getName().toUpperCase())){
      throw new IllegalArgumentException("Cannot perform reversal for [" + tableToBeRemoved.getName() + "] table removal as it already exists.");
    }
    return new AugmentedSchema(schema, tableToBeRemoved);
  }


  /**
   * @return the {@link Table} to be removed.
   */
  public Table getTable() {
    return tableToBeRemoved;
  }

}
