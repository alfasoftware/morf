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
import org.alfasoftware.morf.metadata.SchemaValidator;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AugmentedSchema;
import org.alfasoftware.morf.upgrade.adapt.FilteredSchema;

/**
 * A {@link SchemaChange} which consists of the addition a new table to
 * a database schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class AddTable implements SchemaChange {

  /** {@link Table} to add to the schema. */
  private final Table newTable;


  /**
   * Construct an {@link AddTable} schema change for applying against a schema.
   *
   * @param newTable table to add to metadata.
   */
  public AddTable(Table newTable) {
    this.newTable = newTable;

    new SchemaValidator().validate(newTable);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   * @return {@link Schema} with new table removed.
   */
  @Override
  public Schema reverse(Schema schema) {
    return new FilteredSchema(schema, newTable.getName());
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   * @return {@link Schema} with new table added.
   */
  @Override
  public Schema apply(Schema schema) {
    return new AugmentedSchema(schema, newTable);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    for (String tableName : schema.tableNames()) {
      if (tableName.equalsIgnoreCase(newTable.getName())) {
        return true;
      }
    }

    return false;
  }


  /**
   * @return the {@link Table} to be added.
   */
  public Table getTable() {
    return newTable;
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
    return "AddTable [newTable=" + newTable + "]";
  }
}
