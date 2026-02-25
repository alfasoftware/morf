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

import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;

/**
 * A {@link SchemaChange} which consists of the addition and insert data (from a
 * {@link SelectStatement}) into a new table to a database schema.
 *
 * <p>Note the table being added must have no indexes when it is added here. They can be added subsequently</p>
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class AddTableFrom extends AddTable implements SchemaChange {

  /** {@link SelectStatement} to get the date to populate the new table */
  private final SelectStatement selectStatement;


  /**
   * Construct an {@link AddTableFrom} schema change for applying against a schema.
   *
   * @param newTable table to add to metadata.
   * @param selectStatement the {@link SelectStatement} to get the data for the new table.
   */
  public AddTableFrom(Table newTable, SelectStatement selectStatement) {
    super(newTable);
    this.selectStatement = selectStatement;

    if (!newTable.indexes().isEmpty()) {
      throw new IllegalArgumentException("Tables being added using AddTableFrom must not have any indexes at the point of addition. Table: [" + newTable.getName() + "]");
    }
  }


  /**
   * @return the {@link SelectStatement} to be used.
   */
  public SelectStatement getSelectStatement() {
    return selectStatement;
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
    return "AddTableFrom [table=" + getTable() + ", selectStatement=" + selectStatement + "]";
  }
}
