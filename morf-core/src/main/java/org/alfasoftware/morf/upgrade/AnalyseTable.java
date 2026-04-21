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
import org.alfasoftware.morf.sql.Statement;

/**
  * {@link SchemaChange} which consists of executing SQL statement followed by
 * some database schema change
 *
 * <p><strong>This class is a very tenuous implementation of {@link SchemaChange}
 * since it does not strictly represent a change to the schema, really it is analysing the
 * data in the schema, collecting and managing statistics for a Table in the schema.
 * It always returns true for {@link #isApplied(Schema, ConnectionResources)}
 * so that the upgrade tooling does not constantly attempt to rerun any upgrade
 * that contains a data upgrade step.</strong></p>
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class AnalyseTable implements SchemaChange {


  private final String tableName;


  /**
   * @param tableName The table to analyse.
   */
  public AnalyseTable(String tableName) {
    super();
    this.tableName = tableName;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * Before an upgrade step is run, if the table for analysis is not present, an illegal argument exception is thrown
   * to prevent the upgrade step from starting.
   */
  @Override
  public Schema apply(Schema schema) {

    if (!schema.tableExists(tableName.toUpperCase())) {
      throw new IllegalArgumentException("Cannot analyse table [" + tableName + "] as it does not exist.");
    }
    return schema;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    return schema;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    return true;
  }


  /**
   * Method to get the table that is being analysed.
   *
   * @return {@link Statement} to be executed
   */
  public String getTableName() {
    return tableName;
  }


  @Override
  public String toString() {
    return "AnalyseTable [tableName=" + tableName + "]";
  }

}

