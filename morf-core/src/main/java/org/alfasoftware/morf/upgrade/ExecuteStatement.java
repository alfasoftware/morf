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
 * since it does not strictly represent a change to the schema, really it is a change
 * to the data in the schema. It always returns true for {@link #isApplied(Schema, ConnectionResources)}
 * so that the upgrade tooling does not constantly attempt to rerun any upgrade
 * that contains a data upgrade step.</strong></p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class ExecuteStatement implements SchemaChange {

  /**
   * The {@link Statement} to execute
   */
  private final Statement statement;


  /**
   *
   * @param statement the {@link Statement to execute}
   */
  public ExecuteStatement(Statement statement) {
    this.statement = statement;
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
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    return schema;
  }


  /**
   * Method to get the statement that is being executed
   *
   * @return {@link Statement} to be executed
   */
  public Statement getStatement() {
    return statement;
  }


  @Override
  public String toString() {
    return "ExecuteStatement [statement=" + statement + "]";
  }
}
