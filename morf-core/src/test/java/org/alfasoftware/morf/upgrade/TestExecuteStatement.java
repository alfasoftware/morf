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
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Tests SQL statement execution to a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestExecuteStatement {

  /** Test data */
  private Table appleTable;

  /**
   * Statement to be executes
   */
  private ExecuteStatement executeStmt;


  /**
   * Setup the test.
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple")
      .columns(
        idColumn(),
        versionColumn(),
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      );

    SelectStatement select = new SelectStatement(new FieldLiteral('a')).from(new TableReference("Apple"));
    InsertStatement stmt = new InsertStatement().into(new TableReference("Apple")).fields(new FieldReference("pips")).from(select);

    executeStmt = new ExecuteStatement(stmt);
  }


  /**
   * Tests that apply doesn't change the schema.
   */
  @Test
  public void testApplyDoesNotChangeSchema() {
    Schema testSchema = schema(appleTable);
    Schema updatedSchema = executeStmt.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 4, resultTable.columns().size());

    // verify that both schemas are the same
    assertEquals("Post upgrade column count", testSchema, updatedSchema);
  }


  /**
   * Tests that reverse doesn't change the schema.
   */
  @Test
  public void testReverseDoesNotChangeSchema() {
    Schema testSchema = schema(appleTable);
    Schema updatedSchema = executeStmt.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 4, resultTable.columns().size());

    // verify that both schemas are the same
    assertEquals("Post upgrade column count", testSchema, updatedSchema);
  }
}
