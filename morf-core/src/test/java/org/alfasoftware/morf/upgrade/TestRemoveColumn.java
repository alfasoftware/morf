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
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests removing columns from a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestRemoveColumn {

  /** Test data */
  private Table appleTable;

  /** Test data */
  private RemoveColumn removeColumn;


  /**
   * Setup the test.
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple").columns(
        idColumn(),
        versionColumn(),
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      );
    removeColumn = new RemoveColumn("Apple", column("colour", DataType.STRING).nullable());
  }


  /**
   * Tests that a column can be removed from a table.
   */
  @Test
  public void testRemovingColumn() {
    Schema testSchema = schema(appleTable);
    Schema updatedSchema = removeColumn.apply(testSchema);
    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 3, resultTable.columns().size());
    assertEquals("Post upgrade existing column name", "pips", resultTable.columns().get(2).getName());
  }


  /**
   * Tests that a primary key column can be removed from a table.
   */
  @Test
  public void testRemovePrimaryKey() {
    Schema testSchema = schema(appleTable);
    RemoveColumn removeIdColumn = new RemoveColumn("Apple", column("id", DataType.BIG_INTEGER, 19, 0).primaryKey());
    Schema updatedSchema = removeIdColumn.apply(testSchema);
    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 3, resultTable.columns().size());
  }


  /**
   * Tests that a column removal can be reversed.
   */
  @Test
  public void testReverseAddingColumn() {
    // Remove the colour column so it is there to reverse
    appleTable.columns().remove(3);
    Schema testSchema = schema(appleTable);
    Schema downGradedSchema = removeColumn.reverse(testSchema);

    Table resultTable = downGradedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 4, resultTable.columns().size());
    assertEquals("Post upgrade existing column name", "pips", resultTable.columns().get(2).getName());
    assertEquals("Post upgrade existing column name", "colour", resultTable.columns().get(3).getName());
  }


  /**
   * Tests that attempting to removing a column fails if the column is not present.
   */
  @Test
  public void testRemovingNonExistantColumn() {
    Schema testSchema = schema(appleTable);
    try {
      removeColumn = new RemoveColumn("Apple", column("foo", DataType.STRING).nullable());
      removeColumn.apply(testSchema);
      fail("Should fail since column is not there");
    } catch (Exception e) {
      // Expected
    }
  }
}
