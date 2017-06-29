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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests that {@link AddColumn} works.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestAddColumn {

  /** Test data */
  private Table appleTable;

  /** Test data */
  private AddColumn addColumn;

  /**
   * Setup the test case
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple")
      .columns(
        idColumn(),
        versionColumn(),
        column("pips", DataType.STRING, 10).nullable()
      );
    Column colourColumn = column("colour", DataType.STRING).nullable();
    addColumn = new AddColumn("Apple", colourColumn);
  }

  /**
   * Tests that a column can be added to a table.
   */
  @Test
  public void testAddingColumn() {
    Schema testSchema = schema(appleTable);
    Schema updatedSchema = addColumn.apply(testSchema);
    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 4, resultTable.columns().size());
    assertEquals("Post upgrade existing column name", "pips", resultTable.columns().get(2).getName());
    assertEquals("Post upgrade new column name", "colour", resultTable.columns().get(3).getName());
  }

  /**
   * Tests that a column addition can be reversed.
   */
  @Test
  public void testReverseAddingColumn() {
    // Add the colour column so it is there to reverse
    appleTable = table("Apple")
      .columns(
        idColumn(),
        versionColumn(),
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      );
    Schema testSchema = schema(appleTable);
    Schema downGradedSchema = addColumn.reverse(testSchema);

    Table resultTable = downGradedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 3, resultTable.columns().size());
    assertEquals("Post upgrade existing column name", "pips", resultTable.columns().get(2).getName());
  }

  /**
   * Tests that attempting to reverse an add column fails if the column is not present.
   */
  @Test
  public void testRemovingNonExistantColumn() {
    Schema testSchema = schema(appleTable);
    try {
      addColumn.reverse(testSchema);
      fail("Should fail since column is not there");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testAddingPrimaryKeyColumnIsPrevented() {
    try {
      new AddColumn("Apple", column("foo", DataType.STRING, 10).primaryKey());
      fail("Should fail since column is PK");
    } catch (IllegalArgumentException e) {
      assertTrue(e.toString().contains("primary"));
    }
  }

  @Test
  public void testAddingNullableColumnWithoutDefaultIsPrevented() {
    try {
      new AddColumn("Apple", column("foo", DataType.STRING, 10));
      fail("Should fail since column is nullable and has no default");
    } catch (IllegalArgumentException e) {
      assertTrue(e.toString().contains("nullable"));
    }
  }
}
