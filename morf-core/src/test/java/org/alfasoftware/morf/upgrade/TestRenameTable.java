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
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests renaming a table in a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestRenameTable {

  private Table appleTable;
  private Table orangeTable;


  /**
   * Setup the test case.
   */
  @Before
  public void setUp() {
    appleTable = table("Apple")
      .columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable(),
        column("totalValue", DataType.STRING, 10).nullable()
      );

    orangeTable = table("Orange")
        .columns(
          column("segments", DataType.STRING, 10).nullable(),
          column("colour", DataType.STRING, 10).nullable(),
          column("totalValue", DataType.STRING, 10).nullable()
        );
  }


  /**
   * Tests that renaming a table works
   */
  @Test
  public void testRenameTable() {
    Schema testSchema = schema(appleTable);
    RenameTable renameTable = new RenameTable("Apple", "Pear");
    Schema updatedSchema = renameTable.apply(testSchema);

    assertTrue("Rename should be applied", renameTable.isApplied(updatedSchema, MockConnectionResources.build()));

    Table resultTable = updatedSchema.getTable("Pear");
    assertNotNull(resultTable);
  }


  /**
   * Tests that renaming a table works
   */
  @Test
  public void testRenameTableCaseInsensitive() {
    Schema testSchema = schema(appleTable);
    RenameTable renameTable = new RenameTable("APPLE", "Pear");
    Schema updatedSchema = renameTable.apply(testSchema);

    assertTrue("Rename should be applied", renameTable.isApplied(updatedSchema, MockConnectionResources.build()));

    Table resultTable = updatedSchema.getTable("Pear");
    assertNotNull(resultTable);
  }


  /**
   * Tests that a task to rename an table throws an
   * {@link IllegalArgumentException} when the from table doesn't exist.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameTableNotExists() {
    Schema testSchema = schema(appleTable);
    RenameTable renameTable = new RenameTable("Pear", "Apple");
    renameTable.apply(testSchema);
  }


  /**
   * Tests that a task to rename an table throws an
   * {@link IllegalArgumentException} when the new table name already exists.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameTableAlreadyExists() {
    Schema testSchema = schema(appleTable, orangeTable);
    RenameTable renameTable = new RenameTable("Apple", "Orange");
    renameTable.apply(testSchema);
  }
}
