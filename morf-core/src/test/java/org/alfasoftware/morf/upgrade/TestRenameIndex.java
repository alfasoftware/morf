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
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests renaming indexes for a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestRenameIndex {

  private Table appleTable;


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
      ).indexes(
        index("Apple_1").unique().columns("pips"),
        index("Apple_2").columns("colour"),
        index("Apple_17").unique().columns("colour", "pips")
      );
  }


  /**
   * Tests that renaming a unique index works
   */
  @Test
  public void testRenameUniqueIndex() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_17", "Apple_3");
    Schema updatedSchema = renameIndex.apply(testSchema);

    assertTrue("Rename should be applied", renameIndex.isApplied(updatedSchema, MockConnectionResources.build()));

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);
    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_2", indexTwo.getName());
    assertEquals("Post upgrade existing index name 3", "Apple_3", indexThree.getName());

    assertEquals("Number of columns should still be 1", 1, indexOne.columnNames().size());
    assertEquals("Number of columns should still be 1", 1, indexTwo.columnNames().size());
    assertEquals("Number of columns should still be 2", 2, indexThree.columnNames().size());

    assertEquals("Column name for index 1", "pips", indexOne.columnNames().get(0));
    assertEquals("Column name for index 2", "colour", indexTwo.columnNames().get(0));
    assertEquals("Column 1 name for index 3", "colour", indexThree.columnNames().get(0));
    assertEquals("Column 2 name for index 3", "pips", indexThree.columnNames().get(1));

    assertTrue("Uniqueness should not have changed", indexOne.isUnique());
    assertFalse("Uniqueness should not have changed", indexTwo.isUnique());
    assertTrue("Uniqueness should not have changed", indexThree.isUnique());
  }


  /**
   * Tests that renaming a non-unique index works
   */
  @Test
  public void testRenameNonUniqueIndexName() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_2", "Apple_3");
    Schema updatedSchema = renameIndex.apply(testSchema);

    assertTrue("Rename should be applied", renameIndex.isApplied(updatedSchema, MockConnectionResources.build()));

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);
    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_3", indexTwo.getName());
    assertEquals("Post upgrade existing index name 3", "Apple_17", indexThree.getName());

    assertEquals("Number of columns should still be 1", 1, indexOne.columnNames().size());
    assertEquals("Number of columns should still be 1", 1, indexTwo.columnNames().size());
    assertEquals("Number of columns should still be 2", 2, indexThree.columnNames().size());

    assertEquals("Column name for index 1", "pips", indexOne.columnNames().get(0));
    assertEquals("Column name for index 2", "colour", indexTwo.columnNames().get(0));
    assertEquals("Column 1 name for index 3", "colour", indexThree.columnNames().get(0));
    assertEquals("Column 2 name for index 3", "pips", indexThree.columnNames().get(1));

    assertTrue("Uniqueness should not have changed", indexOne.isUnique());
    assertFalse("Uniqueness should not have changed", indexTwo.isUnique());
    assertTrue("Uniqueness should not have changed", indexThree.isUnique());
  }


  /**
   * Tests that a task to rename an index appears unapplied when the table isn't
   * present.
   */
  @Test
  public void testRenameIndexOnNonExistentTable() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Sweets", "Sweets_1", "Sweets_2");
    assertFalse("Should not be applied", renameIndex.isApplied(testSchema, MockConnectionResources.build()));
  }


  /**
   * Tests that a task to rename an index appears unapplied when the index isn't
   * present.
   */
  @Test
  public void testRenameIndexOnNonExistentIndex() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_3", "Apple_4");
    assertFalse("Should not be applied", renameIndex.isApplied(testSchema, MockConnectionResources.build()));
  }


  /**
   * Tests that a task to rename an index throws an
   * {@link IllegalArgumentException} when the from index name isn't provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameIndexNoFromName() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", " ", "Apple_3");
    renameIndex.apply(testSchema);
  }


  /**
   * Tests that a task to rename an index throws an
   * {@link IllegalArgumentException} when the to index name isn't provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameIndexNoToName() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_1", " ");
    renameIndex.apply(testSchema);
  }


  /**
   * Tests that a task to rename an index throws an
   * {@link IllegalArgumentException} when the from index doesn't exist.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameIndexNotExists() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_3", "Apple_4");
    renameIndex.apply(testSchema);
  }


  /**
   * Tests that a task to rename an index throws an
   * {@link IllegalArgumentException} when the end index name already exists.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRenameIndexAlreadyExists() {
    Schema testSchema = schema(appleTable);
    RenameIndex renameIndex = new RenameIndex("Apple", "Apple_1", "Apple_2");
    renameIndex.apply(testSchema);
  }
}
