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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests changing indexes for a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestChangeIndex {

  /** Test data */
  private Table    appleTable;

  /**
   * Setup the test case.
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple").columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable(),
        column("totalValue", DataType.STRING, 10).nullable()
      ).indexes(	    index("Apple_1").unique().columns("pips"),
        index("Apple_2").unique().columns("colour"),
        index("Apple_3").unique().columns("colour", "pips")
      );
  }


  /**
   * Tests that changing index columns works
   */
  @Test
  public void testChangeIndexColumns() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_1").columns("pips"), index("Apple_1").columns("totalValue"));
    Schema updatedSchema = changeIndex.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_2", indexTwo.getName());

    assertEquals("Number of columns should be 1", 1, indexOne.columnNames().size());
    assertEquals("Number of columns should be 1", 1, indexTwo.columnNames().size());

    assertEquals("Column name should have changed", "totalValue", indexOne.columnNames().get(0));
    assertEquals("Column name should not have changed", "colour", indexTwo.columnNames().get(0));

    assertEquals("Uniqueness should have changed", false, indexOne.isUnique());
    assertEquals("Uniqueness should not have changed", true, indexTwo.isUnique());
  }


  /**
   * Tests that increasing columns in an index works
   */
  @Test
  public void testAddingColumnToIndex() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_1").unique().columns("pips"), index("Apple_1").columns("pips", "totalValue"));
    Schema updatedSchema = changeIndex.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);
    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_2", indexTwo.getName());
    assertEquals("Post upgrade existing index name 3", "Apple_3", indexThree.getName());

    assertEquals("Number of columns should be 2", 2, indexOne.columnNames().size());
    assertEquals("Number of columns should be 1", 1, indexTwo.columnNames().size());
    assertEquals("Number of columns should be 2", 2, indexThree.columnNames().size());

    assertEquals("Column 1 name should have changed", "pips", indexOne.columnNames().get(0));
    assertEquals("Column 2 should exist", "totalValue", indexOne.columnNames().get(1));
    assertEquals("Column name should not have changed", "colour", indexTwo.columnNames().get(0));
    assertEquals("Column name should not have changed", "colour", indexThree.columnNames().get(0));
    assertEquals("Column name should not have changed", "pips", indexThree.columnNames().get(1));

    assertEquals("Uniqueness should have changed", false, indexOne.isUnique());
    assertEquals("Uniqueness should not have changed", true, indexTwo.isUnique());
    assertEquals("Uniqueness should not have changed", true, indexThree.isUnique());
  }


  /**
   * Tests that removing columns from an index works
   */
  @Test
  public void testRemovingColumnFromIndex() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_3").unique().columns("colour", "pips"), index("Apple_3").unique().columns("pips"));
    Schema updatedSchema = changeIndex.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);
    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_2", indexTwo.getName());
    assertEquals("Post upgrade existing index name 3", "Apple_3", indexThree.getName());

    assertEquals("Number of columns should be 1", 1, indexOne.columnNames().size());
    assertEquals("Number of columns should be 1", 1, indexTwo.columnNames().size());
    assertEquals("Number of columns should be 1", 1, indexThree.columnNames().size());

    assertEquals("Column name for index 1", "pips", indexOne.columnNames().get(0));
    assertEquals("Column name for index 2", "colour", indexTwo.columnNames().get(0));
    assertEquals("Column name for index 3", "pips", indexThree.columnNames().get(0));

    assertEquals("Uniqueness should have changed", true, indexOne.isUnique());
    assertEquals("Uniqueness should have changed", true, indexTwo.isUnique());
    assertEquals("Uniqueness should not have changed", true, indexThree.isUnique());
  }


  /**
   * Tests that changing the name of an index works
   */
  @Test
  public void testChangeIndexName() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_3").unique().columns("colour", "pips"), index("Apple_4").unique().columns("colour", "pips"));
    Schema updatedSchema = changeIndex.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexOne = resultTable.indexes().get(0);
    Index indexTwo = resultTable.indexes().get(1);
    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index name 1", "Apple_1", indexOne.getName());
    assertEquals("Post upgrade existing index name 2", "Apple_2", indexTwo.getName());
    assertEquals("Post upgrade existing index name 3", "Apple_4", indexThree.getName());

    assertEquals("Number of columns should be 1", 1, indexOne.columnNames().size());
    assertEquals("Number of columns should be 1", 1, indexTwo.columnNames().size());
    assertEquals("Number of columns should be 2", 2, indexThree.columnNames().size());

    assertEquals("Column name for index 1", "pips", indexOne.columnNames().get(0));
    assertEquals("Column name for index 2", "colour", indexTwo.columnNames().get(0));
    assertEquals("Column 1 name for index 3", "colour", indexThree.columnNames().get(0));
    assertEquals("Column 2 name for index 3", "pips", indexThree.columnNames().get(1));

    assertEquals("Uniqueness should have changed", true, indexOne.isUnique());
    assertEquals("Uniqueness should have changed", true, indexTwo.isUnique());
    assertEquals("Uniqueness should not have changed", true, indexThree.isUnique());
  }


  /**
   * Tests that reversing a change index type operation works
   */
  @Test
  public void testReverseChangeColumnType() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_3").unique().columns("colour", "pips"), index("Apple_3").columns("pips"));

    Schema updatedSchema = changeIndex.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 3, resultTable.indexes().size());

    Index indexThree = resultTable.indexes().get(2);

    assertEquals("Post upgrade existing index 3 column count", 2, indexThree.columnNames().size());
    assertEquals("Post upgrade existing index 3 uniqueness", true, indexThree.isUnique());
  }


  /**
   * Tests that changing a non-existant index fails
   */
  @Test
  public void testChangeMissingIndex() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("doesNotExist").unique().columns("colour", "pips"), index("doesNotExist").unique().columns("colour", "pips"));
    try {
      changeIndex.apply(testSchema);
      fail("Should have failed to change a non existant column on apply");
    } catch(Exception e) {
      // Not a problem
    }
  }


  /**
   * Tests that renaming an index to have the same name as an existing index fails
   */
  @Test
  public void testChangeFieldNameToExistingField() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_1").unique().columns("pips"), index("Apple_2").unique().columns("pips"));
    try {
      changeIndex.apply(testSchema);
      fail("Should have failed to change name to match an existing index on apply");
    } catch(Exception e) {
      // Not a problem
    }

  }


  /**
   * Tests that passing a null for the index change results in an exception
   */
  @Test
  public void testNullChangeIndex() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_1").unique().columns("pips"), null);
    try {
      changeIndex.apply(testSchema);
      fail("Attempting to change an index to null should result in an exception on apply");
    } catch(Exception e) {
      // Not a problem
    }

    changeIndex = new ChangeIndex("Apple", null, index("Apple_1").unique().columns("pips"));
    try {
      changeIndex.apply(testSchema);
      fail("Attempting to change an index from null should result in an exception on apply");
    } catch(Exception e) {
      // Not a problem
    }
  }


  /**
   * Tests that a task to change an index appears unapplied when the table isn't
   * present.
   */
  @Test
  public void testChangeIndexOnNonExistentTable() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Sweets", index("Sweets_1").unique().columns("pieces"),
      index("Sweets_1").unique().columns("pieces"));
    changeIndex.isApplied(testSchema, MockConnectionResources.build());
  }


  /**
   * Tests that a task to change an index appears unapplied when the task hasn't
   * been applied.
   */
  @Test
  public void testChangeIndexOnExistingTable() {
    Schema testSchema = schema(appleTable);
    ChangeIndex changeIndex = new ChangeIndex("Apple", index("Apple_1").unique().columns("pips"),
      index("Apple_1").unique().columns("pips"));
    changeIndex.isApplied(testSchema, MockConnectionResources.build());
  }
}
