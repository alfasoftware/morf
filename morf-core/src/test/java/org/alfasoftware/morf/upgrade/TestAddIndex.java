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
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests adding indexes to a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestAddIndex {

  /** Test data */
  private Table appleTable;

  /** Test data */
  private AddIndex addIndex;

  /**
   * Setup the test
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple").columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      );

    addIndex = new AddIndex("Apple", index("Apple_1").unique().columns("pips"));
  }


  /**
   * Tests that an index can be added to a table.
   */
  @Test
  public void testAddingIndex() {
    Schema testSchema = schema(appleTable);
    Schema updatedSchema = addIndex.apply(testSchema);
    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 1, resultTable.indexes().size());
    assertEquals("Post upgrade index name", "Apple_1", resultTable.indexes().get(0).getName());
    assertEquals("Post upgrade index column count", 1, resultTable.indexes().get(0).columnNames().size());
    assertEquals("Post upgrade index column name", "pips", resultTable.indexes().get(0).columnNames().get(0));
  }


  /**
   * Tests that an index addition can be reversed.
   */
  @Test
  public void testReverseAddingIndex() {
    // Add an index so it is there to reverse
    appleTable = table("Apple").columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      ).indexes(        index("Apple_1").unique().columns("pips")
      );

    Schema testSchema = schema(appleTable);
    Schema downGradedSchema = addIndex.reverse(testSchema);

    Table resultTable = downGradedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 0, resultTable.indexes().size());
  }


  /**
   * Tests that attempting to reverse an add index fails if the index is not present.
   */
  @Test
  public void testRemovingNonExistantIndex() {
    Schema testSchema = schema(appleTable);
    try {
      addIndex.reverse(testSchema);
      fail("Should fail since index is not there");
    } catch (Exception e) {
      // Expected
    }
  }


  /**
   * Tests that a task to add an index appears unapplied when the table isn't
   * present.
   */
  @Test
  public void testAddIndexToNonExistentTable() {
    Schema testSchema = schema(appleTable);
    addIndex = new AddIndex("Sweets", index("Sweets_1").unique().columns("pieces"));
    assertFalse("Index should not have been applied", addIndex.isApplied(testSchema, MockConnectionResources.build()));
  }


  /**
   * Tests that a task to add an index appears unapplied when the task hasn't been applied.
   */
  @Test
  public void testAddIndexToExistingTable() {
    Schema testSchema = schema(appleTable);
    assertFalse("Index should not have been applied", addIndex.isApplied(testSchema, MockConnectionResources.build()));
  }


  /**
   * Test that adding an index of the same name to an existing one fails.
   */
  @Test
  public void testAddIndexOfSameName() {
    appleTable = table("Apple").columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      ).indexes(        index("Apple_1").unique().columns("pips")
      );

    addIndex = new AddIndex("Apple", index("Apple_1").unique().columns("pips"));
    Schema testSchema = schema(appleTable);

    try {
      addIndex.apply(testSchema);
      fail();
    } catch (RuntimeException re) {
      assertTrue(re.getMessage().contains("Apple_1"));
    }
  }


  /**
   * Test that adding an index to a table that's not in the schema gives a sensible error
   */
  @Test
  public void testMissingTable() {
    addIndex = new AddIndex("AppleX", index("Apple_1").unique().columns("pips"));
    Schema testSchema = schema(appleTable);

    try {
      addIndex.apply(testSchema);
      fail();
    } catch (RuntimeException re) {
      assertTrue(re.getMessage(), re.getMessage().contains("Apple"));
    }
  }
}
