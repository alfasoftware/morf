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
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests removing indexes from a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestRemoveIndex {

  /** Test data */
  private Table appleTable;

  /** Test data */
  private RemoveIndex removeIndex;


  /**
   * Setup the test case.
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple").columns(
        column("pips", DataType.STRING, 10).nullable(),
        column("colour", DataType.STRING, 10).nullable()
      ).indexes(        index("Apple_1").unique().columns("pips")
      );
    removeIndex = new RemoveIndex("Apple", index("Apple_1").unique().columns("pips"));
  }


  /**
   * Tests that an index can be removed from a table.
   */
  @Test
  public void testRemovingIndex() {
    Schema testSchema = schema(appleTable);
    assertEquals("Pre upgrade index count", 1, testSchema.getTable("Apple").indexes().size());

    Schema updatedSchema = removeIndex.apply(testSchema);
    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 0, resultTable.indexes().size());
  }


  /**
   * Tests that a index removal can be reversed.
   */
  @Test
  public void testReverseRemovingIndex() {
    // Remove the Apple_1 index so it is there to reverse
    appleTable.indexes().remove(0);
    Schema testSchema = schema(appleTable);
    Schema downGradedSchema = removeIndex.reverse(testSchema);

    Table resultTable = downGradedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade index count", 1, resultTable.indexes().size());
  }


  /**
   * Tests that attempting to removing an index fails if the index is not present.
   */
  @Test
  public void testRemovingNonExistantIndex() {
    Schema testSchema = schema(appleTable);
    try {
      removeIndex = new RemoveIndex("Apple", index("foo").unique().columns("bar"));
      removeIndex.apply(testSchema);
      fail("Should fail since column is not there");
    } catch (Exception e) {
      // Expected
    }
  }


  /**
   * Tests that a task to remove an index appears unapplied when the table isn't
   * present.
   */
  @Test
  public void testRemovingIndexFromNonExistentTable() {
    Schema testSchema = schema(appleTable);
    removeIndex = new RemoveIndex("Sweets", index("Sweets_1").unique().columns("pieces"));
    removeIndex.isApplied(testSchema, MockConnectionResources.build());
  }


  /**
   * Tests that a task to remove an index appears unapplied when the task hasn't
   * been applied.
   */
  @Test
  public void testRemovingIndexFromExistingTable() {
    Schema testSchema = schema(appleTable);
    removeIndex.isApplied(testSchema, MockConnectionResources.build());
  }

}
