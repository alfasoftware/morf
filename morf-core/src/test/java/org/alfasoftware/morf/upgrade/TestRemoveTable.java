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
 * Tests removing tables from a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestRemoveTable {
  /** Test data */
  private Table appleTable;

  /** Test data */
  private Table mangoTable;

  /** Test data */
  private RemoveTable removeTable;

  /**
   * Setup the test case.
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
    mangoTable = table("Mango")
      .columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 10).nullable(),
        column("totalValue", DataType.STRING, 10).nullable()
      );
    removeTable = new RemoveTable(appleTable);
  }

  /**
   * Tests that a table can be removed from a schema.
   */
  @Test
  public void testRemovingTable() {
    Schema testSchema = schema(appleTable, mangoTable);
    Schema updatedSchema = removeTable.apply(testSchema);

    assertEquals("Post upgrade table count", 1, updatedSchema.tables().size());
    assertFalse(updatedSchema.tableExists("Apple"));
    assertTrue(updatedSchema.tableExists("Mango"));

    Table resultTable = updatedSchema.getTable("Mango");

    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 4, resultTable.columns().size());
  }


  /**
   * Tests that a column addition can be reversed.
   */
  @Test
  public void testReverseRemovingTable() {
    Schema testSchema = schema(appleTable, mangoTable);
    testSchema.tables().remove(appleTable);
    Schema updatedSchema = removeTable.reverse(testSchema);

    assertEquals("Post upgrade table count", 2, updatedSchema.tables().size());
    assertTrue(updatedSchema.tableExists("Apple"));
    assertTrue(updatedSchema.tableExists("Mango"));
  }


  /**
   * Tests that attempting to removing a table fails if the table is not present.
   */
  @Test
  public void testRemovingNonExistantIndex() {
    Schema testSchema = schema(appleTable);
    try {
      Table table = table("Orange").columns(
          column("colour", DataType.STRING, 10).nullable(),
          column("totalValue", DataType.STRING, 10).nullable()
        );
      removeTable = new RemoveTable(table);
      removeTable.apply(testSchema);
      fail("Should fail since column is not there");
    } catch (Exception e) {
      // Expected
    }
  }

}
