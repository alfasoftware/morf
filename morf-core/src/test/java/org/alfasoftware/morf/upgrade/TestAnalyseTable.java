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

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests renaming indexes for a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestAnalyseTable {


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


  @Test
  public void testApplyDoesNotChangeSchema() {
    Schema testSchema = schema(appleTable);
    AnalyseTable analyseTable = new AnalyseTable("Apple");
    Schema updatedSchema = analyseTable.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 3, resultTable.columns().size());

    // verify that both schemas are the same
    assertEquals("Post upgrade column count", testSchema, updatedSchema);
  }


  /**
   * Tests that a task to rename an index throws an
   * {@link IllegalArgumentException} when the from index doesn't exist.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAnalyseNonExistentTable() {
    Schema testSchema = schema(appleTable);
    AnalyseTable analyseTable = new AnalyseTable("Pear");
    analyseTable.apply(testSchema);
  }


}

