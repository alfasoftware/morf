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

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.ThrowingDifferenceWriter;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Before;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that {@link ChangeColumn} works
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestChangeColumn {

  /** Test data */
  private Table    appleTable;
  private Table    pearTable;

  /**
   * Sets up the test
   */
  @Before
  public void setUp() throws Exception {
    appleTable = table("Apple").columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 10).nullable(),
        column("variety", DataType.STRING, 15).nullable(),
        column("ispoisoned", DataType.BOOLEAN).nullable(),
        column("datecreated", DataType.DATE).nullable(),
        column("numberavailable", DataType.DECIMAL, 5, 0),
        column("totalvalue", DataType.DECIMAL, 9, 2),
        column("nullcheck", DataType.DECIMAL, 9, 0).nullable()
      ).indexes(
        index("Apple_1").columns("colour")
      );

    pearTable = table("Pear").columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 10).nullable(),
        column("variety", DataType.STRING, 15).nullable()
      ).indexes(
        index("Pear_1").columns("colour")
      );
  }


  /**
   * Tests that increasing the length of a column works
   */
  @Test
  public void testIncreaseColumnLength() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("colour", DataType.STRING, 10).nullable(), column("colour", DataType.STRING, 35).nullable());
    Schema updatedSchema = changeColumn.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnOne = resultTable.columns().get(0);
    Column columnTwo = resultTable.columns().get(1);
    Column columnThree = resultTable.columns().get(2);
    Column columnFour = resultTable.columns().get(3);
    Column columnFive = resultTable.columns().get(4);
    Column columnSix = resultTable.columns().get(5);
    Column columnSeven = resultTable.columns().get(6);
    Column columnEight = resultTable.columns().get(7);
    Column columnNine = resultTable.columns().get(8);

    assertEquals("Post upgrade existing column name 1", "id", columnOne.getName());
    assertEquals("Post upgrade existing column name 2", "version", columnTwo.getName());
    assertEquals("Post upgrade existing column name 3", "colour", columnThree.getName());
    assertEquals("Post upgrade existing column name 4", "variety", columnFour.getName());
    assertEquals("Post upgrade existing column name 5", "ispoisoned", columnFive.getName());
    assertEquals("Post upgrade existing column name 6", "datecreated", columnSix.getName());
    assertEquals("Post upgrade existing column name 7", "numberavailable", columnSeven.getName());
    assertEquals("Post upgrade existing column name 8", "totalvalue", columnEight.getName());
    assertEquals("Post upgrade existing column name 9", "nullcheck", columnNine.getName());

    assertEquals("Width should have changed", 35, columnThree.getWidth());
    assertEquals("Width should not have changed", 15, columnFour.getWidth());
    assertEquals("Width should not have changed", 5, columnSeven.getWidth());
    assertEquals("Width should not have changed", 9, columnEight.getWidth());
    assertEquals("Width should not have changed", 9, columnNine.getWidth());

    assertEquals("Scale should not have changed", 0, columnThree.getScale());
    assertEquals("Scale should not have changed", 0, columnFour.getScale());
    assertEquals("Scale should not have changed", 0, columnFive.getScale());
    assertEquals("Scale should not have changed", 0, columnSeven.getScale());
    assertEquals("Scale should not have changed", 2, columnEight.getScale());
    assertEquals("Scale should not have changed", 0, columnNine.getScale());

    assertEquals("Type should not have changed", DataType.STRING, columnThree.getType());
    assertEquals("Type should not have changed", DataType.STRING, columnFour.getType());
    assertEquals("Type should not have changed", DataType.BOOLEAN, columnFive.getType());
    assertEquals("Type should not have changed", DataType.DATE, columnSix.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSeven.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnEight.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnNine.getType());

    assertTrue("Nullable should not have changed", columnThree.isNullable());
    assertTrue("Nullable should not have changed", columnFour.isNullable());
    assertTrue("Nullable should not have changed", columnFive.isNullable());
    assertTrue("Nullable should not have changed", columnSix.isNullable());
    assertFalse("Nullable should not have changed", columnSeven.isNullable());
    assertFalse("Nullable should not have changed", columnEight.isNullable());
    assertTrue("Nullable should not have changed", columnNine.isNullable());
  }


  /**
   * Tests that changing the type of a column works
   */
  @Test
  public void testChangeColumnType() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("numberavailable", DataType.DECIMAL, 5), column("numberavailable", DataType.BIG_INTEGER));
    Schema updatedSchema = changeColumn.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);

    Table expectedAppleTable = table("Apple").columns(
      idColumn(),
      versionColumn(),
      column("colour", DataType.STRING, 10).nullable(),
      column("variety", DataType.STRING, 15).nullable(),
      column("ispoisoned", DataType.BOOLEAN).nullable(),
      column("datecreated", DataType.DATE).nullable(),
      column("numberavailable", DataType.BIG_INTEGER),
      column("totalvalue", DataType.DECIMAL, 9, 2),
      column("nullcheck", DataType.DECIMAL, 9, 0).nullable()
    ).indexes(
      index("Apple_1").columns("colour")
    );

    assertTrue(new SchemaHomology(new ThrowingDifferenceWriter(), "expected", "result").tablesMatch(expectedAppleTable, resultTable));
  }


  /**
   * Tests that changing the width and scale of a column is checked appropriately
   */
  @Test
  public void testChangeWidthAndScale() {
    new ChangeColumn("Apple", column("someDecimal", DataType.DECIMAL, 5, 3), column("someDecimal", DataType.DECIMAL, 6, 3));
    new ChangeColumn("Apple", column("someDecimal", DataType.DECIMAL, 5, 3), column("someDecimal", DataType.DECIMAL, 6, 4));

    try {
      new ChangeColumn("Apple", column("someDecimal", DataType.DECIMAL, 5), column("someDecimal", DataType.DECIMAL, 4));
      fail("Reduction in precision should not be permitted");
    } catch (Exception e) {
      assertTrue("Column name 'someDecimal' should be mentioned", e.getMessage().contains("someDecimal"));
    }

    try {
      new ChangeColumn("Apple", column("someDecimal", DataType.DECIMAL, 5, 4), column("someDecimal", DataType.DECIMAL, 5, 3));
      fail("Reduction in precision-scale should not be permitted");
    } catch (Exception e) {
      assertTrue("Column name 'someDecimal' should be mentioned", e.getMessage().contains("someDecimal"));
    }

    try {
      new ChangeColumn("Apple", column("someDecimal", DataType.DECIMAL, 5, 3), column("someDecimal", DataType.DECIMAL, 5, 4));
      fail("Reduction in precision-scale should not be permitted");
    } catch (Exception e) {
      assertTrue("Column name 'someDecimal' should be mentioned", e.getMessage().contains("someDecimal"));
    }
  }


  /**
   * Tests that changing the type of a column fails
   */
  @Test
  public void testInvalidChangeColumnType() {
    try {
      new ChangeColumn("Apple", column("ispoisoned", DataType.STRING).nullable(), column("ispoisoned", DataType.DECIMAL, 1).nullable());
      fail("Change column type should not be permitted");
    } catch (Exception e) {
      assertTrue("New data type STRING should be mentioned", e.getMessage().contains("STRING"));
      assertTrue("Old data type BOOLEAN should be mentioned", e.getMessage().contains("DECIMAL"));
      assertTrue("Column name 'ispoisoned' should be mentioned", e.getMessage().contains("ispoisoned"));
    }
  }


  /**
   * Tests that changing the name of a column works
   */
  @Test
  public void testChangeColumnName() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("variety", DataType.STRING, 15).nullable(), column("brand", DataType.STRING, 15).nullable());
    Schema updatedSchema = changeColumn.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnOne = resultTable.columns().get(2);
    Column columnTwo = resultTable.columns().get(3);
    Column columnThree = resultTable.columns().get(4);
    Column columnFour = resultTable.columns().get(5);
    Column columnFive = resultTable.columns().get(6);
    Column columnSix = resultTable.columns().get(7);
    Column columnSeven = resultTable.columns().get(8);

    assertEquals("Post upgrade existing column name 1", "colour", columnOne.getName());
    assertEquals("Post upgrade existing column name 2", "brand", columnTwo.getName());
    assertEquals("Post upgrade existing column name 3", "ispoisoned", columnThree.getName());
    assertEquals("Post upgrade existing column name 4", "datecreated", columnFour.getName());
    assertEquals("Post upgrade existing column name 5", "numberavailable", columnFive.getName());
    assertEquals("Post upgrade existing column name 6", "totalvalue", columnSix.getName());
    assertEquals("Post upgrade existing column name 7", "nullcheck", columnSeven.getName());

    assertEquals("Width should not have changed", 10, columnOne.getWidth());
    assertEquals("Width should not have changed", 15, columnTwo.getWidth());
    assertEquals("Width should not have changed", 5, columnFive.getWidth());
    assertEquals("Width should not have changed", 9, columnSix.getWidth());
    assertEquals("Width should not have changed", 9, columnSeven.getWidth());

    assertEquals("Scale should not have changed", 0, columnOne.getScale());
    assertEquals("Scale should not have changed", 0, columnTwo.getScale());
    assertEquals("Scale should not have changed", 0, columnFive.getScale());
    assertEquals("Scale should not have changed", 2, columnSix.getScale());
    assertEquals("Scale should not have changed", 0, columnSeven.getScale());

    assertEquals("Type should not have changed", DataType.STRING, columnOne.getType());
    assertEquals("Type should not have changed", DataType.STRING, columnTwo.getType());
    assertEquals("Type should not have changed", DataType.BOOLEAN, columnThree.getType());
    assertEquals("Type should not have changed", DataType.DATE, columnFour.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnFive.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSix.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSeven.getType());

    assertTrue("Nullable should not have changed", columnOne.isNullable());
    assertTrue("Nullable should not have changed", columnTwo.isNullable());
    assertTrue("Nullable should not have changed", columnThree.isNullable());
    assertTrue("Nullable should not have changed", columnFour.isNullable());
    assertFalse("Nullable should not have changed", columnFive.isNullable());
    assertFalse("Nullable should not have changed", columnSix.isNullable());
    assertTrue("Nullable should not have changed", columnSeven.isNullable());
  }


  /**
   * Tests that changing the null status of a column works
   */
  @Test
  public void testChangeColumnNullable() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("nullcheck", DataType.DECIMAL, 9, 0).nullable(), column("nullcheck", DataType.DECIMAL, 9, 0));
    Schema updatedSchema = changeColumn.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnOne = resultTable.columns().get(2);
    Column columnTwo = resultTable.columns().get(3);
    Column columnThree = resultTable.columns().get(4);
    Column columnFour = resultTable.columns().get(5);
    Column columnFive = resultTable.columns().get(6);
    Column columnSix = resultTable.columns().get(7);
    Column columnSeven = resultTable.columns().get(8);

    assertEquals("Post upgrade existing column name 1", "colour", columnOne.getName());
    assertEquals("Post upgrade existing column name 2", "variety", columnTwo.getName());
    assertEquals("Post upgrade existing column name 3", "ispoisoned", columnThree.getName());
    assertEquals("Post upgrade existing column name 4", "datecreated", columnFour.getName());
    assertEquals("Post upgrade existing column name 5", "numberavailable", columnFive.getName());
    assertEquals("Post upgrade existing column name 6", "totalvalue", columnSix.getName());
    assertEquals("Post upgrade existing column name 7", "nullcheck", columnSeven.getName());

    assertEquals("Width should not have changed", 10, columnOne.getWidth());
    assertEquals("Width should not have changed", 15, columnTwo.getWidth());
    assertEquals("Width should not have changed", 5, columnFive.getWidth());
    assertEquals("Width should not have changed", 9, columnSix.getWidth());
    assertEquals("Width should not have changed", 9, columnSeven.getWidth());

    assertEquals("Scale should not have changed", 0, columnOne.getScale());
    assertEquals("Scale should not have changed", 0, columnTwo.getScale());
    assertEquals("Scale should not have changed", 0, columnFive.getScale());
    assertEquals("Scale should not have changed", 2, columnSix.getScale());
    assertEquals("Scale should not have changed", 0, columnSeven.getScale());

    assertEquals("Type should not have changed", DataType.STRING, columnOne.getType());
    assertEquals("Type should not have changed", DataType.STRING, columnTwo.getType());
    assertEquals("Type should not have changed", DataType.BOOLEAN, columnThree.getType());
    assertEquals("Type should not have changed", DataType.DATE, columnFour.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnFive.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSix.getType());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSeven.getType());

    assertTrue("Nullable should not have changed", columnOne.isNullable());
    assertTrue("Nullable should not have changed", columnTwo.isNullable());
    assertTrue("Nullable should not have changed", columnThree.isNullable());
    assertTrue("Nullable should not have changed", columnFour.isNullable());
    assertFalse("Nullable should not have changed", columnFive.isNullable());
    assertFalse("Nullable should not have changed", columnSix.isNullable());
    assertFalse("Nullable should not have changed", columnSeven.isNullable());
  }


  /**
   * Tests that reversing a change column length operation works
   */
  @Test
  public void testReverseChangeLength() {
    appleTable = table("Apple").columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 35).nullable(),
        column("variety", DataType.STRING, 15).nullable(),
        column("ispoisoned", DataType.BOOLEAN).nullable(),
        column("datecreated", DataType.DATE).nullable(),
        column("numberavailable", DataType.DECIMAL, 5, 0),
        column("totalvalue", DataType.DECIMAL, 9, 2),
        column("nullcheck", DataType.DECIMAL, 9, 0).nullable()
      );

    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("colour", DataType.STRING, 10).nullable(), column("colour", DataType.STRING, 35).nullable());

    Schema updatedSchema = changeColumn.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnOne = resultTable.columns().get(2);

    assertEquals("Post upgrade existing column name 1", "colour", columnOne.getName());
    assertEquals("Width should not have changed", 10, columnOne.getWidth());
    assertEquals("Scale should not have changed", 0, columnOne.getScale());
    assertEquals("Type should not have changed", DataType.STRING, columnOne.getType());
    assertTrue("Nullable should not have changed", columnOne.isNullable());
  }


  /**
   * Tests that reversing a change column type operation works.
   */
  @Test
  public void testReverseChangeColumnType() {
    Table alreadyChangedTable = table("Apple").columns(
      idColumn(),
      versionColumn(),
      column("colour", DataType.STRING, 10).nullable(),
      column("variety", DataType.STRING, 15).nullable(),
      column("ispoisoned", DataType.BOOLEAN).nullable(),
      column("datecreated", DataType.DATE).nullable(),
      column("numberavailable", DataType.BIG_INTEGER),
      column("totalvalue", DataType.DECIMAL, 9, 2),
      column("nullcheck", DataType.DECIMAL, 9, 0).nullable()
    ).indexes(
      index("Apple_1").columns("colour")
    );

    Schema testSchema = schema(alreadyChangedTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("numberavailable", DataType.DECIMAL, 5), column("numberavailable", DataType.BIG_INTEGER));

    Schema updatedSchema = changeColumn.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);

    assertTrue(new SchemaHomology(new ThrowingDifferenceWriter(), "expected", "result").tablesMatch(appleTable, resultTable));
  }


  /**
   * Tests that reversing a change column name operation works
   */
  @Test
  public void testReverseChangeColumnName() {
    appleTable = table("Apple").columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 10).nullable(),
        column("brand", DataType.STRING, 15).nullable(),
        column("ispoisoned", DataType.BOOLEAN).nullable(),
        column("datecreated", DataType.DATE).nullable(),
        column("numberavailable", DataType.DECIMAL, 5, 0),
        column("totalvalue", DataType.DECIMAL, 9, 2),
        column("nullcheck", DataType.DECIMAL, 9, 0).nullable()
      );


    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("variety", DataType.STRING, 15).nullable(), column("brand", DataType.STRING, 15).nullable());

    Schema updatedSchema = changeColumn.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnTwo = resultTable.columns().get(3);

    assertEquals("Post upgrade existing column name 2", "variety", columnTwo.getName());
    assertEquals("Width should not have changed", 15, columnTwo.getWidth());
    assertEquals("Scale should not have changed", 0, columnTwo.getScale());
    assertEquals("Type should not have changed", DataType.STRING, columnTwo.getType());
    assertTrue("Nullable should not have changed", columnTwo.isNullable());
  }


  /**
   * Tests that reversing a change column is nullable operation works
   */
  @Test
  public void testReverseChangeColumnNullable() {
    appleTable = table("Apple").columns(
        idColumn(),
        versionColumn(),
        column("colour", DataType.STRING, 10).nullable(),
        column("variety", DataType.STRING, 15).nullable(),
        column("ispoisoned", DataType.BOOLEAN).nullable(),
        column("datecreated", DataType.DATE).nullable(),
        column("numberavailable", DataType.DECIMAL, 5, 0),
        column("totalvalue", DataType.DECIMAL, 9, 2),
        column("nullcheck", DataType.DECIMAL, 9, 0)
      );

    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("nullcheck", DataType.DECIMAL, 9, 0).nullable(), column("nullcheck", DataType.DECIMAL, 9, 0));

    Schema updatedSchema = changeColumn.reverse(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnSeven = resultTable.columns().get(8);

    assertEquals("Post upgrade existing column name 7", "nullcheck", columnSeven.getName());
    assertEquals("Width should not have changed", 9, columnSeven.getWidth());
    assertEquals("Scale should not have changed", 0, columnSeven.getScale());
    assertEquals("Type should not have changed", DataType.DECIMAL, columnSeven.getType());
    assertTrue("Nullable should not have changed", columnSeven.isNullable());
  }


  /**
   * Tests that changing a non-existant field fails
   */
  @Test
  public void testChangeMissingField() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("doesntexist", DataType.STRING, 10).nullable(), column("doesntexist", DataType.STRING, 35).nullable());
    try {
      changeColumn.apply(testSchema);
      fail("Should have failed to change a non existant column on apply");
    } catch(Exception e) {
      // Not a problem
    }
  }


  /**
   * Tests that changing a non-existant field fails
   */
  @Test
  public void testChangeFieldIsCaseInsensitive() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("APPLE", column("VARIETY", DataType.STRING, 15).nullable(), column("Variety", DataType.STRING, 35).nullable());
    Schema result = changeColumn.apply(testSchema);

    Table newTable = result.getTable("Apple");
    assertNotNull("Table not found", newTable);

    // Find the colour colummn
    Column changedColumn = null;
    for (Column column : newTable.columns()) {
      if (column.getName().equals("Variety")) {
        assertEquals("Changed column length", 35, column.getWidth());
        changedColumn = column;
      }
    }

    assertNotNull("Column not found", changedColumn);
  }


  /**
   * Tests that renaming a field to have the same name as an existing field fails
   */
  @Test
  public void testChangeFieldNameToExistingField() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("ispoisoned", DataType.STRING, 10).nullable(), column("colour", DataType.STRING, 35).nullable());
    try {
      changeColumn.apply(testSchema);
      fail("Should have failed to change name to match an existing field on apply");
    } catch(Exception e) {
      // Not a problem
    }
  }


  /**
   * Tests that renaming a field which is contained in an index fails.
   */
  @Test
  public void testChangeFieldNameInIndex() {
    Schema testSchema = schema(pearTable);
    ChangeColumn changeColumn = new ChangeColumn("Pear", column("color", DataType.STRING, 35).nullable(), column("colour", DataType.STRING, 35).nullable());
    try {
      changeColumn.apply(testSchema);
      fail("Should have failed to change name to match an existing field on apply");
    } catch(IllegalArgumentException e) {
      // Not a problem
    }
  }


  /**
   * Tests that passing a null for the column change results in an exception
   */
  @Test
  public void testNullChangeColumn() {
    try {
      new ChangeColumn("Apple", column("ispoisoned", DataType.STRING, 10).nullable(), null);
      fail("Attempting to change a column to null should result in an exception on apply");
    } catch(Exception e) {
      // Not a problem
    }


    try {
      new ChangeColumn("Apple", null, column("nullcheck", DataType.STRING, 10).nullable());
      fail("Attempting to change a column from null should result in an exception on apply");
    } catch(Exception e) {
      // Not a problem
    }
  }


  /**
   * Tests that a column can be changed so it's no longer the primary key.
   */
  @Test
  public void testChangePrimaryKey() {
    Schema testSchema = schema(appleTable);
    ChangeColumn changeColumn = new ChangeColumn("Apple", column("id", DataType.BIG_INTEGER).primaryKey(), column("id", DataType.BIG_INTEGER));
    Schema updatedSchema = changeColumn.apply(testSchema);

    Table resultTable = updatedSchema.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post upgrade column count", 9, resultTable.columns().size());

    Column columnOne = resultTable.columns().get(0);

    assertEquals("Post upgrade existing column name 1", "id", columnOne.getName());
    assertFalse("Primary key should have changed", columnOne.isPrimaryKey());
  }


  /**
   * Test that renames of columns present in indexes is also changing the column name within the index.
   */
  @Test
  public void testCannotRenameColumnThatAppearsInIndexes() {
    Schema testSchema = schema(pearTable);

    Schema changedSchema = new ChangeColumn("Pear",
      column("colour", DataType.STRING, 10).nullable(),
      column("hue", DataType.STRING, 10).nullable()
    ).apply(testSchema);

    // Make sure the column name in the index has changed
    assertTrue("Index with a field named 'hue'", changedSchema.getTable("Pear").indexes().stream().flatMap(index -> index.columnNames().stream()).anyMatch(colName -> colName.equalsIgnoreCase("hue")));
    assertFalse("Index with a field named 'colour'", changedSchema.getTable("Pear").indexes().stream().flatMap(index -> index.columnNames().stream()).anyMatch(colName -> colName.equalsIgnoreCase("colour")));
  }
}
