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

package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.SchemaUtils.autonumber;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


/**
 * Tests for {@link SchemaHomology}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSchemaHomology {

  /** Test data */
  private Table appleTable;

  /** Test data */
  private Table appleTableDuplicate;

  /** Test data */
  private Table appleTableChangeCase;

  /** Test data */
  private Table appleNameChange;

  /** Test data */
  private Table appleTableWithExtraColumn;

  /** Test data */
  private Table appleTableWithMissingColumn;

  /** Test data */
  private Table appleTableWithRenamedColumn;

  /** Test data */
  private Table appleTableMissingIndex;

  /** Test data */
  private Table appleTableRenamedIndex;

  /** Test data */
  private Table appleTableNotAutonumbered;

  /** Test data */
  private Table pearTable;

  /** Test data */
  private Table pearTableModifiedWidth;

  /** Test data */
  private Table pearTableModifiedScale;

  /** Test data */
  private Table pearTableModifiedNullable;

  /** Test data */
  private Table simpleTable;

  /** Test data */
  private Table appleTableWithComplexKey;

  /** Homology to be tested */
  private SchemaHomology schemaHomology;

  private Set<String> differences;

  /**
   * @see junit.framework.TestCase#setUp()
   */
  @Before
  public void setUp() throws Exception {

    differences = Sets.newHashSet();

    DifferenceWriter differenceWriter = new DifferenceWriter() {
      @Override
      public void difference(String message) {
        differences.add(message);
      }
    };

    schemaHomology = new SchemaHomology(differenceWriter, "SCHEMA1", "SCHEMA2");

    appleTable = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );

    appleTableDuplicate = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );


    appleTableChangeCase = table("APPLE").columns(
        autonumber("autonum", 3),
        column("COLOUR", DataType.STRING).nullable(),
        column("FLAVOUR", DataType.DECIMAL).nullable(),
        column("SWEET", DataType.BOOLEAN).nullable()
      ).indexes(        index("SWEETNESS").unique().columns("COLOUR")
      );


    appleNameChange = table("Pear").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );


    appleTableWithExtraColumn = table("Apple").columns(
      autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("name", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );


    appleTableWithMissingColumn = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );


    appleTableWithRenamedColumn = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavor", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetness").unique().columns("colour")
      );


    appleTableMissingIndex = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweetneess").unique().columns("colour")
      );


    appleTableRenamedIndex = table("Apple").columns(
        autonumber("autonum", 3),
        column("colour", DataType.STRING).nullable(),
        column("flavour", DataType.DECIMAL).nullable(),
        column("sweet", DataType.BOOLEAN).nullable()
      ).indexes(        index("sweety").unique().columns("colour")
      );

    appleTableNotAutonumbered = table("Apple").columns(
      column("autonum", DataType.BIG_INTEGER).primaryKey(),
      column("colour", DataType.STRING).nullable(),
      column("flavour", DataType.DECIMAL).nullable(),
      column("sweet", DataType.BOOLEAN).nullable()
    ).indexes(
      index("sweetness").unique().columns("colour")
    );

    appleTableWithComplexKey = table("AppleTableWithComplexKey").columns(
      column("colour", DataType.STRING).primaryKey(),
      column("flavour", DataType.DECIMAL).primaryKey(),
      column("sweet", DataType.BOOLEAN).primaryKey()
    ).indexes(
      index("sweetness").unique().columns("colour")
    );


    pearTable = table("Pear").columns(column("flavour", DataType.DECIMAL, 30, 10).nullable());

    pearTableModifiedWidth = table("Pear").columns(column("flavour", DataType.DECIMAL, 31, 10).nullable());

    pearTableModifiedScale = table("Pear").columns(column("flavour", DataType.DECIMAL, 30, 11).nullable());

    pearTableModifiedNullable = table("Pear").columns(column("flavour", DataType.DECIMAL, 30, 10));

    simpleTable = table("MyTable");
  }

  /**
   * Test two identical tables
   */
  @Test
  public void testDuplicate() {
    boolean match = schemaHomology.tablesMatch(appleTable, appleTableDuplicate);
    assertTrue(match);
  }


  /**
   * Change the name of one table
   */
  @Test
  public void testNameChange() {
    assertFalse(schemaHomology.tablesMatch(appleTable, appleNameChange));
  }


  /**
   * Make one table autonumbered and the other not
   */
  @Test
  public void testAutonumbering() {
    assertFalse(schemaHomology.tablesMatch(appleTable, appleTableNotAutonumbered));
  }


  /**
   * The comparator should be case-insensitive
   */
  @Test
  public void testCaseInsensitivity() {
    boolean match = schemaHomology.tablesMatch(appleTable, appleTableChangeCase);
    assertTrue(match);
  }



  /**
   * Add a column
   */
  @Test
  public void testExtraColumn() {
    assertFalse(schemaHomology.tablesMatch(appleTable, appleTableWithExtraColumn));
  }


  /**
   * Remove a column
   */
  @Test
  public void testMissingColumn() {
    assertFalse(schemaHomology.tablesMatch(appleTable, appleTableWithMissingColumn));
  }


  /**
   * Rename a column
   */
  @Test
  public void testRenameColumn() {
    assertFalse("Renamed column should cause tables to be different", schemaHomology.tablesMatch(appleTable, appleTableWithRenamedColumn));
  }


  /**
   * Remove an index
   */
  @Test
  public void testMissingIndex() {
    assertFalse("Missing index should cause tables to be different", schemaHomology.tablesMatch(appleTable, appleTableMissingIndex));
  }


  /**
   * Rename an index
   */
  @Test
  public void testRenamedIndex() {
    assertFalse(schemaHomology.tablesMatch(appleTable, appleTableRenamedIndex));
  }

  /**
   * Modify width
   */
  @Test
  public void testModifyWidth() {
    assertFalse(schemaHomology.tablesMatch(pearTable, pearTableModifiedWidth));
  }

  /**
   * Modify scale
   */
  @Test
  public void testModifyScale() {
    assertFalse(schemaHomology.tablesMatch(pearTable, pearTableModifiedScale));
  }

  /**
   * Modify nullable
   */
  @Test
  public void testModifyNullable() {
    assertFalse(schemaHomology.tablesMatch(pearTable, pearTableModifiedNullable));
  }

  /**
   * The order of indexes shouldn't matter
   */
  @Test
  public void testReorderedIndexes() {
    appleTable.indexes().clear();
    appleTable.indexes().add(index("indexA").unique().columns("A", "B"));
    appleTable.indexes().add(index("indexB").unique().columns("B", "C"));

    appleTableDuplicate.indexes().clear();
    appleTableDuplicate.indexes().add(index("indexB").unique().columns("B", "C"));
    appleTableDuplicate.indexes().add(index("indexA").unique().columns("A", "B"));

    boolean match = schemaHomology.tablesMatch(appleTable, appleTableDuplicate);
    assertTrue(match);
  }

  /**
   * Test that identical schemas match.
   */
  @Test
  public void testIdenticalSchemas() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTable, pearTable, simpleTable);

    boolean match = schemaHomology.schemasMatch(schema1, schema2, Sets.<String>newHashSet());
    assertTrue(match);
  }

  /**
   * Tests that schemas with different tables do not match.
   */
  @Test
  public void testSchemasWithDifferentTables() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTable, pearTable);

    assertFalse(schemaHomology.schemasMatch(schema1, schema2, Sets.<String>newHashSet()));
    assertEquals(ImmutableSet.of("Table [MYTABLE] is present in SCHEMA1 but was not found in SCHEMA2"), differences);
  }


  /**
   * As above but with the schemas reversed.
   */
  @Test
  public void testSchemasWithDifferentTablesReversed() {
    Schema schema1 = schema(appleTable, pearTable);
    Schema schema2 = schema(appleTable, pearTable, simpleTable);

    assertFalse(schemaHomology.schemasMatch(schema1, schema2, Sets.<String>newHashSet()));
    assertEquals(ImmutableSet.of("Table [MYTABLE] is present in SCHEMA2 but was not found in SCHEMA1"), differences);
  }


  /**
   * Tests that schemas that vary only in table definitions do not match.
   */
  @Test
  public void testSchemasWithDifferentTableDefinitions() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTableMissingIndex, pearTable, simpleTable);

    assertFalse(schemaHomology.schemasMatch(schema1, schema2, Sets.<String>newHashSet()));
  }


  /**
   * Check the public {@link SchemaHomology#columnsMatch(Column, Column)} API works.
   */
  @Test
  public void testColumnsMatch() {
    assertTrue("columns should match", schemaHomology.columnsMatch(column("colour", DataType.STRING).nullable(), column("colour", DataType.STRING).nullable()));
    assertTrue("columns should match", schemaHomology.columnsMatch(column("colour", DataType.STRING).nullable(), column("COLOUR", DataType.STRING).nullable()));
    assertFalse("columns should not match", schemaHomology.columnsMatch(column("colour", DataType.STRING).nullable(), column("color", DataType.STRING).nullable()));
  }


  /**
   * Check the public {@link SchemaHomology#indexesMatch(Index, Index)} API works.
   */
  @Test
  public void testIndexesMatch() {
    assertTrue("indexes should match", schemaHomology.indexesMatch(index("ABC").unique().columns("a", "b", "c"), index("ABC").unique().columns("a", "b", "c")));
    assertFalse("indexes should match", schemaHomology.indexesMatch(index("ABC").unique().columns("a", "b", "c"), index("ABC").unique().columns("a", "c", "b")));
  }


  /**
   * Check that the default value on a column is considered as part of the comparison
   */
  @Test
  public void testColumnDefaultValueIsChecked() {
    assertTrue("columns should match", schemaHomology.columnsMatch(column("colour", DataType.STRING, 10), column("colour", DataType.STRING, 10)));
    assertTrue("columns should match", schemaHomology.columnsMatch(column("colour", DataType.STRING, 10).defaultValue("XYZ"), column("colour", DataType.STRING, 10).defaultValue("XYZ")));
    assertFalse("columns should not match", schemaHomology.columnsMatch(column("colour", DataType.STRING, 10).defaultValue("XYZ"), column("colour", DataType.STRING, 10).defaultValue("ABC")));
    assertFalse("columns should not match", schemaHomology.columnsMatch(column("colour", DataType.STRING, 10).defaultValue("XYZ"), column("colour", DataType.STRING, 10)));
  }


  /**
   * Check that different column ordering is detected as a mismatch.
   */
  @Test
  public void testPrimaryKeyColumnOrderIsChecked() {
    Table appleTableReOrdered= table("AppleTableWithComplexKey").columns(
      column("sweet", DataType.BOOLEAN).primaryKey(),
      column("flavour", DataType.DECIMAL).primaryKey(),
      column("colour", DataType.STRING).primaryKey()
    ).indexes(
      index("sweetness").unique().columns("colour")
    );

    assertFalse("Tables should not match", schemaHomology.tablesMatch(appleTableWithComplexKey, appleTableReOrdered));
  }


  /**
   * Check that the table match check is tolerant of a mismatched number of primary key columns.
   */
  @Test
  public void testColumnOrderIsCheckedForDifferentNumberOfColumns() {
    Table appleTableReOrdered= table("appleTableWithComplexKey").columns(
      column("sweet", DataType.BOOLEAN).primaryKey(),
      column("flavour", DataType.DECIMAL).primaryKey()
    ).indexes(
      index("sweetness").unique().columns("colour")
    );

    assertFalse("Tables should not match", schemaHomology.tablesMatch(appleTableWithComplexKey, appleTableReOrdered));
  }


  /**
   * Test that when two schemas differ in the number of tables, it doesn't matter if they are included on the
   * list of excluded tables.
   */
  @Test
  public void testDifferingSchemasWithExcludedTablesMatch() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTable, pearTable);

    Set<String> exclusionRegex = Sets.newHashSet("MYTABLE");
    assertTrue("Schemas", schemaHomology.schemasMatch(schema1, schema2, exclusionRegex));
  }


  /**
   * Test that when two schemas differ in the number of tables the schemas do not match if the
   * list of regular expression is empty.
   */
  @Test
  public void testDifferingSchemasWithoutExcludedTablesDoNotMatch() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTable, pearTable);

    assertFalse("Schemas", schemaHomology.schemasMatch(schema1, schema2, Sets.<String>newHashSet()));
  }


  /**
   * Tests that schemas do not match when there are more tables in one than the other, and the
   * regular expressions available don't cover the difference.
   */
  @Test
  public void testSchemasDoNotMatchWhenNoMatchingRegexIsFound() {
    Schema schema1 = schema(appleTable, pearTable, simpleTable);
    Schema schema2 = schema(appleTable, pearTable);

    Set<String> exclusionRegex = Sets.newHashSet(".*YOURTABLE");
    assertFalse("Schemas", schemaHomology.schemasMatch(schema1, schema2, exclusionRegex));
  }


  /**
   * Tests that schemas match when there are multiple extra tables, all covered by the same regex
   */
  @Test
  public void testSchemasMatchWhenRegexMatchesMultipleTables() {
    Table table1 = table("AbcTable");
    Table table2 = table("AbcChair");
    Table table3 = table("AbcSofa");

    Set<String> exclusionRegex = Sets.newHashSet("^ABC.*");
    Schema schema1 = schema(appleTable, pearTable, table1, table2, table3);
    Schema schema2 = schema(appleTable, pearTable);

    assertTrue("Schemas", schemaHomology.schemasMatch(schema1, schema2, exclusionRegex));
  }


  /**
   * Tests that schemas match when there are multiple extra tables, all covered by the same regex
   */
  @Test
  public void testRegexesAreCaseInsensitive() {
    Table table1 = table("AbcTable");
    Table table2 = table("AbcChair");
    Table table3 = table("AbcSofa");

    Set<String> exclusionRegex = Sets.newHashSet("^aBc.*");
    Schema schema1 = schema(appleTable, pearTable, table1, table2, table3);
    Schema schema2 = schema(appleTable, pearTable);

    assertTrue("Schemas", schemaHomology.schemasMatch(schema1, schema2, exclusionRegex));
  }


  /**
   * Tests that schemas are reported as not matching when two tables with the same name are both present,
   * but differ, and that name is also covered by the ignore regex
   */
  @Test
  public void testSchemasDoNotMatchWhenOneTableIsMisMatchedButIsAlsoCoveredBhTheExclusionRegex() {
    Set<String> exclusionRegex = Sets.newHashSet("APPLE");
    Schema schema1 = schema(appleTable, pearTable);
    Schema schema2 = schema(appleTableWithExtraColumn, pearTable);

    assertFalse("Schemas", schemaHomology.schemasMatch(schema1, schema2, exclusionRegex));
  }


  /**
   * Tests that when given an invalid regular expression, the
   */
  @Test(expected=RuntimeException.class)
  public void testRuntimeExceptionThrownWhenGivenInvalidRegex() {
    Set<String> exclusionRegex = Sets.newHashSet("(A-B)+++++++++");
    Schema schema1 = schema(appleTable, pearTable);
    Schema schema2 = schema(appleTable, pearTable, simpleTable);

    schemaHomology.schemasMatch(schema1, schema2, exclusionRegex);
    fail("Did not throw RuntimeException");
  }
}
