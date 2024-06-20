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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.sequence;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Test;

/**
 * Tests for {@link SchemaValidator}.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSchemaValidator {

  private static final String EDGE_CASE_VALID_NAME_60_CHARACTERS = "EdgeCaseValidName60CharactersxSomeMoreThingsAndEvenMoreXYZ12";
  private static final String INVALID_NAME_61_CHARACTERS = "InvalidName64CharactersXxxZZZZZabc123abc123abc123abc123aXYZ12";

  /**
   * A simple valid table
   */
  private final Table simpleValidTable =
      table("SimpleValid")
        .columns(
          column("myname", DataType.STRING, 10).nullable(),
          column("strength", DataType.STRING, 10).nullable(),
          column("palate", DataType.STRING, 10).nullable(),
          column("cost", DataType.DECIMAL, 10).nullable()
        ).indexes(
          index("SimpleValid_NK").unique().columns("myname")
        );

  /**
   * A valid table on the edge
   * i.e. with 60 character table name, index name and column names
   */
  private final Table edgeCaseValidTable =
      table(EDGE_CASE_VALID_NAME_60_CHARACTERS)
        .columns(
          column("myname", DataType.STRING, 10).nullable(),
          column("vintage", DataType.DECIMAL, 10).nullable(),
          column("EdgeCaseValid60CharColumnNameXab123ab123ab123ab123ab12345678", DataType.STRING, 10).nullable()
        ).indexes(
          index("EdgeCaseValid_NK").unique().columns("myname"),
          index("EdgeCaseValid60CharIndNameXab123ab123ab123ab123ab12356789_NK").columns("vintage")
        );

  /**
   * An invalid table with a 61 character table name
   */
  private final Table invalidTableNameTable =
      table(INVALID_NAME_61_CHARACTERS)
        .columns(
          column("myname", DataType.STRING, 10).nullable(),
          column("altitude", DataType.DECIMAL, 10).nullable()
        ).indexes(
          index("InvalidTableNameOkIndex_NK").unique().columns("myname")
        );


  /**
   * An invalid table with a 61 character index name
   */
  private final Table invalidIndexNameTable =
      table("InvalidIndexNameTable")
        .columns(
          column("myname", DataType.STRING, 10).nullable(),
          column("maximumPressure", DataType.DECIMAL, 10).nullable()
        ).indexes(
          index("OkIndex_NK").unique().columns("myname"),
          index("InvalidIndexName61CharactersXxxab123ab123ab123ab123ab123XYZ12").columns("altitude")
        );


  /**
   * An invalid table with a 61 character column name
   */
  private final Table invalidColumnNameTable =
      table("InvalidColumnNameTable")
        .columns(
          column("myname", DataType.STRING, 10).nullable(),
          column("minimumPurity", DataType.DECIMAL, 10).nullable(),
          column("Invalid61CharacterColumnNameXxxab123ab123ab123ab123ab123XYZ12", DataType.STRING, 10).nullable()
        ).indexes(
          index("OkIndex_NK").unique().columns("myname"),
          index("InvalidColumnNameOKIndex").columns("altitude")
        );


  /**
   * Tests a valid schema, including edge cases for table/column names.;
   */
  @Test
  public void testValidSchema() {
    Schema validSchema = schema(simpleValidTable, edgeCaseValidTable);
    SchemaValidator validator = new SchemaValidator();
    validator.validate(validSchema);
  }


  /**
   * Tests a schema containing a table with an invalid name throws an exception.
   */
  @Test
  public void testInvalidTableName() {
    Schema invalidTableNameSchema = schema(simpleValidTable, invalidTableNameTable);
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(invalidTableNameSchema);
      fail("Expected a RuntimeException for an invalid table name");
    } catch (RuntimeException e) {
      assertTrue("Expected [" + INVALID_NAME_61_CHARACTERS + "] in error message", e.getMessage().contains(INVALID_NAME_61_CHARACTERS));
    }
  }


  /**
   * Tests a schema containing table names which should be valid.
   */
  @Test
  public void testValidTableNames() {
    SchemaValidator validator = new SchemaValidator();
    validator.validate(
      table("Table_with_underscores").columns(
        column("id", DataType.BIG_INTEGER).primaryKey()
      )
    );
  }


  /**
   * Tests a schema containing view names which should be valid.
   */
  @Test
  public void testValidViewNames() {
    SchemaValidator validator = new SchemaValidator();
    validator.validate(view("View_with_underscores", null));
    validator.validate(view(EDGE_CASE_VALID_NAME_60_CHARACTERS, null));
    validator.validate(view("ABCDEFGHIJKLMNOPQRSTUVWXYZ", null));
    validator.validate(view("abcdefghijklmnopqrstuvwxyz", null));
    validator.validate(view("a1234567890", null));
  }


  /**
   * Tests a schema containing view names which should not be valid.
   */
  @Test
  public void testInvalidViewNames() {
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(view(INVALID_NAME_61_CHARACTERS, null));
      fail("should have thrown an exception");
    } catch (RuntimeException e) {
      assertTrue("Expected [ " + INVALID_NAME_61_CHARACTERS + "] in error message", e.getMessage().contains(INVALID_NAME_61_CHARACTERS));
    }

    final String badViewName = "NameWithBadCharacter!";
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(view(badViewName, null));
      fail("should have thrown an exception");
    } catch (RuntimeException e) {
      assertTrue("Expected [ " + badViewName + "] in error message", e.getMessage().contains(badViewName));
    }
  }


  /**
   * Tests a schema containing sequence names which should be valid.
   */
  @Test
  public void testValidSequenceNames() {
    SchemaValidator validator = new SchemaValidator();
    validator.validate(sequence("Sequence_with_underscores"));
    validator.validate(sequence(EDGE_CASE_VALID_NAME_60_CHARACTERS));
    validator.validate(sequence("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
    validator.validate(sequence("abcdefghijklmnopqrstuvwxyz"));
    validator.validate(sequence("a1234567890"));
  }


  /**
   * Tests a schema containing sequence names which should not be valid.
   */
  @Test
  public void testInvalidSequenceNames() {
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(sequence(INVALID_NAME_61_CHARACTERS));
      fail("should have thrown an exception");
    } catch (RuntimeException e) {
      assertTrue("Expected [ " + INVALID_NAME_61_CHARACTERS + "] in error message", e.getMessage().contains(INVALID_NAME_61_CHARACTERS));
    }

    final String badSequenceName = "NameWithBadCharacter!";
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(sequence(badSequenceName));
      fail("should have thrown an exception");
    } catch (RuntimeException e) {
      assertTrue("Expected [ " + badSequenceName + "] in error message", e.getMessage().contains(badSequenceName));
    }
  }


  /**
   * Tests a schema containing view with selected fields that are valid.
   */
  @Test
  public void testValidViewColumnNames() {
    SchemaValidator validator = new SchemaValidator();
    TableReference table = tableRef("Foo");
    SelectStatement selectStatement = select(table.field(EDGE_CASE_VALID_NAME_60_CHARACTERS),
      table.field("Column_with_underscores"),
      table.field("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
      table.field("abcdefghijklmnopqrstuvwxyz"),
      table.field("a1234567890"));

    validator.validate(view("SimpleView", selectStatement));
  }


  /**
   * Tests a schema containing view with selected fields that are not valid.
   */
  @Test
  public void testInvalidViewColumnNames() {
    final String columnWithBadCharacters = "Column_with?!";
    final String columnWithReservedWords = "operator";
    try {
      TableReference table = tableRef("Foo");
      SelectStatement selectStatement = select(table.field(INVALID_NAME_61_CHARACTERS),
        table.field(columnWithBadCharacters),
        table.field(columnWithReservedWords));

      SchemaValidator validator = new SchemaValidator();
      validator.validate(view("SimpleView", selectStatement));
      fail("should have thrown an exception");
    } catch (RuntimeException e) {
      assertTrue("Expected [ " + INVALID_NAME_61_CHARACTERS + "] in error message", e.getMessage().contains(INVALID_NAME_61_CHARACTERS));
      assertTrue("Expected [ " + columnWithBadCharacters + "] in error message", e.getMessage().contains(columnWithBadCharacters));
      assertTrue("Expected [ " + columnWithReservedWords + "] in error message", e.getMessage().contains(columnWithReservedWords));
    }
  }


  /**
   * Tests a schema containing a table with an invalid index name throws an exception.
   */
  @Test
  public void testInvalidIndexName() {
    Schema invalidIndexNameSchema = schema(simpleValidTable, invalidIndexNameTable);
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(invalidIndexNameSchema);
      fail("Expected a RuntimeException for an invalid index name");
    } catch (RuntimeException e) {
      assertTrue("Expected [InvalidIndexName61CharactersXxxab123ab123ab123ab123ab123XYZ12] in error message",
        e.getMessage().contains("InvalidIndexName61CharactersXxxab123ab123ab123ab123ab123XYZ12"));
    }
  }


  /**
   * Tests a schema containing a table with an invalid column name throws an exception.
   */
  @Test
  public void testInvalidColumnName() {
    Schema invalidColumnNameSchema = schema(simpleValidTable, invalidColumnNameTable);
    try {
      SchemaValidator validator = new SchemaValidator();
      validator.validate(invalidColumnNameSchema);
      fail("Expected a RuntimeException for an invalid column name");
    } catch (RuntimeException e) {
      assertTrue("Expected [Invalid61CharacterColumnNameXxxab123ab123ab123ab123ab123XYZ1] in error message",
          e.getMessage().contains("Invalid61CharacterColumnNameXxxab123ab123ab123ab123ab123XYZ1"));
    }
  }


  /**
   * "Transaction" is a reserved word. We shouldn't be able to name a table that.
   */
  @Test
  public void testInvalidReservedWordTableName() {
    tryInvalidTableName("transaction");
    tryInvalidTableName("Transaction");
    tryInvalidTableName("TRANSACTION");
    tryInvalidTableName("User");
  }


  /**
   * "Transaction" is a reserved word. We shouldn't be able to name a table that.
   */
  @Test
  public void testInvalidReservedWordIndexName() {
    tryInvalidIndexName("attribute");
  }


  /**
   * "data" is a reserved word. We shouldn't be able to name a column that.
   */
  @Test
  public void testInvalidReservedWordColumnName() {
    tryInvalidColumnName("data");
  }


  /**
   * "data" is a reserved word. We shouldn't be able to name a column that.
   */
  @Test
  public void testInvalidCharactersInColumnName() {
    tryInvalidColumnName("£perk#");
    tryInvalidColumnName("some_field");
    tryInvalidColumnName("3trees");
    tryInvalidColumnName("a#field");
    tryInvalidColumnName("a!field");
    tryInvalidColumnName("a!field");
    tryInvalidColumnName("a$field");
  }


  /**
   *
   */
  @Test
  public void testInvalidCharactersInTableName() {
    tryInvalidTableName("1hello");
    tryInvalidTableName("a_b_c");
    tryInvalidTableName("Blah!");
  }

  /**
   *
   */
  @Test
  public void testInvalidCharactersInIndexName() {
    tryInvalidIndexName("a#b");
    tryInvalidIndexName("1abc");
  }


  /**
   * Try to run the validator with the specified column name, expect a failure.
   *
   * @param columnName The column name to try.
   */
  private void tryInvalidColumnName(String columnName) {
    Table table = table("SomeValidTableName").columns(column(columnName, DataType.STRING).nullable());
    Schema schema = schema(table);

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an invalid column name [" + columnName + "]");
    } catch (RuntimeException e) {
      assertTrue("Expected [+columnName+] in error message: " + e.getMessage(),
        e.getMessage().contains(columnName));
    }
  }


  /**
   * Try to run the validator with the specified table name, expect a failure.
   *
   * @param tableName The column name to try.
   */
  private void tryInvalidTableName(String tableName) {
    Schema schema = schema(
      table(tableName)
        .columns(
          column("column1", DataType.STRING).nullable(),
          column("column1", DataType.DECIMAL).nullable()
         )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an invalid table name: [" + tableName + "]");
    } catch (RuntimeException e) {
      assertTrue("Expected [" + tableName + "] in error message", e.getMessage().contains(tableName));
    }
  }


  /**
   * Try to run the validator with the specified index name, expect a failure.
   *
   * @param indexName The column name to try.
   */
  private void tryInvalidIndexName(String indexName) {
    Schema schema = schema(
      table("SomeValidTableName")
        .columns(
          column("column1", DataType.STRING).nullable()
        ).indexes(
          index(indexName).columns("column1").unique()
        )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an invalid index name: [" + indexName + "]");
    } catch (RuntimeException e) {
      assertTrue("Expected ["+indexName+"] in error message: ["+e.getMessage()+"]",
        e.getMessage().contains(indexName));
    }
  }

  /**
   * Try adding a superfluous index by id
   */
  @Test
  public void testIndexesThatDuplicatesId() {
    Schema schema = schema(
      table("SomeValidTableName")
        .columns(
          column("column1", DataType.STRING).nullable()
        ).indexes(
          index("badIndex").unique().columns("id")
        )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an invalid index: [badIndex]");
    } catch (RuntimeException e) {
      assertTrue("Expected badIndex in error message",
        e.getMessage().contains("badIndex"));
    }
  }

  /**
   * Try adding a duplicate index.
   */
  @Test
  public void testDuplicateIndexes() {
    Schema schema = schema(
      table("SomeValidTableName")
        .columns(
          column("column1", DataType.STRING).nullable(),
          column("column2", DataType.STRING).nullable()
        ).indexes(
          index("index1").unique().columns("column1", "column2"),
          index("index2").unique().columns("column1", "column2")
        )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an duplicate index: [index1] [index2]");
    } catch (RuntimeException e) {
      assertTrue("Expected index1 in error message",
        e.getMessage().contains("index1"));
      assertTrue("Expected index2 in error message",
        e.getMessage().contains("index2"));
    }
  }


  /**
   * Tests for an index which duplicates the primary domain index.
   */
  @Test
  public void testIndexesThatDuplicatesPrimaryIndex() {
    Schema schema = schema(
      table("SomeValidTableName")
        .columns(
          column("column1", DataType.STRING, 15).primaryKey()
        ).indexes(
          index("index1").unique().columns("column1")
        )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an duplicate index: [index1] [PRIMARY]");
    } catch (RuntimeException e) {
      assertTrue("Expected index1 in error message",
        e.getMessage().contains("index1"));
      assertTrue("Expected index2 in error message",
        e.getMessage().contains("PRIMARY"));
    }
  }

  /**
   * Tests for an index which duplicates a composite primary domain index.
   */
  @Test
  public void testIndexesThatDuplicatesCompositePrimaryIndex() {
    Schema schema = schema(
      table("SomeValidTableName")
        .columns(
          column("column1", DataType.STRING, 15).primaryKey(),
          column("column2", DataType.STRING, 15).primaryKey(),
          column("column3", DataType.STRING, 15)
        ).indexes(
          index("index1").unique().columns("column1", "column2")
        )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an duplicate index: [index1]");
    } catch (RuntimeException e) {
      assertTrue("Expected index1 in error message", e.getMessage().contains("index1"));
      assertTrue("Expected index2 in error message", e.getMessage().contains("PRIMARY"));
    }
  }


  /**
   * Tests a schema containing a table with an invalid zero width column throws an exception.
   */
  @Test
  public void testInvalidColumnWidthTable() {
    // An invalid table with a zero width column
    Schema schema = schema(table("InvalidColumnWidthTable")
      .columns(
        column("myName", DataType.STRING, 20).nullable(),
        column("dateOfBirth", DataType.DECIMAL, 8).nullable(),
        column("gender", DataType.STRING, 0).nullable(), // Bad - STRING requires width
        column("vo2Max", DataType.DECIMAL, 0).nullable(), // Bad - DECIMAL requires width
        column("restingHeartRate", DataType.INTEGER, 0).nullable() // Fine - INTEGER doesn't require width
      )
    );

    try {
      new SchemaValidator().validate(schema);
      fail("Expected a RuntimeException for an invalid zero width column");
    } catch (RuntimeException e) {
      assertTrue("Expected column gender in error message: "+e.getMessage(),
        e.getMessage().contains("[gender]"));
      assertTrue("Expected column vo2Max in error message",
        e.getMessage().contains("[vo2Max]"));

      assertFalse("Don't expect column myName in error message", e.getMessage().contains("myName"));
      assertFalse("Don't expect column dateOfBirth in error message", e.getMessage().contains("dateOfBirth"));
      assertFalse("Don't expect column restingHeartRate in error message", e.getMessage().contains("restingHeartRate"));
    }
  }


  @Test
  public void testInvalidPrimaryKeyAndNullableColumn() {
    Table invalidPkAndNullable =
        table("InvalidColumn")
          .columns(
            column("pkandnullable", DataType.STRING, 10).nullable().primaryKey()
          );
    try {
      new SchemaValidator().validate(schema(invalidPkAndNullable));
      fail();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("pkandnullable"));
      assertTrue(e.getMessage().contains("InvalidColumn"));
    }

    try {
      new SchemaValidator().validate(invalidPkAndNullable);
      fail();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("pkandnullable"));
      assertTrue(e.getMessage().contains("InvalidColumn"));
    }
  }
}


