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

package org.alfasoftware.morf.jdbc.h2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;

/**
 * Tests SQL statements generated for H2.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestH2Dialect extends AbstractSqlDialectTest {


  private final static String TEST_SCHEMA = "TESTSCHEMA";


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new H2Dialect(TEST_SCHEMA);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE "+TEST_SCHEMA+".Test (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), blobField LONGVARBINARY, bigIntegerField BIGINT DEFAULT 12345, clobField NCLOB, CONSTRAINT Test_PK PRIMARY KEY (id))",
          "CREATE UNIQUE INDEX Test_NK ON "+TEST_SCHEMA+".Test (stringField)",
          "CREATE UNIQUE INDEX Test_1 ON "+TEST_SCHEMA+".Test (intField,floatField)",
          "CREATE TABLE "+TEST_SCHEMA+".Alternate (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), CONSTRAINT Alternate_PK PRIMARY KEY (id))",
          "CREATE INDEX Alternate_1 ON "+TEST_SCHEMA+".Alternate (stringField)",
          "CREATE TABLE "+TEST_SCHEMA+".NonNull (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BIT NOT NULL, dateField DATE NOT NULL, blobField LONGVARBINARY NOT NULL, CONSTRAINT NonNull_PK PRIMARY KEY (id))",
          "CREATE TABLE "+TEST_SCHEMA+".CompositePrimaryKey (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, secondPrimaryKey VARCHAR(3) NOT NULL, CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id, secondPrimaryKey))",
          "CREATE TABLE "+TEST_SCHEMA+".AutoNumber (intField BIGINT AUTO_INCREMENT(5) COMMENT 'AUTONUMSTART:[5]', CONSTRAINT AutoNumber_PK PRIMARY KEY (intField))"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTemporaryTableStatements()
   */
  @Override
  protected List<String> expectedCreateTemporaryTableStatements() {
    return Arrays
        .asList(
          "CREATE TEMPORARY TABLE "+TEST_SCHEMA+".TEMP_TempTest (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), blobField LONGVARBINARY, bigIntegerField BIGINT DEFAULT 12345, clobField NCLOB, CONSTRAINT TEMP_TempTest_PK PRIMARY KEY (id))",
          "CREATE UNIQUE INDEX TempTest_NK ON "+TEST_SCHEMA+".TEMP_TempTest (stringField)",
          "CREATE INDEX TempTest_1 ON "+TEST_SCHEMA+".TEMP_TempTest (intField,floatField)",
          "CREATE TEMPORARY TABLE "+TEST_SCHEMA+".TEMP_TempAlternate (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), CONSTRAINT TEMP_TempAlternate_PK PRIMARY KEY (id))",
          "CREATE INDEX TempAlternate_1 ON "+TEST_SCHEMA+".TEMP_TempAlternate (stringField)",
          "CREATE TEMPORARY TABLE "+TEST_SCHEMA+".TEMP_TempNonNull (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BIT NOT NULL, dateField DATE NOT NULL, blobField LONGVARBINARY NOT NULL, CONSTRAINT TEMP_TempNonNull_PK PRIMARY KEY (id))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays
        .asList("CREATE TABLE "+TEST_SCHEMA+"."
            + TABLE_WITH_VERY_LONG_NAME
            + " (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), CONSTRAINT "
            + TABLE_WITH_VERY_LONG_NAME + "_PK PRIMARY KEY (id))",
            "CREATE UNIQUE INDEX Test_NK ON "+TEST_SCHEMA+"."+ TABLE_WITH_VERY_LONG_NAME + " (stringField)",
            "CREATE INDEX Test_1 ON "+TEST_SCHEMA+"."+ TABLE_WITH_VERY_LONG_NAME + " (intField,floatField)"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTableStatements()
   */
  @Override
  protected List<String> expectedDropTableStatements() {
    return Arrays.asList("DROP TABLE "+TEST_SCHEMA+".Test CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropSingleTable()
   */
  @Override
  protected List<String> expectedDropSingleTable() {
    return Arrays.asList("DROP TABLE "+TEST_SCHEMA+".Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTables()
   */
  @Override
  protected List<String> expectedDropTables() {
    return Arrays.asList("DROP TABLE "+TEST_SCHEMA+".Test, "+TEST_SCHEMA+".Other");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTablesWithParameters()
   */
  @Override
  protected List<String> expectedDropTablesWithParameters() {
    return Arrays.asList("DROP TABLE IF EXISTS "+TEST_SCHEMA+".Test, "+TEST_SCHEMA+".Other CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("DROP TABLE "+TEST_SCHEMA+".TEMP_TempTest CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("TRUNCATE TABLE "+TEST_SCHEMA+".Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTempTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTempTableStatements() {
    return Arrays.asList("TRUNCATE TABLE "+TEST_SCHEMA+".TEMP_TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDeleteAllFromTableStatements()
   */
  @Override
  protected List<String> expectedDeleteAllFromTableStatements() {
    return Arrays.asList("DELETE FROM "+TEST_SCHEMA+".Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatement()
   */
  @Override
  protected String expectedParameterisedInsertStatement() {
    return "INSERT INTO "+TEST_SCHEMA+".Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, CAST(:version AS INTEGER), CAST('Escap''d' AS VARCHAR(7)), 7, CAST(:floatField AS DECIMAL(13,2)), 20100405, true, CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithTableInDifferentSchema()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithTableInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, CAST(:version AS INTEGER), CAST('Escap''d' AS VARCHAR(7)), 7, CAST(:floatField AS DECIMAL(13,2)), 20100405, true, CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutoGenerateIdStatement()
   */
  @Override
  protected List<String> expectedAutoGenerateIdStatement() {
    return Arrays.asList(
      "DELETE FROM "+TEST_SCHEMA+".idvalues where name = 'Test'",
      "INSERT INTO "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" (name, "+ID_INCREMENTOR_TABLE_COLUMN_VALUE+") VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM "+TEST_SCHEMA+".Test))",
      "INSERT INTO "+TEST_SCHEMA+".Test (version, stringField, id) SELECT version, stringField, (SELECT COALESCE("+ID_INCREMENTOR_TABLE_COLUMN_VALUE+", 0) FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" WHERE (name = CAST('Test' AS VARCHAR(4)))) + Other.id FROM "+TEST_SCHEMA+".Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" where name = 'Test'",
      "INSERT INTO "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" (name, "+ID_INCREMENTOR_TABLE_COLUMN_VALUE+") VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM "+TEST_SCHEMA+".Test))",
      "INSERT INTO "+TEST_SCHEMA+".Test (stringField, id, version) SELECT stringField, (SELECT COALESCE("+ID_INCREMENTOR_TABLE_COLUMN_VALUE+", 0) FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" WHERE (name = CAST('Test' AS VARCHAR(4)))) + Other.id, 0 AS version FROM "+TEST_SCHEMA+".Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" where name = 'Test'",
      "INSERT INTO "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" (name, "+ID_INCREMENTOR_TABLE_COLUMN_VALUE+") VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM "+TEST_SCHEMA+".Test))",
      "INSERT INTO "+TEST_SCHEMA+".Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (CAST('Escap''d' AS VARCHAR(7)), 7, 11.25, 20100405, true, CAST('X' AS VARCHAR(1)), (SELECT COALESCE("+ID_INCREMENTOR_TABLE_COLUMN_VALUE+", 1) FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsertWithTableInDifferentSchema()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema() {
    return Arrays.asList(
      "DELETE FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" where name = 'Test'",
      "INSERT INTO "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" (name, "+ID_INCREMENTOR_TABLE_COLUMN_VALUE+") VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM MYSCHEMA.Test))",
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (CAST('Escap''d' AS VARCHAR(7)), 7, 11.25, 20100405, true, CAST('X' AS VARCHAR(1)), (SELECT COALESCE("+ID_INCREMENTOR_TABLE_COLUMN_VALUE+", 1) FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithNoColumnValues()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithNoColumnValues() {
    return "INSERT INTO "+TEST_SCHEMA+".Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (CAST(:id AS BIGINT), CAST(:version AS INTEGER), CAST(:stringField AS VARCHAR(3)), CAST(:intField AS INTEGER), CAST(:floatField AS DECIMAL(13,2)), CAST(:dateField AS DATE), CAST(:booleanField AS BIT), CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedEmptyStringInsertStatement()
   */
  @Override
  protected String expectedEmptyStringInsertStatement() {
    return "INSERT INTO "+TEST_SCHEMA+".Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT COALESCE("+ID_INCREMENTOR_TABLE_COLUMN_VALUE+", 1) FROM "+TEST_SCHEMA+"."+ID_VALUES_TABLE+" WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, 0, 0, null, false, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CASE WHEN (taxVariationIndicator = CAST('Y' AS VARCHAR(1))) THEN exposureCustomerNumber ELSE invoicingCustomerNumber END,'') AS test FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(MAX(scheduleStartDate),'') AS test FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT COALESCE(CAST('ABC' AS VARCHAR(3)),'') || COALESCE(CAST(' ' AS VARCHAR(1)),'') || COALESCE(CAST('DEF' AS VARCHAR(3)),'') AS assetDescription FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */
  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT COALESCE(field1,'') || COALESCE(COALESCE(field2,'') || COALESCE(CAST('XYZ' AS VARCHAR(3)),''),'') AS test FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CAST(' ' AS VARCHAR(1)),'') || COALESCE(assetDescriptionLine2,'') AS assetDescription FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CAST('XYZ' AS VARCHAR(3)),'') || COALESCE(assetDescriptionLine2,'') AS assetDescription FROM "+TEST_SCHEMA+".schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIsNull()
   */
  @Override
  protected String expectedIsNull() {
    return "COALESCE(CAST('A' AS VARCHAR(1)), CAST('B' AS VARCHAR(1)))";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBlobLiteral(String)
   */
  @Override
  protected String expectedBlobLiteral(String value) {
    return String.format("X'%s'", value);
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMathsPlus()
   */
  @Override
  protected String expectedMathsPlus() {
    return "1 + 1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMathsMinus()
   */
  @Override
  protected String expectedMathsMinus() {
    return "1 - 1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMathsDivide()
   */
  @Override
  protected String expectedMathsDivide() {
    return "1 / 1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMathsMultiply()
   */
  @Override
  protected String expectedMathsMultiply() {
    return "1 * 1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringCast()
   */
  @Override
  protected String expectedStringCast() {
    return "CAST(value AS VARCHAR(10))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntCast()
   */
  @Override
  protected String expectedBigIntCast() {
    return "CAST(value AS BIGINT)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntFunctionCast()
   */
  @Override
  protected String expectedBigIntFunctionCast() {
    return "CAST(MIN(value) AS BIGINT)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanCast()
   */
  @Override
  protected String expectedBooleanCast() {
    return "CAST(value AS BIT)";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanLiteral(boolean)
   */
  @Override
  protected String expectedBooleanLiteral(boolean value) {
    return value ? "true" : "false";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateCast()
   */
  @Override
  protected String expectedDateCast() {
    return "CAST(value AS DATE)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDecimalCast()
   */
  @Override
  protected String expectedDecimalCast() {
    return "CAST(value AS DECIMAL(10,2))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIntegerCast()
   */
  @Override
  protected String expectedIntegerCast() {
    return "CAST(value AS INTEGER)";
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithUnion()
   */
  @Override
  protected String expectedSelectWithUnion() {
    return "SELECT stringField FROM "+TEST_SCHEMA+".Other UNION SELECT stringField FROM "+TEST_SCHEMA+".Test UNION ALL SELECT stringField FROM "+TEST_SCHEMA+".Alternate ORDER BY stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedLeftPad()
   */
  @Override
  protected String expectedLeftPad() {
    return "SELECT LPAD(stringField, 10, CAST('j' AS VARCHAR(1))) FROM "+TEST_SCHEMA+".Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRightPad()
   */
  @Override
  protected String expectedRightPad() {
    return "SELECT RPAD(stringField, 10, CAST('j' AS VARCHAR(1))) FROM "+TEST_SCHEMA+".Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN blobField_new LONGVARBINARY NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN blobField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN booleanField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN booleanField_new BIT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN stringField_new VARCHAR(6) NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN stringField VARCHAR(6)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN intField_new INTEGER NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN intField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN dateField_new DATE NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN dateField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN floatField_new DECIMAL(6,3) NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN floatField SET NULL",
      "ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN floatField DECIMAL(14,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN bigIntegerField_new BIGINT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN bigIntegerField BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN dateField_new DATE DEFAULT DATE '2010-01-01' NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN dateField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN floatField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableDropColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test DROP COLUMN bigIntegerField");
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN floatField SET NULL",
      "ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN floatField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN floatField_new DECIMAL(6,3) DEFAULT 20.33 NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN bigIntegerField SET DEFAULT 54321",
      "ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN bigIntegerField BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      // dropIndexStatements & addIndexStatements
      "DROP INDEX Test_1",
      "CREATE INDEX Test_1 ON "+TEST_SCHEMA+".Test (intField)",
      // changeColumnStatements
      "ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN intField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("CREATE INDEX indexName ON "+TEST_SCHEMA+".Test (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("CREATE INDEX indexName ON "+TEST_SCHEMA+".Test (id,version)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("CREATE UNIQUE INDEX indexName ON "+TEST_SCHEMA+".Test (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUniqueNullable()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUniqueNullable() {
    return Arrays.asList("CREATE UNIQUE INDEX indexName ON "+TEST_SCHEMA+".Test (stringField,intField,floatField,dateField)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIndexDropStatements()
   */
  @Override
  protected List<String> expectedIndexDropStatements() {
    return Arrays.asList("DROP INDEX indexName");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnMakePrimaryStatements()
   */
  @Override
  protected List<String> expectedAlterColumnMakePrimaryStatements() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD CONSTRAINT Test_PK PRIMARY KEY (id, dateField)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".CompositePrimaryKey ALTER COLUMN secondPrimaryKey VARCHAR(5)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return ImmutableList.of(
      "ALTER TABLE "+TEST_SCHEMA+".CompositePrimaryKey DROP PRIMARY KEY",
      "ALTER TABLE "+TEST_SCHEMA+".CompositePrimaryKey ALTER COLUMN secondPrimaryKey SET NULL",
      "ALTER TABLE "+TEST_SCHEMA+".CompositePrimaryKey ALTER COLUMN secondPrimaryKey VARCHAR(5)",
      "ALTER TABLE "+TEST_SCHEMA+".CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
      "ALTER TABLE "+TEST_SCHEMA+".Test ALTER COLUMN id RENAME TO renamedId"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenameNonPrimaryIndexedColumn()
   */
  @Override
  protected List<String> expectedAlterColumnRenameNonPrimaryIndexedColumn() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Alternate ALTER COLUMN stringField RENAME TO blahField");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Other ALTER COLUMN floatField RENAME TO blahField",
      "ALTER TABLE "+TEST_SCHEMA+".Other ALTER COLUMN blahField SET NULL", "ALTER TABLE "+TEST_SCHEMA+".Other ALTER COLUMN blahField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnChangingLengthAndCase()
   */
  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Other ALTER COLUMN floatField RENAME TO FloatField",
      "ALTER TABLE "+TEST_SCHEMA+".Other ALTER COLUMN FloatField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#varCharCast(java.lang.String)
   */
  @Override
  protected String varCharCast(String value) {
    return String.format("CAST(%s AS VARCHAR(%d))", value, StringUtils.replace(value, "'", "").length());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE "+TEST_SCHEMA+".Test ADD COLUMN stringField_with_default VARCHAR(6) DEFAULT CAST('N' AS VARCHAR(1)) NOT NULL");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdate()
   */
  @Override
  protected List<String> expectedAutonumberUpdate() {
    return Arrays.asList("MERGE INTO "+TEST_SCHEMA+".Autonumber (id, value) SELECT 'TestTable', (SELECT GREATEST((SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM "+TEST_SCHEMA+".TestTable), (SELECT value from Autonumber WHERE name='TestTable'), 1))");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateWithSelectMinimum()
   */
  @Override
  protected String expectedUpdateWithSelectMinimum() {
    String value1 = varCharCast("'S'");
    String value2 = varCharCast("'Y'");
    return "UPDATE " + tableName("Other") + " O SET intField = (SELECT MIN(intField) FROM " + tableName("Test") + " T WHERE ((T.charField = " + stringLiteralPrefix() + value1 + ") AND (T.stringField = O.stringField) AND (T.intField = O.intField))) WHERE (stringField = " + stringLiteralPrefix() + value2 + ")";
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateUsingAliasedDestinationTable()
   */
  @Override
  protected String expectedUpdateUsingAliasedDestinationTable() {
    return "UPDATE " + tableName("FloatingRateRate") + " A SET settlementFrequency = (SELECT settlementFrequency FROM " + tableName("FloatingRateDetail") + " B WHERE (A.floatingRateDetailId = B.id))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewStatements()
   */
  @Override
  protected List<String> expectedCreateViewStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName("Test") + " WHERE (stringField = " + varCharCast("'blah'") + "))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewStatements()
   */
  @Override
  protected List<String> expectedCreateSequenceStatements() {
    return Arrays.asList("CREATE SEQUENCE " + tableName("TestSequence") + " START WITH 1");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewStatements()
   */
  @Override
  protected List<String> expectedCreateTemporarySequenceStatements() {
    return Arrays.asList("CREATE SEQUENCE " + tableName("TestSequence") + " START WITH 1");
  }


  /**
   * @see AbstractSqlDialectTest#expectedCreateSequenceStatementsWithNoStartWith()
   * @return
   */
  @Override
  protected List<String> expectedCreateSequenceStatementsWithNoStartWith() {
    return Arrays.asList("CREATE SEQUENCE " + tableName("TestSequence"));
  }


  /**
   * @see AbstractSqlDialectTest#expectedCreateTemporarySequenceStatementsWithNoStartWith()
   * @return
   */
  @Override
  protected List<String> expectedCreateTemporarySequenceStatementsWithNoStartWith() {
    return Arrays.asList("CREATE SEQUENCE " + tableName("TestSequence"));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedYYYYMMDDToDate()
   */
  @Override
  protected String expectedYYYYMMDDToDate() {
    return "CAST(SUBSTRING(CAST('20100101' AS VARCHAR(8)), 1, 4)||'-'||SUBSTRING(CAST('20100101' AS VARCHAR(8)), 5, 2)||'-'||SUBSTRING(CAST('20100101' AS VARCHAR(8)), 7, 2) AS DATE)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmdd()
   */
  @Override
  protected String expectedDateToYyyymmdd() {
    return "CAST(SUBSTRING(testField, 1, 4)||SUBSTRING(testField, 6, 2)||SUBSTRING(testField, 9, 2) AS DECIMAL(8))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "CAST(SUBSTRING(testField, 1, 4)||SUBSTRING(testField, 6, 2)||SUBSTRING(testField, 9, 2)||SUBSTRING(testField, 12, 2)||SUBSTRING(testField, 15, 2)||SUBSTRING(testField, 18, 2) AS DECIMAL(14))";
  }

  @Override
  protected String expectedClobLiteralCast() {
    return "CAST('CREATE VIEW viewName AS (SELECT tableField1, tableField2, tableField3, tableField4, tableField5, tableField6, tableField7, tableField8, tableField9, tableField10, tableField11, tableField12, tableField13, tableField14, tableField15, tableField16, tableField17, tableField18, tableField19, tableField20, tableField21, tableField22, tableField23, tableField24, tableField25, tableField26, tableField27, tableField28, tableField29, tableField30 FROM table INNER JOIN table2 ON (table1.tableField1 = table2 = tableField1));' AS VARCHAR(519))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "CURRENT_TIMESTAMP()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatements()
   */
  @Override
  protected List<String> expectedDropViewStatements() {
    return Arrays.asList("DROP VIEW " + tableName("TestView") + " IF EXISTS CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringLiteralToIntegerCast()
   */
  @Override
  protected String expectedStringLiteralToIntegerCast() {
    return "CAST(" + varCharCast("'1234567890'") + " AS INTEGER)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSubstring()
   */
  @Override
  protected String expectedSubstring() {
    return "SELECT SUBSTRING(field1, 1, 3) FROM " + tableName("schedule");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdateForNonIdColumn()
   */
  @Override
  protected List<String> expectedAutonumberUpdateForNonIdColumn() {
    return Arrays.asList("MERGE INTO "+TEST_SCHEMA+".Autonumber (id, value) SELECT 'TestTable', (SELECT GREATEST((SELECT COALESCE(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TestTable), (SELECT value from Autonumber WHERE name='TestTable'), 1))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringFunctionCast()
   */
  @Override
  protected String expectedStringFunctionCast() {
    return "CAST(MIN(field) AS VARCHAR(8))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDaysBetween()
   */
  @Override
  protected String expectedDaysBetween() {
    return "SELECT DATEDIFF('DAY',dateOne, dateTwo) FROM "+TEST_SCHEMA+".MyTable";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "MERGE INTO "+TEST_SCHEMA+".foo"
            + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM "+TEST_SCHEMA+".somewhere) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
            + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
     return "MERGE INTO "+TEST_SCHEMA+".foo"
            + " USING (SELECT somewhere.newId AS id, join.joinBar AS bar FROM "+TEST_SCHEMA+".somewhere INNER JOIN "+TEST_SCHEMA+".join ON (somewhere.newId = join.joinId)) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
            + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "MERGE INTO "+TEST_SCHEMA+".foo"
            + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM MYSCHEMA.somewhere) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
            + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeTargetInDifferentSchema()
   */
  @Override
  protected String expectedMergeTargetInDifferentSchema() {

    return "MERGE INTO MYSCHEMA.foo"
            + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM "+TEST_SCHEMA+".somewhere) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
            + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "MERGE INTO "+TEST_SCHEMA+".foo"
            + " USING (SELECT somewhere.newId AS id FROM "+TEST_SCHEMA+".somewhere) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN NOT MATCHED THEN INSERT (id) VALUES (xmergesource.id)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeWithUpdateExpressions()
   */
  @Override
  protected String expectedMergeWithUpdateExpressions() {
    return "MERGE INTO "+TEST_SCHEMA+".foo"
            + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM "+TEST_SCHEMA+".somewhere) xmergesource"
            + " ON (foo.id = xmergesource.id)"
            + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar + foo.bar"
            + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddDays()
   */
  @Override
  protected String expectedAddDays() {
    return "DATEADD('DAY', -20, testField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddMonths()
   */
  @Override
  protected String expectedAddMonths() {
    return "DATEADD('MONTH', -3, testField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromSimpleKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromSimpleKeyStatements() {
    return Collections.singletonList("ALTER TABLE "+TEST_SCHEMA+".Test DROP COLUMN id");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of(
      "ALTER TABLE "+TEST_SCHEMA+".Test DROP PRIMARY KEY",
      "ALTER TABLE "+TEST_SCHEMA+".Test RENAME TO Renamed",
      "ALTER TABLE "+TEST_SCHEMA+".Renamed ADD CONSTRAINT Renamed_PK PRIMARY KEY (id)"
    );
  }


  /**
   * @return the expected statements for renaming a table with a long name.
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of(
      "ALTER TABLE "+TEST_SCHEMA+".123456789012345678901234567890X DROP PRIMARY KEY",
      "ALTER TABLE "+TEST_SCHEMA+".123456789012345678901234567890X RENAME TO Blah",
      "ALTER TABLE "+TEST_SCHEMA+".Blah ADD CONSTRAINT Blah_PK PRIMARY KEY (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of("ALTER INDEX "+TEST_SCHEMA+".Test_1 RENAME TO Test_2");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameTempIndexStatements() {
    return ImmutableList.of("ALTER INDEX "+TEST_SCHEMA+".TempTest_1 RENAME TO TempTest_2");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomString()
   */
  @Override
  protected String expectedRandomString() {
    return "SUBSTRING(REPLACE(RANDOM_UUID(),'-'), 1, 10)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectLiteralWithWhereClauseString()
   */
  @Override
  protected String expectedSelectLiteralWithWhereClauseString() {
    return "SELECT CAST('LITERAL' AS VARCHAR(7)) FROM dual WHERE (CAST('ONE' AS VARCHAR(3)) = CAST('ONE' AS VARCHAR(3)))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddTableFromStatements()
   */
  @Override
  protected List<String> expectedAddTableFromStatements() {
    return ImmutableList.of(
      "CREATE TABLE "+TEST_SCHEMA+".SomeTable (someField VARCHAR(3) NOT NULL, otherField DECIMAL(3,0) NOT NULL, CONSTRAINT SomeTable_PK PRIMARY KEY (someField))",
      "CREATE INDEX SomeTable_1 ON "+TEST_SCHEMA+".SomeTable (otherField)",
      "INSERT INTO "+TEST_SCHEMA+".SomeTable SELECT someField, otherField FROM "+TEST_SCHEMA+".OtherTable"
    );
  }


  @Override
  protected List<String> expectedReplaceTableFromStatements() {
    return ImmutableList.of(
        "CREATE TABLE "+TEST_SCHEMA+".tmp_SomeTable (someField VARCHAR(3) NOT NULL, otherField DECIMAL(3,0) NOT NULL, thirdField DECIMAL(5,0) NOT NULL, CONSTRAINT tmp_SomeTable_PK PRIMARY KEY (someField))",
        "INSERT INTO "+TEST_SCHEMA+".tmp_SomeTable SELECT someField, otherField, CAST(thirdField AS DECIMAL(5,0)) AS thirdField FROM "+TEST_SCHEMA+".OtherTable",
        "DROP TABLE "+TEST_SCHEMA+".SomeTable CASCADE",
        "ALTER TABLE "+TEST_SCHEMA+".tmp_SomeTable DROP PRIMARY KEY",
        "ALTER TABLE "+TEST_SCHEMA+".tmp_SomeTable RENAME TO SomeTable",
        "ALTER TABLE "+TEST_SCHEMA+".SomeTable ADD CONSTRAINT SomeTable_PK PRIMARY KEY (someField)",
        "CREATE INDEX SomeTable_1 ON "+TEST_SCHEMA+".SomeTable (otherField)"
    );
  }


  @Override
  protected List<String> expectedReplaceTableWithAutonumber() {
    return ImmutableList.of(
        "CREATE TABLE "+TEST_SCHEMA+".tmp_SomeTable (someField VARCHAR(3) NOT NULL, otherField DECIMAL(3,0) AUTO_INCREMENT(1) COMMENT 'AUTONUMSTART:[1]', thirdField DECIMAL(5,0) NOT NULL, CONSTRAINT tmp_SomeTable_PK PRIMARY KEY (someField))",
        "INSERT INTO "+TEST_SCHEMA+".tmp_SomeTable SELECT someField, otherField, CAST(thirdField AS DECIMAL(5,0)) AS thirdField FROM "+TEST_SCHEMA+".OtherTable",
        "DROP TABLE "+TEST_SCHEMA+".SomeTable CASCADE",
        "ALTER TABLE "+TEST_SCHEMA+".tmp_SomeTable DROP PRIMARY KEY",
        "ALTER TABLE "+TEST_SCHEMA+".tmp_SomeTable RENAME TO SomeTable",
        "ALTER TABLE "+TEST_SCHEMA+".SomeTable ADD CONSTRAINT SomeTable_PK PRIMARY KEY (someField)",
        "CREATE INDEX SomeTable_1 ON "+TEST_SCHEMA+".SomeTable (otherField)"
    );
  }


  /**
   * No hints are supported.
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT * FROM SCHEMA2.Foo INNER JOIN "+TEST_SCHEMA+".Bar ON (a = b) LEFT OUTER JOIN "+TEST_SCHEMA+".Fo ON (a = b) INNER JOIN "+TEST_SCHEMA+".Fum Fumble ON (a = b) ORDER BY a";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAnalyseTableSql()
   */
  @Override
  protected Collection<String> expectedAnalyseTableSql() {
    return SqlDialect.NO_STATEMENTS;
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndWhere(String value) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE (Test.stringField = " + stringLiteralPrefix() + value + ") LIMIT 1000";
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndComplexWhere(String value1, String value2) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ((Test.stringField = " + stringLiteralPrefix() + value1 + ") OR (Test.stringField = " + stringLiteralPrefix() + value2 + ")) LIMIT 1000";
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitWithoutWhere() {
    return "DELETE FROM " + tableName(TEST_TABLE) + " LIMIT 1000";
  }


  /**
   * @return The expected SQL for retrieving the row number
   */
  @Override
  protected String expectedRowNumber() {
    return "ROW_NUMBER() OVER()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#tableName(java.lang.String)
   */
  @Override
  protected String tableName(String baseName) {
    return TEST_SCHEMA+"." + baseName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewOverUnionSelectStatements()
   */
  @Override
  protected List<String> expectedCreateViewOverUnionSelectStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "CAST('blah' AS VARCHAR(4))) UNION ALL SELECT stringField FROM " + tableName(OTHER_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "CAST('blah' AS VARCHAR(4))))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExcept()
   */
  @Override
  protected String expectedSelectWithExcept() {
    return null;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithDbLink()
   */
  @Override
  protected String expectedSelectWithDbLink() {
    return null;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExceptAndDbLinkFormer()
   */
  @Override
  protected String expectedSelectWithExceptAndDbLinkFormer() {
    return null;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExceptAndDbLinkLatter()
   */
  @Override
  protected String expectedSelectWithExceptAndDbLinkLatter() {
    return null;
  }


  /**
   * @see AbstractSqlDialectTest#expectedNextValForSequence()
   * @return
   */
  @Override
  protected String expectedNextValForSequence() {
    return "SELECT NEXT VALUE FOR TestSequence FROM dual";
  }


  /**
   * @see AbstractSqlDialectTest#expectedCurrValForSequence()
   */
  @Override
  protected String expectedCurrValForSequence() {
    return "SELECT CURRENT VALUE FOR TestSequence FROM dual";
  }


  @Override
  protected String expectedPortableStatement() {
    return "UPDATE TESTSCHEMA.Table SET field = BTRIM(field, CAST('2' AS VARCHAR(1)), CAST('B' AS VARCHAR(1)))";
  }


  /**
   * @return The expected value for the force serial import setting.
   */
  @Override
  protected boolean expectedForceSerialImport() {
    return true;
  }
}
