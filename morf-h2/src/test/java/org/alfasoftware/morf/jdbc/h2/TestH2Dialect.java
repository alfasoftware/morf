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
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;

/**
 * Tests SQL statements generated for MySQL.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestH2Dialect extends AbstractSqlDialectTest {

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new H2Dialect();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE Test (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), blobField LONGVARBINARY, bigIntegerField BIGINT DEFAULT 12345, clobField NCLOB, CONSTRAINT Test_PK PRIMARY KEY (id))",
          "CREATE UNIQUE INDEX Test_NK ON Test (stringField)",
          "CREATE INDEX Test_1 ON Test (intField,floatField)",
          "CREATE TABLE Alternate (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), CONSTRAINT Alternate_PK PRIMARY KEY (id))",
          "CREATE INDEX Alternate_1 ON Alternate (stringField)",
          "CREATE TABLE NonNull (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BIT NOT NULL, dateField DATE NOT NULL, blobField LONGVARBINARY NOT NULL, CONSTRAINT NonNull_PK PRIMARY KEY (id))",
          "CREATE TABLE CompositePrimaryKey (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, secondPrimaryKey VARCHAR(3) NOT NULL, CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id, secondPrimaryKey))",
          "CREATE TABLE AutoNumber (intField BIGINT AUTO_INCREMENT(5) COMMENT 'AUTONUMSTART:[5]', CONSTRAINT AutoNumber_PK PRIMARY KEY (intField))"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTemporaryTableStatements()
   */
  @Override
  protected List<String> expectedCreateTemporaryTableStatements() {
    return Arrays
        .asList(
          "CREATE TEMPORARY TABLE TEMP_TempTest (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), blobField LONGVARBINARY, bigIntegerField BIGINT DEFAULT 12345, clobField NCLOB, CONSTRAINT TEMP_TempTest_PK PRIMARY KEY (id))",
          "CREATE UNIQUE INDEX TempTest_NK ON TEMP_TempTest (stringField)",
          "CREATE INDEX TempTest_1 ON TEMP_TempTest (intField,floatField)",
          "CREATE TEMPORARY TABLE TEMP_TempAlternate (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), CONSTRAINT TEMP_TempAlternate_PK PRIMARY KEY (id))",
          "CREATE INDEX TempAlternate_1 ON TEMP_TempAlternate (stringField)",
          "CREATE TEMPORARY TABLE TEMP_TempNonNull (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BIT NOT NULL, dateField DATE NOT NULL, blobField LONGVARBINARY NOT NULL, CONSTRAINT TEMP_TempNonNull_PK PRIMARY KEY (id))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays
        .asList("CREATE TABLE "
            + TABLE_WITH_VERY_LONG_NAME
            + " (id BIGINT NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3), intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BIT, charField VARCHAR(1), CONSTRAINT "
            + TABLE_WITH_VERY_LONG_NAME + "_PK PRIMARY KEY (id))",
            "CREATE UNIQUE INDEX Test_NK ON tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation (stringField)",
            "CREATE INDEX Test_1 ON tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation (intField,floatField)"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTableStatements()
   */
  @Override
  protected List<String> expectedDropTableStatements() {
    return Arrays.asList("drop table Test cascade");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("drop table TEMP_TempTest cascade");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("truncate table Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTempTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTempTableStatements() {
    return Arrays.asList("truncate table TEMP_TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDeleteAllFromTableStatements()
   */
  @Override
  protected List<String> expectedDeleteAllFromTableStatements() {
    return Arrays.asList("delete from Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatement()
   */
  @Override
  protected String expectedParameterisedInsertStatement() {
    return "INSERT INTO Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, CAST(:version AS INTEGER), CAST('Escap''d' AS VARCHAR(7)), 7, CAST(:floatField AS DECIMAL(13,2)), 20100405, 1, CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithTableInDifferentSchema()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithTableInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, CAST(:version AS INTEGER), CAST('Escap''d' AS VARCHAR(7)), 7, CAST(:floatField AS DECIMAL(13,2)), 20100405, 1, CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutoGenerateIdStatement()
   */
  @Override
  protected List<String> expectedAutoGenerateIdStatement() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM Test))",
      "INSERT INTO Test (version, stringField, id) SELECT version, stringField, (SELECT COALESCE(value, 0)  FROM idvalues WHERE (name = CAST('Test' AS VARCHAR(4)))) + Other.id FROM Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM Test))",
      "INSERT INTO Test (stringField, id, version) SELECT stringField, (SELECT COALESCE(value, 0)  FROM idvalues WHERE (name = CAST('Test' AS VARCHAR(4)))) + Other.id, 0 AS version FROM Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM Test))",
      "INSERT INTO Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (CAST('Escap''d' AS VARCHAR(7)), 7, 11.25, 20100405, 1, CAST('X' AS VARCHAR(1)), (SELECT COALESCE(value, 1)  FROM idvalues WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsertWithTableInDifferentSchema()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM MYSCHEMA.Test))",
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (CAST('Escap''d' AS VARCHAR(7)), 7, 11.25, 20100405, 1, CAST('X' AS VARCHAR(1)), (SELECT COALESCE(value, 1)  FROM idvalues WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithNoColumnValues()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithNoColumnValues() {
    return "INSERT INTO Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (CAST(:id AS BIGINT), CAST(:version AS INTEGER), CAST(:stringField AS VARCHAR(3)), CAST(:intField AS DECIMAL(8,0)), CAST(:floatField AS DECIMAL(13,2)), CAST(:dateField AS DATE), CAST(:booleanField AS BIT), CAST(:charField AS VARCHAR(1)), CAST(:blobField AS LONGVARBINARY), CAST(:bigIntegerField AS BIGINT), CAST(:clobField AS NCLOB))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedEmptyStringInsertStatement()
   */
  @Override
  protected String expectedEmptyStringInsertStatement() {
    return "INSERT INTO Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT COALESCE(value, 1)  FROM idvalues WHERE (name = CAST('Test' AS VARCHAR(4)))), 0, 0, 0, null, 0, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CASE WHEN (taxVariationIndicator = CAST('Y' AS VARCHAR(1))) THEN exposureCustomerNumber ELSE invoicingCustomerNumber END,'') AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(MAX(scheduleStartDate),'') AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT COALESCE(CAST('ABC' AS VARCHAR(3)),'') || COALESCE(CAST(' ' AS VARCHAR(1)),'') || COALESCE(CAST('DEF' AS VARCHAR(3)),'') AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */
  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT COALESCE(field1,'') || COALESCE(COALESCE(field2,'') || COALESCE(CAST('XYZ' AS VARCHAR(3)),''),'') AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CAST(' ' AS VARCHAR(1)),'') || COALESCE(assetDescriptionLine2,'') AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT COALESCE(assetDescriptionLine1,'') || COALESCE(CAST('XYZ' AS VARCHAR(3)),'') || COALESCE(assetDescriptionLine2,'') AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIsNull()
   */
  @Override
  protected String expectedIsNull() {
    return "COALESCE(CAST('A' AS VARCHAR(1)), CAST('B' AS VARCHAR(1))) ";
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
    return "SELECT stringField FROM Other UNION SELECT stringField FROM Test UNION ALL SELECT stringField FROM Alternate ORDER BY stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedLeftPad()
   */
  @Override
  protected String expectedLeftPad() {
    return "SELECT LPAD(stringField, 10, CAST('j' AS VARCHAR(1))) FROM Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN blobField_new LONGVARBINARY NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN blobField LONGVARBINARY");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN booleanField BIT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN booleanField_new BIT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN stringField_new VARCHAR(6) NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN stringField VARCHAR(6)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN intField_new INTEGER NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN intField DECIMAL(10,0)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN dateField_new DATE NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN dateField DATE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN floatField_new DECIMAL(6,3) NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN floatField SET NULL",
      "ALTER TABLE Test ALTER COLUMN floatField DECIMAL(14,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN bigIntegerField_new BIGINT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN bigIntegerField BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN dateField_new DATE DEFAULT DATE '2010-01-01' NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN dateField SET NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN floatField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN floatField SET NULL",
      "ALTER TABLE Test ALTER COLUMN floatField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE Test ADD COLUMN floatField_new DECIMAL(6,3) DEFAULT 20.33 NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE Test ALTER COLUMN bigIntegerField SET DEFAULT 54321",
      "ALTER TABLE Test ALTER COLUMN bigIntegerField BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      // dropIndexStatements & addIndexStatements
      "DROP INDEX Test_1",
      "CREATE INDEX Test_1 ON Test (intField)",
      // changeColumnStatements
      "ALTER TABLE Test ALTER COLUMN intField DECIMAL(11,0)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("CREATE INDEX indexName ON Test (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("CREATE INDEX indexName ON Test (id,version)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("CREATE UNIQUE INDEX indexName ON Test (id)");
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
    return Arrays.asList("ALTER TABLE Test ADD CONSTRAINT Test_PK PRIMARY KEY (id, dateField)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList("ALTER TABLE CompositePrimaryKey ALTER COLUMN secondPrimaryKey VARCHAR(5)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return ImmutableList.of(
      "ALTER TABLE CompositePrimaryKey DROP CONSTRAINT CompositePrimaryKey_PK",
      "ALTER TABLE CompositePrimaryKey ALTER COLUMN secondPrimaryKey SET NULL",
      "ALTER TABLE CompositePrimaryKey ALTER COLUMN secondPrimaryKey VARCHAR(5)",
      "ALTER TABLE CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
      "ALTER TABLE Test ALTER COLUMN id RENAME TO renamedId",
      "ALTER TABLE Test ALTER COLUMN renamedId BIGINT"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList("ALTER TABLE Other ALTER COLUMN floatField RENAME TO blahField",
      "ALTER TABLE Other ALTER COLUMN blahField SET NULL", "ALTER TABLE Other ALTER COLUMN blahField DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnChangingLengthAndCase()
   */
  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("ALTER TABLE Other ALTER COLUMN floatField RENAME TO FloatField",
      "ALTER TABLE Other ALTER COLUMN FloatField DECIMAL(20,3)");
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
    return Arrays.asList("ALTER TABLE Test ADD COLUMN stringField_with_default VARCHAR(6) DEFAULT CAST('N' AS VARCHAR(1)) NOT NULL");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdate()
   */
  @Override
  protected List<String> expectedAutonumberUpdate() {
    return Arrays.asList("MERGE INTO Autonumber (id, value) SELECT 'TestTable', (SELECT GREATEST((SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM TestTable), (SELECT value from Autonumber WHERE name='TestTable'), 1))");
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewStatement()
   */
  @Override
  protected String expectedCreateViewStatement() {
    return "CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName("Test") + " WHERE (stringField = " + varCharCast("'blah'") + "))";
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
    return "CAST(SUBSTRING(testField, 1, 4)||SUBSTRING(testField, 6, 2)||SUBSTRING(testField, 9, 2) AS INT)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "CAST(SUBSTRING(testField, 1, 4)||SUBSTRING(testField, 6, 2)||SUBSTRING(testField, 9, 2)||SUBSTRING(testField, 12, 2)||SUBSTRING(testField, 15, 2)||SUBSTRING(testField, 18, 2) AS BIGINT)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "CURRENT_TIMESTAMP()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatement()
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
    return Arrays.asList("MERGE INTO Autonumber (id, value) SELECT 'TestTable', (SELECT GREATEST((SELECT COALESCE(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TestTable), (SELECT value from Autonumber WHERE name='TestTable'), 1))");
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
    return "SELECT DATEDIFF('DAY',dateOne, dateTwo) FROM MyTable";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "MERGE INTO foo"
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
    return "MERGE INTO foo"
        + " USING (SELECT somewhere.newId AS id, join.joinBar AS bar FROM somewhere INNER JOIN join ON (somewhere.newId = join.joinId)) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "MERGE INTO foo"
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
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "MERGE INTO foo"
        + " USING (SELECT somewhere.newId AS id FROM somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN NOT MATCHED THEN INSERT (id) VALUES (xmergesource.id)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeWithUpdateExpressions()
   */
  @Override
  protected String expectedMergeWithUpdateExpressions() {
    return "MERGE INTO foo"
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere) xmergesource"
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
    return Collections.singletonList("ALTER TABLE Test DROP COLUMN id");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of(
      "ALTER TABLE Test DROP CONSTRAINT Test_PK",
      "ALTER TABLE Test RENAME TO Renamed",
      "ALTER TABLE Renamed ADD CONSTRAINT Renamed_PK PRIMARY KEY (id)"
    );
  }


  /**
   * @return the expected statements for renaming a table with a long name.
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of(
      "ALTER TABLE 123456789012345678901234567890XXX DROP CONSTRAINT 123456789012345678901234567890XXX_PK",
      "ALTER TABLE 123456789012345678901234567890XXX RENAME TO Blah",
      "ALTER TABLE Blah ADD CONSTRAINT Blah_PK PRIMARY KEY (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of("ALTER INDEX TempTest_1 RENAME TO TempTest_2");
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
      "CREATE TABLE SomeTable (someField VARCHAR(3) NOT NULL, otherField DECIMAL(3,0) NOT NULL, CONSTRAINT SomeTable_PK PRIMARY KEY (someField))",
      "CREATE INDEX SomeTable_1 ON SomeTable (otherField)",
      "INSERT INTO SomeTable SELECT someField, otherField FROM OtherTable"
    );
  }


  /**
   * No hints are supported.
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT * FROM SCHEMA2.Foo INNER JOIN Bar ON (a = b) LEFT OUTER JOIN Fo ON (a = b) INNER JOIN Fum Fumble ON (a = b) ORDER BY a";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#supportsWindowFunctions()
   */
  @Override
  protected boolean supportsWindowFunctions() {
    return false;
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
}
