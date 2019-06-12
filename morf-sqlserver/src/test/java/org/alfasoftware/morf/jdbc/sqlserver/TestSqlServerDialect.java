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

package org.alfasoftware.morf.jdbc.sqlserver;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.apache.commons.lang.StringUtils;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;

/**
 * Test that {@link SqlServerDialect} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestSqlServerDialect extends AbstractSqlDialectTest {

  @SuppressWarnings({"unchecked","rawtypes"})
  private final ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass((Class<List<String>>)(Class)List.class);

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new SqlServerDialect("TESTSCHEMA");
  }


  /**
   * This test covers table and index deployments to the default tablespace and
   * an alternate table space.
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE TESTSCHEMA.Test ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT Test_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS, [intField] NUMERIC(8,0), [floatField] NUMERIC(13,2) NOT NULL, [dateField] DATE, [booleanField] BIT, [charField] NVARCHAR(1) COLLATE SQL_Latin1_General_CP1_CS_AS, [blobField] IMAGE, [bigIntegerField] BIGINT CONSTRAINT Test_bigIntegerField_DF DEFAULT 12345, [clobField] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CS_AS, CONSTRAINT [Test_PK] PRIMARY KEY ([id]))",
          "CREATE UNIQUE NONCLUSTERED INDEX Test_NK ON TESTSCHEMA.Test ([stringField])",
          "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField], [floatField])",
          "CREATE TABLE TESTSCHEMA.Alternate ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT Alternate_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS, CONSTRAINT [Alternate_PK] PRIMARY KEY ([id]))",
          "CREATE INDEX Alternate_1 ON TESTSCHEMA.Alternate ([stringField])",
          "CREATE TABLE TESTSCHEMA.NonNull ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT NonNull_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, [intField] NUMERIC(8,0) NOT NULL, [booleanField] BIT NOT NULL, [dateField] DATE NOT NULL, [blobField] IMAGE NOT NULL, CONSTRAINT [NonNull_PK] PRIMARY KEY ([id]))",
          "CREATE TABLE TESTSCHEMA.CompositePrimaryKey ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT CompositePrimaryKey_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, [secondPrimaryKey] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, CONSTRAINT [CompositePrimaryKey_PK] PRIMARY KEY ([id], [secondPrimaryKey]))",
          "CREATE TABLE TESTSCHEMA.AutoNumber ([intField] BIGINT NOT NULL IDENTITY(5, 1), CONSTRAINT [AutoNumber_PK] PRIMARY KEY ([intField]))"
            );
  }


  /**
   * This test covers table and index deployments to the default tablespace and
   * an alternate table space.
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTemporaryTableStatements()
   */
  @Override
  protected List<String> expectedCreateTemporaryTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE TESTSCHEMA.#TempTest ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT #TempTest_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS, [intField] NUMERIC(8,0), [floatField] NUMERIC(13,2) NOT NULL, [dateField] DATE, [booleanField] BIT, [charField] NVARCHAR(1) COLLATE SQL_Latin1_General_CP1_CS_AS, [blobField] IMAGE, [bigIntegerField] BIGINT CONSTRAINT #TempTest_bigIntegerField_DF DEFAULT 12345, [clobField] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CS_AS, CONSTRAINT [TempTest_PK] PRIMARY KEY ([id]))",
          "CREATE UNIQUE NONCLUSTERED INDEX TempTest_NK ON TESTSCHEMA.#TempTest ([stringField])",
          "CREATE INDEX TempTest_1 ON TESTSCHEMA.#TempTest ([intField], [floatField])",
          "CREATE TABLE TESTSCHEMA.#TempAlternate ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT #TempAlternate_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS, CONSTRAINT [TempAlternate_PK] PRIMARY KEY ([id]))",
          "CREATE INDEX TempAlternate_1 ON TESTSCHEMA.#TempAlternate ([stringField])",
            "CREATE TABLE TESTSCHEMA.#TempNonNull ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT #TempNonNull_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, [intField] NUMERIC(8,0) NOT NULL, [booleanField] BIT NOT NULL, [dateField] DATE NOT NULL, [blobField] IMAGE NOT NULL, CONSTRAINT [TempNonNull_PK] PRIMARY KEY ([id]))");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays.asList(
      "CREATE TABLE TESTSCHEMA.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation ([id] BIGINT NOT NULL, [version] INTEGER CONSTRAINT tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation_version_DF DEFAULT 0, [stringField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS, [intField] NUMERIC(8,0), [floatField] NUMERIC(13,2) NOT NULL, [dateField] DATE, [booleanField] BIT, [charField] NVARCHAR(1) COLLATE SQL_Latin1_General_CP1_CS_AS, CONSTRAINT [tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation_PK] PRIMARY KEY ([id]))",
      "CREATE UNIQUE NONCLUSTERED INDEX Test_NK ON TESTSCHEMA.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation ([stringField])",
      "CREATE INDEX Test_1 ON TESTSCHEMA.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation ([intField], [floatField])"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTableStatements()
   */
  @Override
  protected List<String> expectedDropTableStatements() {
    return Arrays.asList("DROP TABLE TESTSCHEMA.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("DROP TABLE TESTSCHEMA.#TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("TRUNCATE TABLE TESTSCHEMA.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTempTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTempTableStatements() {
    return Arrays.asList("TRUNCATE TABLE TESTSCHEMA.#TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDeleteAllFromTableStatements()
   */
  @Override
  protected List<String> expectedDeleteAllFromTableStatements() {
    return Arrays.asList("DELETE FROM TESTSCHEMA.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatement()
   */
  @Override
  protected String expectedParameterisedInsertStatement() {
    return "INSERT INTO TESTSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, 'Escap''d', 7, :floatField, 20100405, 1, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithTableInDifferentSchema()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithTableInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, 'Escap''d', 7, :floatField, 20100405, 1, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutoGenerateIdStatement()
   */
  @Override
  protected List<String> expectedAutoGenerateIdStatement() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT ISNULL(MAX(id) + 1, 1)  AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (version, stringField, id) SELECT version, stringField, (SELECT ISNULL(value, 0)  FROM TESTSCHEMA.idvalues WHERE (name = 'Test')) + Other.id FROM TESTSCHEMA.Other"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT ISNULL(MAX(id) + 1, 1)  AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (stringField, id, version) SELECT stringField, (SELECT ISNULL(value, 0)  FROM TESTSCHEMA.idvalues WHERE (name = 'Test')) + Other.id, 0 AS version FROM TESTSCHEMA.Other"
        );
  }


  private List<String> expectedPreInsertStatements() {
    return ImmutableList.of(
      "SET IDENTITY_INSERT TESTSCHEMA.AutoNumber ON"
        );
  }




  private void verifyPostInsertStatements(List<String> executedStatements) {
    assertThat(executedStatements,contains(
      "SET IDENTITY_INSERT TESTSCHEMA.AutoNumber OFF",

      "IF EXISTS (SELECT 1 FROM TESTSCHEMA.AutoNumber)\n" +
      "BEGIN\n" +
      "  DBCC CHECKIDENT (\"TESTSCHEMA.AutoNumber\", RESEED, 4)\n" +
      "  DBCC CHECKIDENT (\"TESTSCHEMA.AutoNumber\", RESEED)\n" +
      "END\n" +
      "ELSE\n" +
      "BEGIN\n" +
      "  DBCC CHECKIDENT (\"TESTSCHEMA.AutoNumber\", RESEED, 5)\n" +
      "END"
    ));
  }



  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyPostInsertStatementsInsertingUnderAutonumLimit(org.alfasoftware.morf.jdbc.SqlScriptExecutor, com.mysql.jdbc.Connection)
   */
  @Override
  protected void verifyPostInsertStatementsInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor, Connection connection) {
    verify(sqlScriptExecutor).execute(listCaptor.capture(),eq(connection));
    verifyPostInsertStatements(listCaptor.getValue());
    verifyNoMoreInteractions(sqlScriptExecutor);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyPostInsertStatementsNotInsertingUnderAutonumLimit(org.alfasoftware.morf.jdbc.SqlScriptExecutor, com.mysql.jdbc.Connection)
   */
  @Override
  protected void verifyPostInsertStatementsNotInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor, Connection connection) {
    verify(sqlScriptExecutor).execute(listCaptor.capture(),eq(connection));
    verifyPostInsertStatements(listCaptor.getValue());
    verifyNoMoreInteractions(sqlScriptExecutor);
  }


  /**
   * @return The expected SQL statements to be run prior to insert for the test database table.
   */
  @Override
  protected List<String> expectedPreInsertStatementsInsertingUnderAutonumLimit() {
    return expectedPreInsertStatements();
  }


  /**
   * @return The expected SQL statements to be run prior to insert for the test database table.
   */
  @Override
  protected List<String> expectedPreInsertStatementsNotInsertingUnderAutonumLimit() {
    return expectedPreInsertStatements();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#tableName(java.lang.String)
   */
  @Override
  protected String tableName(String baseName) {
    return "TESTSCHEMA." + baseName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT ISNULL(MAX(id) + 1, 1)  AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, 1, 'X', (SELECT ISNULL(value, 1)  FROM TESTSCHEMA.idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsertWithTableInDifferentSchema()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT ISNULL(MAX(id) + 1, 1)  AS CurrentValue FROM MYSCHEMA.Test))",
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, 1, 'X', (SELECT ISNULL(value, 1)  FROM TESTSCHEMA.idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithNoColumnValues()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithNoColumnValues() {
    return "INSERT INTO TESTSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (:id, :version, :stringField, :intField, :floatField, :dateField, :booleanField, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedEmptyStringInsertStatement()
   */
  @Override
  protected String expectedEmptyStringInsertStatement() {
    return "INSERT INTO TESTSCHEMA.Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT ISNULL(value, 1)  FROM TESTSCHEMA.idvalues WHERE (name = 'Test')), 0, 0, 0, null, 0, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT ISNULL(assetDescriptionLine1,'') + ISNULL(' ','') + ISNULL(assetDescriptionLine2,'') AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT ISNULL(assetDescriptionLine1,'') + ISNULL('XYZ','') + ISNULL(assetDescriptionLine2,'') AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT ISNULL(assetDescriptionLine1,'') + ISNULL(CASE WHEN (taxVariationIndicator = 'Y') THEN exposureCustomerNumber ELSE invoicingCustomerNumber END,'') AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT ISNULL(assetDescriptionLine1,'') + ISNULL(MAX(scheduleStartDate),'') AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT ISNULL('ABC','') + ISNULL(' ','') + ISNULL('DEF','') AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */
  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT ISNULL(field1,'') + ISNULL(ISNULL(field2,'') + ISNULL('XYZ',''),'') AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIsNull()
   */
  @Override
  protected String expectedIsNull() {
    return "ISNULL('A', 'B') ";
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
    return "CAST(value AS NVARCHAR(10)) COLLATE SQL_Latin1_General_CP1_CS_AS";
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
    return "CAST(value AS NUMERIC(10,2))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIntegerCast()
   */
  @Override
  protected String expectedIntegerCast() {
    return "CAST(value AS INTEGER)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringLiteralToIntegerCast()
   */
  @Override
  protected String expectedStringLiteralToIntegerCast() {
    return "CAST(" + stringLiteralPrefix() + "'1234567890' AS INTEGER)";
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithUnion()
   */
  @Override
  protected String expectedSelectWithUnion() {
    return "SELECT stringField FROM TESTSCHEMA.Other UNION SELECT stringField FROM TESTSCHEMA.Test UNION ALL SELECT stringField FROM TESTSCHEMA.Alternate ORDER BY stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD stringField_new NVARCHAR(6) COLLATE SQL_Latin1_General_CP1_CS_AS");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList("DROP INDEX Test_NK ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN stringField NVARCHAR(6) COLLATE SQL_Latin1_General_CP1_CS_AS",
        "CREATE UNIQUE NONCLUSTERED INDEX Test_NK ON TESTSCHEMA.Test ([stringField])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD floatField_new NUMERIC(6,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList("DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN floatField NUMERIC(14,3)",
        "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField], [floatField])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD bigIntegerField_new BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList(
      SqlServerDialect.dropDefaultForColumnSql.replace("{table}", "Test").replace("{column}", "bigIntegerField"),
        "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN bigIntegerField BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD blobField_new IMAGE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList(
        "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN blobField IMAGE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD floatField_new NUMERIC(6,3) CONSTRAINT Test_floatField_new_DF DEFAULT 20.33 WITH VALUES");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList(
      SqlServerDialect.dropDefaultForColumnSql.replace("{table}", "Test").replace("{column}", "bigIntegerField"),
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN bigIntegerField BIGINT CONSTRAINT Test_bigIntegerField_DF DEFAULT 54321 WITH VALUES"
        );
  }


  // *********************************************************************************************

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList(
        "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN booleanField BIT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD booleanField_new BIT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD intField_new INTEGER");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList(
      "DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN intField NUMERIC(10,0)",
      "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField], [floatField])"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD dateField_new DATE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList(
        "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN dateField DATE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD dateField_new DATE NOT NULL CONSTRAINT Test_dateField_new_DF DEFAULT '2010-01-01' WITH VALUES");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList(
        "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN dateField DATE NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList("DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN floatField NUMERIC(20,3) NOT NULL",
        "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField], [floatField])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList("DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN floatField NUMERIC(20,3)",
        "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField], [floatField])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("CREATE INDEX indexName ON TESTSCHEMA.Test ([id])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("CREATE INDEX indexName ON TESTSCHEMA.Test ([id], [version])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("CREATE UNIQUE NONCLUSTERED INDEX indexName ON TESTSCHEMA.Test ([id])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIndexDropStatements()
   */
  @Override
  protected List<String> expectedIndexDropStatements() {
    return Arrays.asList("DROP INDEX indexName ON TESTSCHEMA.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnMakePrimaryStatements()
   */
  @Override
  protected List<String> expectedAlterColumnMakePrimaryStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.Test DROP CONSTRAINT [Test_PK]",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN dateField DATE",
      "ALTER TABLE TESTSCHEMA.Test ADD CONSTRAINT [Test_PK] PRIMARY KEY ([id], [dateField])"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey DROP CONSTRAINT [CompositePrimaryKey_PK]",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ALTER COLUMN secondPrimaryKey NVARCHAR(5) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ADD CONSTRAINT [CompositePrimaryKey_PK] PRIMARY KEY ([id], [secondPrimaryKey])"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey DROP CONSTRAINT [CompositePrimaryKey_PK]",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ALTER COLUMN secondPrimaryKey NVARCHAR(5) COLLATE SQL_Latin1_General_CP1_CS_AS",
        "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ADD CONSTRAINT [CompositePrimaryKey_PK] PRIMARY KEY ([id])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
      "EXEC sp_rename 'TESTSCHEMA.Test.id', 'renamedId', 'COLUMN'",
      "ALTER TABLE TESTSCHEMA.Test DROP CONSTRAINT [Test_PK]",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN renamedId BIGINT NOT NULL",
        "ALTER TABLE TESTSCHEMA.Test ADD CONSTRAINT [Test_PK] PRIMARY KEY ([renamedId])");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList(
      "EXEC sp_rename 'TESTSCHEMA.Other.floatField', 'blahField', 'COLUMN'",
      "ALTER TABLE TESTSCHEMA.Other ALTER COLUMN blahField NUMERIC(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnChangingLengthAndCase()
   */
  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("EXEC sp_rename 'TESTSCHEMA.Other.floatField', 'FloatField', 'COLUMN'",
      "ALTER TABLE TESTSCHEMA.Other ALTER COLUMN FloatField NUMERIC(20,3) NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#varCharCast(java.lang.String)
   */
  @Override
  protected String varCharCast(String value) {
    return value;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD stringField_with_default NVARCHAR(6) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL CONSTRAINT Test_stringField_with_default_DF DEFAULT 'N' WITH VALUES");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableDropColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return ImmutableList.of(
      SqlServerDialect.dropDefaultForColumnSql.replace("{table}", "Test").replace("{column}", "bigIntegerField"),
      "ALTER TABLE TESTSCHEMA.Test DROP COLUMN bigIntegerField"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      // dropIndexStatements & addIndexStatements
      "DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([intField])",
      // changeColumnStatements
      "DROP INDEX Test_1 ON TESTSCHEMA.Test",
      "ALTER TABLE TESTSCHEMA.Test ALTER COLUMN intField NUMERIC(11,0)",
      "CREATE INDEX Test_1 ON TESTSCHEMA.Test ([INTFIELD])");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdate()
   */
  @Override
  protected List<String> expectedAutonumberUpdate() {
    return Arrays.asList("MERGE INTO TESTSCHEMA.Autonumber A USING (SELECT ISNULL(MAX(id) + 1, 1)  AS CurrentValue FROM TESTSCHEMA.TestTable) S ON (A.id = 'TestTable') WHEN MATCHED THEN UPDATE SET A.value = CASE WHEN S.CurrentValue > A.value THEN S.CurrentValue ELSE A.value END WHEN NOT MATCHED THEN INSERT (id, value) VALUES ('TestTable', S.CurrentValue);");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateWithSelectMinimum()
   */
  @Override
  protected String expectedUpdateWithSelectMinimum() {
    String value1 = varCharCast("'S'");
    String value2 = varCharCast("'Y'");
    return "UPDATE " + tableName("Other") + " SET intField = (SELECT MIN(intField) FROM " + tableName("Test") + " T WHERE ((T.charField = " + stringLiteralPrefix() + value1 + ") AND (T.stringField = O.stringField) AND (T.intField = O.intField))) FROM " + tableName("Other") + " O WHERE (stringField = " + stringLiteralPrefix() + value2 + ")";
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateUsingAliasedDestinationTable()
   */
  @Override
  protected String expectedUpdateUsingAliasedDestinationTable() {
    return "UPDATE " + tableName("FloatingRateRate") + " SET settlementFrequency = (SELECT settlementFrequency FROM " + tableName("FloatingRateDetail") + " B WHERE (A.floatingRateDetailId = B.id)) FROM " + tableName("FloatingRateRate") + " A";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateUsingTargetTableInDifferentSchema()
   */
  @Override
  protected String expectedUpdateUsingTargetTableInDifferentSchema() {
    return "UPDATE MYSCHEMA.FloatingRateRate SET settlementFrequency = (SELECT settlementFrequency FROM " + tableName("FloatingRateDetail") + " B WHERE (A.floatingRateDetailId = B.id)) FROM MYSCHEMA.FloatingRateRate A";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedUpdateUsingSourceTableInDifferentSchema()
   */
  @Override
  protected String expectedUpdateUsingSourceTableInDifferentSchema() {
    return "UPDATE " + tableName("FloatingRateRate") + " SET settlementFrequency = (SELECT settlementFrequency FROM " +
    		"MYSCHEMA.FloatingRateDetail B WHERE (A.floatingRateDetailId = B.id)) FROM " + tableName("FloatingRateRate") + " A";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedYYYYMMDDToDate()
   */
  @Override
  protected String expectedYYYYMMDDToDate() {
    return "CONVERT(date, " + stringLiteralPrefix() + "'20100101', 112)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmdd()
   */
  @Override
  protected String expectedDateToYyyymmdd() {
    return "CONVERT(VARCHAR(8),testField, 112)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(19),testField, 120),'-',''), ':', ''), ' ', '')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "GETUTCDATE()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDaysBetween()
   */
  @Override
  protected String expectedDaysBetween() {
    return "SELECT DATEDIFF(DAY, dateOne, dateTwo) FROM " + tableName("MyTable");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatement()
   */
  @Override
  protected List<String> expectedDropViewStatements() {
    return Arrays.asList(
      "IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'" + tableName("TestView") + "')) DROP VIEW " + tableName("TestView")
        );
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
    return Arrays.asList("MERGE INTO TESTSCHEMA.Autonumber A USING (SELECT ISNULL(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TESTSCHEMA.TestTable) S ON (A.id = 'TestTable') WHEN MATCHED THEN UPDATE SET A.value = CASE WHEN S.CurrentValue > A.value THEN S.CurrentValue ELSE A.value END WHEN NOT MATCHED THEN INSERT (id, value) VALUES ('TestTable', S.CurrentValue);");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringFunctionCast()
   */
  @Override
  protected String expectedStringFunctionCast() {
    return "CAST(MIN(field) AS NVARCHAR(8)) COLLATE SQL_Latin1_General_CP1_CS_AS";
  }


  /**
   * @return The expected SQL for the MOD operator.
   */
  @Override
  protected String expectedSelectModSQL() {
    return "SELECT intField % 5 FROM " + tableName("Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateLiteral()
   */
  @Override
  protected String expectedDateLiteral() {
    return "'2010-01-02'";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "MERGE INTO TESTSCHEMA.foo USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM TESTSCHEMA.somewhere) AS _mergesource ON foo.id = _mergesource.id WHEN MATCHED THEN UPDATE SET foo.bar = _mergesource.bar WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (_mergesource.id, _mergesource.bar);";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
    return "MERGE INTO TESTSCHEMA.foo USING (SELECT somewhere.newId AS id, join.joinBar AS bar FROM TESTSCHEMA.somewhere INNER JOIN TESTSCHEMA.join ON (somewhere.newId = join.joinId)) AS _mergesource ON foo.id = _mergesource.id WHEN MATCHED THEN UPDATE SET foo.bar = _mergesource.bar WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (_mergesource.id, _mergesource.bar);";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "MERGE INTO TESTSCHEMA.foo USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM MYSCHEMA.somewhere) AS _mergesource ON foo.id = _mergesource.id WHEN MATCHED THEN UPDATE SET foo.bar = _mergesource.bar WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (_mergesource.id, _mergesource.bar);";
  };


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeTargetInDifferentSchema()
   */
  @Override
  protected String expectedMergeTargetInDifferentSchema() {
    return "MERGE INTO MYSCHEMA.foo USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM TESTSCHEMA.somewhere) AS _mergesource ON foo.id = _mergesource.id WHEN MATCHED THEN UPDATE SET foo.bar = _mergesource.bar WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (_mergesource.id, _mergesource.bar);";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddDays()
   */
  @Override
  protected String expectedAddDays() {
    return "DATEADD(dd, -20, testField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddMonths()
   */
  @Override
  protected String expectedAddMonths() {
    return "DATEADD(month, -3, testField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromSimpleKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromSimpleKeyStatements() {
    return ImmutableList.of("ALTER TABLE TESTSCHEMA.Test DROP CONSTRAINT [Test_PK]", "ALTER TABLE TESTSCHEMA.Test DROP COLUMN id");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of(
      "IF EXISTS (SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'Test_version_DF') AND type = (N'D')) exec sp_rename N'Test_version_DF', N'Renamed_version_DF'",
      "sp_rename N'Test.Test_PK', N'Renamed_PK', N'INDEX'",
      "sp_rename N'Test', N'Renamed'");
  }

  /**
   * @return the expected statements for renaming a table with a long name.
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of(
      "IF EXISTS (SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'123456789012345678901234567890XXX_version_DF') AND type = (N'D')) exec sp_rename N'123456789012345678901234567890XXX_version_DF', N'Blah_version_DF'",
      "sp_rename N'123456789012345678901234567890XXX.123456789012345678901234567890XXX_PK', N'Blah_PK', N'INDEX'",
      "sp_rename N'123456789012345678901234567890XXX', N'Blah'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of("sp_rename N'TESTSCHEMA.#TempTest.TempTest_1', N'TempTest_2', N'INDEX'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "MERGE INTO TESTSCHEMA.foo USING (SELECT somewhere.newId AS id FROM TESTSCHEMA.somewhere) AS _mergesource ON foo.id = _mergesource.id WHEN NOT MATCHED THEN INSERT (id) VALUES (_mergesource.id);";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomString()
   */
  @Override
  protected String expectedRandomString() {
    return "SUBSTRING(REPLACE(CONVERT(varchar(255),NEWID()),'-',''), 1, 10)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedLeftPad()
   */
  @Override
  protected String expectedLeftPad() {
    return "SELECT CASE WHEN LEN(stringField) > 10 THEN LEFT(stringField, 10) ELSE RIGHT(REPLICATE('j', 10) + stringField, 10) END FROM TESTSCHEMA.Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectOrderByNullsLast()
   */
  @Override
  protected String expectedSelectOrderByNullsLast() {
    return "SELECT stringField FROM " + tableName("Alternate") + " ORDER BY (CASE WHEN stringField IS NULL THEN 1 ELSE 0 END), stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectOrderByNullsFirstDesc()
   */
  @Override
  protected String expectedSelectOrderByNullsFirstDesc() {
    return "SELECT stringField FROM " + tableName("Alternate") + " ORDER BY (CASE WHEN stringField IS NULL THEN 0 ELSE 1 END), stringField DESC";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  @Override
  protected String expectedSelectOrderByTwoFields() {
    return "SELECT stringField1, stringField2 FROM " + tableName("Alternate") + " ORDER BY (CASE WHEN stringField1 IS NULL THEN 0 ELSE 1 END), stringField1 DESC, (CASE WHEN stringField2 IS NULL THEN 1 ELSE 0 END), stringField2";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  @Override
  protected String expectedSelectFirstOrderByNullsLastDesc() {
    return "SELECT TOP 1 stringField FROM " + tableName("Alternate") + " ORDER BY (CASE WHEN stringField IS NULL THEN 1 ELSE 0 END), stringField DESC";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectLiteralWithWhereClauseString()
   */
  @Override
  protected String expectedSelectLiteralWithWhereClauseString() {
    return "SELECT 'LITERAL' WHERE ('ONE' = 'ONE')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddTableFromStatements()
   */
  @Override
  protected List<String> expectedAddTableFromStatements() {
    return ImmutableList.of(
      "CREATE TABLE TESTSCHEMA.SomeTable ([someField] NVARCHAR(3) COLLATE SQL_Latin1_General_CP1_CS_AS NOT NULL, [otherField] NUMERIC(3,0) NOT NULL, CONSTRAINT [SomeTable_PK] PRIMARY KEY ([someField]))",
      "CREATE INDEX SomeTable_1 ON TESTSCHEMA.SomeTable ([otherField])",
      "INSERT INTO TESTSCHEMA.SomeTable SELECT someField, otherField FROM TESTSCHEMA.OtherTable"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT * "
         + "FROM SCHEMA2.Foo "
         + "INNER JOIN " + tableName("Bar") + " ON (a = b) "
         + "LEFT OUTER JOIN " + tableName("Fo") + " ON (a = b) "
         + "INNER JOIN " + tableName("Fum") + " Fumble ON (a = b) "
         + "ORDER BY a "
         + "OPTION("
         +  "FORCE ORDER, "
         +  "FAST " + rowCount + ", "
         +  "TABLE HINT(SCHEMA2.Foo, INDEX(Foo_1)), "
         +  "TABLE HINT(aliased, INDEX(Foo_2))"
         + ")";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints2(int)
   */
  @Override
  protected String expectedHints2(int rowCount) {
    return "SELECT a, b "
         + "FROM " + tableName("Foo") + " "
         + "ORDER BY a "
         + "OPTION("
         +  "TABLE HINT(" + tableName("Foo") + ", INDEX(Foo_1)), "
         +  "FAST " + rowCount + ", "
         +  "FORCE ORDER"
         + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedForUpdate()
   */
  @Override
  protected String expectedForUpdate() {
    return StringUtils.EMPTY;
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
    return "DELETE TOP (1000) FROM " + tableName(TEST_TABLE) + " WHERE (Test.stringField = " + stringLiteralPrefix() + value + ")";
  };


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndComplexWhere(String value1, String value2) {
    return "DELETE TOP (1000) FROM " + tableName(TEST_TABLE) + " WHERE ((Test.stringField = " + stringLiteralPrefix() + value1 + ") OR (Test.stringField = " + stringLiteralPrefix() + value2 + "))";
  };


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitWithoutWhere() {
    return "DELETE TOP (1000) FROM " + tableName(TEST_TABLE);
  };
}
