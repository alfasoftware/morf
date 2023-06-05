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

package org.alfasoftware.morf.jdbc.mysql;

import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;

/**
 * Tests SQL statements generated for MySQL.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestMySqlDialect extends AbstractSqlDialectTest {

  private final QueryBuilder queryBuilder = mock(QueryBuilder.class);
  private long maxIdValue;

  @SuppressWarnings("unchecked")
  @Override
  @Before
  public void setUp() {
    super.setUp();
    when(sqlScriptExecutor.executeQuery(any(String.class))).thenReturn(queryBuilder);
    when(queryBuilder.withConnection(connection)).thenReturn(queryBuilder);
    when(queryBuilder.processWith(any(ResultSetProcessor.class))).thenReturn(5L);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#setMaxIdOnAutonumberTable(long)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected void setMaxIdOnAutonumberTable(long id) {
    when(queryBuilder.processWith(any(ResultSetProcessor.class))).thenReturn(id);
    maxIdValue = id;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new MySqlDialect();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE `Test` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3), `intField` INTEGER, `floatField` DECIMAL(13,2) NOT NULL, `dateField` DATE, `booleanField` TINYINT(1), `charField` VARCHAR(1), `blobField` LONGBLOB, `bigIntegerField` BIGINT DEFAULT 12345, `clobField` LONGTEXT, CONSTRAINT `Test_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "ALTER TABLE `Test` ADD UNIQUE INDEX `Test_NK` (`stringField`)",
          "ALTER TABLE `Test` ADD UNIQUE INDEX `Test_1` (`intField`, `floatField`)",
          "CREATE TABLE `Alternate` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3), CONSTRAINT `Alternate_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "ALTER TABLE `Alternate` ADD INDEX `Alternate_1` (`stringField`)",
          "CREATE TABLE `NonNull` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3) NOT NULL, `intField` DECIMAL(8,0) NOT NULL, `booleanField` TINYINT(1) NOT NULL, `dateField` DATE NOT NULL, `blobField` LONGBLOB NOT NULL, CONSTRAINT `NonNull_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "CREATE TABLE `CompositePrimaryKey` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3) NOT NULL, `secondPrimaryKey` VARCHAR(3) NOT NULL, CONSTRAINT `CompositePrimaryKey_PK` PRIMARY KEY (`id`, `secondPrimaryKey`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "CREATE TABLE `AutoNumber` (`intField` BIGINT AUTO_INCREMENT COMMENT 'AUTONUMSTART:[5]', CONSTRAINT `AutoNumber_PK` PRIMARY KEY (`intField`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=5"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTemporaryTableStatements() {
    return Arrays
        .asList(
          "CREATE TEMPORARY TABLE `TempTest` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3), `intField` INTEGER, `floatField` DECIMAL(13,2) NOT NULL, `dateField` DATE, `booleanField` TINYINT(1), `charField` VARCHAR(1), `blobField` LONGBLOB, `bigIntegerField` BIGINT DEFAULT 12345, `clobField` LONGTEXT, CONSTRAINT `TempTest_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "ALTER TABLE `TempTest` ADD UNIQUE INDEX `TempTest_NK` (`stringField`)",
          "ALTER TABLE `TempTest` ADD INDEX `TempTest_1` (`intField`, `floatField`)",
          "CREATE TEMPORARY TABLE `TempAlternate` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3), CONSTRAINT `TempAlternate_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "ALTER TABLE `TempAlternate` ADD INDEX `TempAlternate_1` (`stringField`)",
          "CREATE TEMPORARY TABLE `TempNonNull` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3) NOT NULL, `intField` DECIMAL(8,0) NOT NULL, `booleanField` TINYINT(1) NOT NULL, `dateField` DATE NOT NULL, `blobField` LONGBLOB NOT NULL, CONSTRAINT `TempNonNull_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays
        .asList("CREATE TABLE `"
            + TABLE_WITH_VERY_LONG_NAME
            + "` (`id` BIGINT NOT NULL, `version` INTEGER DEFAULT 0, `stringField` VARCHAR(3), `intField` DECIMAL(8,0), `floatField` DECIMAL(13,2) NOT NULL, `dateField` DATE, `booleanField` TINYINT(1), `charField` VARCHAR(1), CONSTRAINT `"
            + TABLE_WITH_VERY_LONG_NAME + "_PK` PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
            "ALTER TABLE `"+TABLE_WITH_VERY_LONG_NAME+"` ADD UNIQUE INDEX `Test_NK` (`stringField`)",
            "ALTER TABLE `"+TABLE_WITH_VERY_LONG_NAME+"` ADD INDEX `Test_1` (`intField`, `floatField`)"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTableStatements()
   */
  @Override
  protected List<String> expectedDropTableStatements() {
    return Arrays.asList("FLUSH TABLES `Test`", "DROP TABLE `Test`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropSingleTable()
   */
  @Override
  protected List<String> expectedDropSingleTable() {
    return Arrays.asList("FLUSH TABLES `Test`", "DROP TABLE `Test`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTables()
   */
  @Override
  protected List<String> expectedDropTables() {
    return Arrays.asList("FLUSH TABLES `Test`, `Other`", "DROP TABLE `Test`, `Other`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTablesWithParameters()
   */
  @Override
  protected List<String> expectedDropTablesWithParameters() {
    return Arrays.asList("FLUSH TABLES `Test`, `Other`", "DROP TABLE IF EXISTS `Test`, `Other` CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("FLUSH TABLES `TempTest`", "DROP TABLE `TempTest`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("TRUNCATE TABLE Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTempTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTempTableStatements() {
    return Arrays.asList("TRUNCATE TABLE TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDeleteAllFromTableStatements()
   */
  @Override
  protected List<String> expectedDeleteAllFromTableStatements() {
    return Arrays.asList("DELETE FROM Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatement()
   */
  @Override
  protected String expectedParameterisedInsertStatement() {
    return "INSERT INTO Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, 'Escap''d', 7, :floatField, 20100405, 1, :charField, :blobField, :bigIntegerField, :clobField)";
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
    return ImmutableList.of(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM Test))",
      "INSERT INTO Test (version, stringField, id) SELECT version, stringField, (SELECT COALESCE(value, 0) FROM idvalues WHERE (name = 'Test')) + Other.id FROM Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM Test))",
      "INSERT INTO Test (stringField, id, version) SELECT stringField, (SELECT COALESCE(value, 0) FROM idvalues WHERE (name = 'Test')) + Other.id, 0 AS version FROM Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyPostInsertStatementsInsertingUnderAutonumLimit(org.alfasoftware.morf.jdbc.SqlScriptExecutor, java.sql.Connection)
   */
  @Override
  protected void verifyPostInsertStatementsInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    verifyRepairAutoNumberStartPosition(sqlScriptExecutor,connection); // it's the same
  }


  /**
   * @return The expected SQL statements to be run after insert for the test database table.
   */
  @Override
  protected void verifyPostInsertStatementsNotInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    verifyRepairAutoNumberStartPosition(sqlScriptExecutor,connection); // it's the same
  }


  @SuppressWarnings({"unchecked","rawtypes"})
  @Override
  protected void  verifyRepairAutoNumberStartPosition(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    boolean isOverRepairLimit = maxIdValue >= 1000;
    ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass((Class<List<String>>)(Class)List.class);

    verify(sqlScriptExecutor).execute("ANALYZE TABLE Test", connection);
    verify(sqlScriptExecutor).executeQuery("SELECT MAX(intField) FROM AutoNumber");
    verify(sqlScriptExecutor).execute(listCaptor.capture(), eq(connection));
    verifyNoMoreInteractions(sqlScriptExecutor);

    if(isOverRepairLimit) {
      assertThat(listCaptor.getValue(),hasSize(1));
      assertEquals("ANALYZE TABLE AutoNumber",listCaptor.getValue().get(0));
    }else {
      assertThat(listCaptor.getValue(),hasSize(2));
      assertEquals("ALTER TABLE AutoNumber AUTO_INCREMENT = 5",listCaptor.getValue().get(0));
      assertEquals("ANALYZE TABLE AutoNumber",listCaptor.getValue().get(1));
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM Test))",
      "INSERT INTO Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, 1, 'X', (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsertWithTableInDifferentSchema()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM MYSCHEMA.Test))",
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, 1, 'X', (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithNoColumnValues()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithNoColumnValues() {
    return "INSERT INTO Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (:id, :version, :stringField, :intField, :floatField, :dateField, :booleanField, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedEmptyStringInsertStatement()
   */
  @Override
  protected String expectedEmptyStringInsertStatement() {
    return "INSERT INTO Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, 0, 0, null, 0, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT CONCAT_WS('', assetDescriptionLine1, CASE WHEN (taxVariationIndicator = 'Y') THEN exposureCustomerNumber ELSE invoicingCustomerNumber END) AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT CONCAT_WS('', assetDescriptionLine1, MAX(scheduleStartDate)) AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT CONCAT_WS('', 'ABC', ' ', 'DEF') AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */
  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT CONCAT_WS('', field1, CONCAT_WS('', field2, 'XYZ')) AS test FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT CONCAT_WS('', assetDescriptionLine1, ' ', assetDescriptionLine2) AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT CONCAT_WS('', assetDescriptionLine1, 'XYZ', assetDescriptionLine2) AS assetDescription FROM schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIsNull()
   */
  @Override
  protected String expectedIsNull() {
    return "COALESCE('A', 'B')";
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
    return "CAST(value AS CHAR(10))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntCast()
   */
  @Override
  protected String expectedBigIntCast() {
    return "value";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntFunctionCast()
   */
  @Override
  protected String expectedBigIntFunctionCast() {
    return "MIN(value)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanCast()
   */
  @Override
  protected String expectedBooleanCast() {
    return "CAST(value AS TINYINT(1))";
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
    return "CAST(value AS SIGNED)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringLiteralToIntegerCast()
   */
  @Override
  protected String expectedStringLiteralToIntegerCast() {
    return "CAST(" + stringLiteralPrefix() + "'1234567890' AS SIGNED)";
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `intField_new` INTEGER");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `intField` `intField` INTEGER NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `stringField_new` VARCHAR(6)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `stringField` `stringField` VARCHAR(6)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `booleanField_new` TINYINT(1)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `booleanField` `booleanField` TINYINT(1) NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `dateField_new` DATE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `dateField` `dateField` DATE NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `floatField_new` DECIMAL(6,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `floatField` `floatField` DECIMAL(14,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `bigIntegerField_new` BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `bigIntegerField` `bigIntegerField` BIGINT");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `blobField_new` LONGBLOB");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `blobField` `blobField` LONGBLOB NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `dateField_new` DATE DEFAULT '2010-01-01' NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `dateField` `dateField` DATE NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE `Test` ADD `floatField_new` DECIMAL(6,3) DEFAULT 20.33");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `bigIntegerField` `bigIntegerField` BIGINT DEFAULT 54321");
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableDropColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return Collections.singletonList("ALTER TABLE `Test` DROP `bigIntegerField`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      // dropIndexStatements & addIndexStatements
      "ALTER TABLE `Test` DROP INDEX `Test_1`",
      "ALTER TABLE `Test` ADD INDEX `Test_1` (`intField`)",
      // changeColumnStatements
      "ALTER TABLE `Test` CHANGE `intField` `intField` INTEGER NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("ALTER TABLE `Test` ADD INDEX `indexName` (`id`)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("ALTER TABLE `Test` ADD INDEX `indexName` (`id`, `version`)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("ALTER TABLE `Test` ADD UNIQUE INDEX `indexName` (`id`)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUniqueNullable()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUniqueNullable() {
    return Arrays.asList("ALTER TABLE `Test` ADD UNIQUE INDEX `indexName` (`stringField`, `intField`, `floatField`, `dateField`)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `floatField` `floatField` DECIMAL(20,3) NOT NULL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList("ALTER TABLE `Test` CHANGE `floatField` `floatField` DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIndexDropStatements()
   */
  @Override
  protected List<String> expectedIndexDropStatements() {
    return Arrays.asList("ALTER TABLE `Test` DROP INDEX `indexName`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnMakePrimaryStatements()
   */
  @Override
  protected List<String> expectedAlterColumnMakePrimaryStatements() {
    return Arrays.asList(
      "ALTER TABLE `Test` DROP PRIMARY KEY",
      "ALTER TABLE `Test` CHANGE `dateField` `dateField` DATE",
      "ALTER TABLE `Test` ADD CONSTRAINT `Test_PK` PRIMARY KEY (`id`, `dateField`)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE `CompositePrimaryKey` CHANGE `secondPrimaryKey` `secondPrimaryKey` VARCHAR(5) NOT NULL"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE `CompositePrimaryKey` DROP PRIMARY KEY",
      "ALTER TABLE `CompositePrimaryKey` CHANGE `secondPrimaryKey` `secondPrimaryKey` VARCHAR(5)",
      "ALTER TABLE `CompositePrimaryKey` ADD CONSTRAINT `CompositePrimaryKey_PK` PRIMARY KEY (`id`)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
      "ALTER TABLE `Test` CHANGE `id` `renamedId` BIGINT NOT NULL"
      );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList("ALTER TABLE `Other` CHANGE `floatField` `blahField` DECIMAL(20,3)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnChangingLengthAndCase()
   */
  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("ALTER TABLE `Other` CHANGE `floatField` `FloatField` DECIMAL(20,3) NOT NULL");
  }


  /**
   * It is only necessary to cast for HSQLDB. Returns the value without casting.
   *
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
    return Arrays.asList("ALTER TABLE `Test` ADD `stringField_with_default` VARCHAR(6) DEFAULT 'N' NOT NULL");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdate()
   */
  @Override
  protected List<String> expectedAutonumberUpdate() {
    return Arrays.asList("INSERT IGNORE INTO Autonumber (id, value) VALUES('TestTable', 1)", "UPDATE Autonumber set value = (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM TestTable) where id = 'TestTable' and value < (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM TestTable) and (SELECT COALESCE(MAX(id) + 1, 1)  AS CurrentValue FROM TestTable) <> 1");
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedYYYYMMDDToDate()
   */
  @Override
  protected String expectedYYYYMMDDToDate() {
    return "DATE(" + stringLiteralPrefix() + "'20100101')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmdd()
   */
  @Override
  protected String expectedDateToYyyymmdd() {
    return "CAST(DATE_FORMAT(testField, '%Y%m%d') AS DECIMAL(8))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "CAST(DATE_FORMAT(testField, '%Y%m%d%H%i%s') AS DECIMAL(14))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "UTC_TIMESTAMP()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatements()
   */
  @Override
  protected List<String> expectedDropViewStatements() {
    return Arrays.asList("DROP VIEW IF EXISTS `" + tableName("TestView") + "`") ;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSubstring()
   */
  @Override
  protected String expectedSubstring() {
    return "SELECT SUBSTRING(field1, 1, 3) FROM " + tableName("schedule");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDaysBetween()
   */
  @Override
  protected String expectedDaysBetween() {
    return "SELECT TO_DAYS(dateTwo) - TO_DAYS(dateOne) FROM MyTable";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdateForNonIdColumn()
   */
  @Override
  protected List<String> expectedAutonumberUpdateForNonIdColumn() {
    return Arrays.asList("INSERT IGNORE INTO Autonumber (id, value) VALUES('TestTable', 1)", "UPDATE Autonumber set value = (SELECT COALESCE(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TestTable) where id = 'TestTable' and value < (SELECT COALESCE(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TestTable) and (SELECT COALESCE(MAX(generatedColumn) + 1, 1)  AS CurrentValue FROM TestTable) <> 1");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringFunctionCast()
   */
  @Override
  protected String expectedStringFunctionCast() {
    return "CAST(MIN(field) AS CHAR(8))";
  }


  /**
   * Overrides the standard behaviour to ensure that the prepared statement is
   * set up with an integer for booleans
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyBooleanPrepareStatementParameter()
   */
  @Override
  protected void verifyBooleanPrepareStatementParameter() throws SQLException {

    final SqlParameter booleanColumn = parameter(SchemaUtils.column("a", DataType.BOOLEAN));
    BigDecimal nullCheck = null;
    verify(callPrepareStatementParameter(booleanColumn, null)).setObject(any(SqlParameter.class), eq(nullCheck));

    NamedParameterPreparedStatement mockStatement = callPrepareStatementParameter(booleanColumn, "true");
    ArgumentCaptor<Integer>intCapture = ArgumentCaptor.forClass(Integer.class);
    verify(mockStatement).setInt(any(SqlParameter.class), intCapture.capture());
    assertEquals("Integer not correctly set on statement", 1, intCapture.getValue().intValue());

    mockStatement = callPrepareStatementParameter(booleanColumn, "false");
    intCapture = ArgumentCaptor.forClass(Integer.class);
    verify(mockStatement).setInt(any(SqlParameter.class), intCapture.capture());
    assertEquals("Integer not correctly set on statement", 0, intCapture.getValue().intValue());

  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "INSERT INTO foo(id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere"
        + " ON DUPLICATE KEY UPDATE bar = values(bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
    return "INSERT INTO foo(id, bar)"
        + " SELECT somewhere.newId AS id, join.joinBar AS bar FROM somewhere INNER JOIN join ON (somewhere.newId = join.joinId)"
        + " ON DUPLICATE KEY UPDATE bar = values(bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeTargetInDifferentSchema()
   */
  @Override
  protected String expectedMergeTargetInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.foo(id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere"
        + " ON DUPLICATE KEY UPDATE bar = values(bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "INSERT INTO foo(id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM MYSCHEMA.somewhere"
        + " ON DUPLICATE KEY UPDATE bar = values(bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "INSERT IGNORE INTO foo(id)"
        + " SELECT somewhere.newId AS id FROM somewhere";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeWithUpdateExpressions()
   */
  @Override
  protected String expectedMergeWithUpdateExpressions() {
    return "INSERT INTO foo(id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM somewhere"
        + " ON DUPLICATE KEY UPDATE bar = values(bar) + foo.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddDays()
   */
  @Override
  protected String expectedAddDays() {
    return "DATE_ADD(testField, INTERVAL -20 DAY)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddMonths()
   */
  @Override
  protected String expectedAddMonths() {
    return "DATE_ADD(testField, INTERVAL -3 MONTH)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromSimpleKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromSimpleKeyStatements() {
    return Collections.singletonList("ALTER TABLE `Test` DROP `id`");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of("RENAME TABLE Test TO Renamed");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#getRenamingTableWithLongNameStatements()
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of("RENAME TABLE 123456789012345678901234567890XXX TO Blah");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of(
      "ALTER TABLE `Test` DROP INDEX `Test_1`",
      "ALTER TABLE `Test` ADD UNIQUE INDEX `Test_2` (`intField`, `floatField`)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameTempIndexStatements() {
    return ImmutableList.of(
      "ALTER TABLE `TempTest` DROP INDEX `TempTest_1`",
      "ALTER TABLE `TempTest` ADD INDEX `TempTest_2` (`intField`, `floatField`)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomString()
   */
  @Override
  protected String expectedRandomString() {
    return "SUBSTRING(MD5(RAND()), 1, 10)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectOrderByNullsLast()
   */
  @Override
  protected String expectedSelectOrderByNullsLast() {
    return "SELECT stringField FROM " + tableName("Alternate") + " ORDER BY ISNULL(stringField), stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectOrderByNullsFirstDesc()
   */
  @Override
  protected String expectedSelectOrderByNullsFirstDesc() {
    return "SELECT stringField FROM " + tableName("Alternate") + " ORDER BY -ISNULL(stringField), stringField DESC";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  @Override
  protected String expectedSelectOrderByTwoFields() {
    return "SELECT stringField1, stringField2 FROM " + tableName("Alternate") + " ORDER BY -ISNULL(stringField1), stringField1 DESC, ISNULL(stringField2), stringField2";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  @Override
  protected String expectedSelectFirstOrderByNullsLastDesc() {
    return "SELECT stringField FROM " + tableName("Alternate") + " ORDER BY ISNULL(stringField), stringField DESC LIMIT 0,1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectLiteralWithWhereClauseString()
   */
  @Override
  protected String expectedSelectLiteralWithWhereClauseString() {
    return "SELECT 'LITERAL' FROM dual WHERE ('ONE' = 'ONE')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddTableFromStatements()
   */
  @Override
  protected List<String> expectedAddTableFromStatements() {
    return ImmutableList.of(
      "CREATE TABLE `SomeTable` (`someField` VARCHAR(3) NOT NULL, `otherField` DECIMAL(3,0) NOT NULL, CONSTRAINT `SomeTable_PK` PRIMARY KEY (`someField`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
      "ALTER TABLE `SomeTable` ADD INDEX `SomeTable_1` (`otherField`)",
      "INSERT INTO SomeTable SELECT someField, otherField FROM OtherTable"
    );
  }


  @Override
  protected List<String> expectedReplaceTableFromStatements() {
    return ImmutableList.of(
      "CREATE TABLE `tmp_SomeTable` (`someField` VARCHAR(3) NOT NULL, `otherField` DECIMAL(3,0) NOT NULL, `thirdField` DECIMAL(5,0) NOT NULL, CONSTRAINT `tmp_SomeTable_PK` PRIMARY KEY (`someField`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
          "INSERT INTO tmp_SomeTable SELECT someField, otherField, CAST(thirdField AS DECIMAL(5,0)) AS thirdField FROM OtherTable",
          "FLUSH TABLES `SomeTable`",
          "DROP TABLE `SomeTable` CASCADE",
          "RENAME TABLE tmp_SomeTable TO SomeTable",
          "ALTER TABLE `SomeTable` ADD INDEX `SomeTable_1` (`otherField`)"
    );
  }


  @Override
  protected List<String> expectedReplaceTableWithAutonumber() {
    return ImmutableList.of(
        "CREATE TABLE `tmp_SomeTable` (`someField` VARCHAR(3) NOT NULL, `otherField` DECIMAL(3,0) AUTO_INCREMENT COMMENT 'AUTONUMSTART:[1]', `thirdField` DECIMAL(5,0) NOT NULL, CONSTRAINT `tmp_SomeTable_PK` PRIMARY KEY (`someField`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1",
        "INSERT INTO tmp_SomeTable SELECT someField, otherField, CAST(thirdField AS DECIMAL(5,0)) AS thirdField FROM OtherTable",
        "FLUSH TABLES `SomeTable`",
        "DROP TABLE `SomeTable` CASCADE",
        "RENAME TABLE tmp_SomeTable TO SomeTable",
        "ALTER TABLE `SomeTable` ADD INDEX `SomeTable_1` (`otherField`)"
    );
  }


  /**
   * We only support {@link SelectStatement#useImplicitJoinOrder()}, and only to a limited extent.
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT * FROM SCHEMA2.Foo STRAIGHT_JOIN Bar ON (a = b) LEFT OUTER JOIN Fo ON (a = b) STRAIGHT_JOIN Fum Fumble ON (a = b) ORDER BY a";
  }


  /**
   * No need for an escape on MySQL
   */
  @Override
  protected String likeEscapeSuffix() {
    return "";
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBlobLiteral(String)  ()
   */
  @Override
  protected String expectedBlobLiteral(String value) {
    return String.format("x%s", super.expectedBlobLiteral(value));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewOverUnionSelectStatements()
   */
  @Override
  protected List<String> expectedCreateViewOverUnionSelectStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah') UNION ALL SELECT stringField FROM " + tableName(OTHER_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah')");
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
}
