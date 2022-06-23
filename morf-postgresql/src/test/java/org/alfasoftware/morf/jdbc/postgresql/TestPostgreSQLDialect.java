package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.CustomHint;
import org.alfasoftware.morf.sql.PostgreSQLCustomHint;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

/**
 * Tests SQL statements generated for PostgreSQL.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class TestPostgreSQLDialect extends AbstractSqlDialectTest {

  @Override
  protected String expectedSelectFirstOrderByNullsLastDesc() {
    return "SELECT stringField FROM testschema.Alternate ORDER BY stringField DESC NULLS LAST LIMIT 1 OFFSET 0";
  }


  @Override
  protected String expectedRandomFunction() {
    return "RANDOM()";
  }


  @Override
  protected String expectedRound() {
    return "SELECT ROUND((field1) :: NUMERIC, 2) FROM " + tableName("schedule");
  }


  @Override
  protected String expectedSqlForMathOperationsForExistingDataFix1() {
    return "ROUND((doublevalue / 1000 * doublevalue) :: NUMERIC, 2)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new PostgreSQLDialect("testschema");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatements()
   */
  @Override
  protected List<String> expectedCreateTableStatements() {
    return Arrays
        .asList(
          "CREATE TABLE testschema.Test (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\", intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BOOLEAN, charField VARCHAR(1) COLLATE \"POSIX\", blobField BYTEA, bigIntegerField NUMERIC(19) DEFAULT 12345, clobField TEXT, CONSTRAINT Test_PK PRIMARY KEY(id))",
          "COMMENT ON TABLE testschema.Test IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test]'",
          "COMMENT ON COLUMN testschema.Test.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN testschema.Test.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN testschema.Test.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN testschema.Test.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN testschema.Test.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN testschema.Test.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN testschema.Test.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN testschema.Test.charField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN testschema.Test.blobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
          "COMMENT ON COLUMN testschema.Test.bigIntegerField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN testschema.Test.clobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[clobField]/TYPE:[CLOB]'",
          "CREATE UNIQUE INDEX Test_NK ON testschema.Test (stringField)",
          "COMMENT ON INDEX Test_NK IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_NK]'",
          "CREATE UNIQUE INDEX Test_NK$null0 ON testschema.Test ((0)) WHERE stringField IS NULL",
          "COMMENT ON INDEX Test_NK$null0 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[e069928444ba25d94fe4d5f64e1423ae/cfcd208495d565ef66e7dff9f98764da]'",
          "CREATE UNIQUE INDEX Test_1 ON testschema.Test (intField, floatField)",
          "COMMENT ON INDEX Test_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_1]'",
          "CREATE UNIQUE INDEX Test_1$null0 ON testschema.Test (floatField) WHERE intField IS NULL",
          "COMMENT ON INDEX Test_1$null0 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[9c2e9ac2612f55025a2656b5c2cc9316/cfcd208495d565ef66e7dff9f98764da]'",
          "CREATE TABLE testschema.Alternate (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\", CONSTRAINT Alternate_PK PRIMARY KEY(id))",
          "COMMENT ON TABLE testschema.Alternate IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Alternate]'",
          "COMMENT ON COLUMN testschema.Alternate.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN testschema.Alternate.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN testschema.Alternate.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "CREATE INDEX Alternate_1 ON testschema.Alternate (stringField)",
          "COMMENT ON INDEX Alternate_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Alternate_1]'",
          "CREATE TABLE testschema.NonNull (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\" NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BOOLEAN NOT NULL, dateField DATE NOT NULL, blobField BYTEA NOT NULL, CONSTRAINT NonNull_PK PRIMARY KEY(id))",
          "COMMENT ON TABLE testschema.NonNull IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[NonNull]'",
          "COMMENT ON COLUMN testschema.NonNull.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN testschema.NonNull.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN testschema.NonNull.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN testschema.NonNull.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN testschema.NonNull.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN testschema.NonNull.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN testschema.NonNull.blobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
          "CREATE TABLE testschema.CompositePrimaryKey (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\" NOT NULL, secondPrimaryKey VARCHAR(3) COLLATE \"POSIX\" NOT NULL, CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY(id, secondPrimaryKey))",
          "COMMENT ON TABLE testschema.CompositePrimaryKey IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[CompositePrimaryKey]'",
          "COMMENT ON COLUMN testschema.CompositePrimaryKey.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN testschema.CompositePrimaryKey.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN testschema.CompositePrimaryKey.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN testschema.CompositePrimaryKey.secondPrimaryKey IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'",
          "DROP SEQUENCE IF EXISTS testschema.AutoNumber_intField_seq",
          "CREATE SEQUENCE testschema.AutoNumber_intField_seq START 5",
          "CREATE TABLE testschema.AutoNumber (intField NUMERIC(19) DEFAULT nextval('testschema.AutoNumber_intField_seq'), CONSTRAINT AutoNumber_PK PRIMARY KEY(intField))",
          "COMMENT ON TABLE testschema.AutoNumber IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[AutoNumber]'",
          "ALTER SEQUENCE testschema.AutoNumber_intField_seq OWNED BY testschema.AutoNumber.intField",
          "COMMENT ON COLUMN testschema.AutoNumber.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[BIG_INTEGER]/AUTONUMSTART:[5]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTemporaryTableStatements()
   */
  @Override
  protected List<String> expectedCreateTemporaryTableStatements() {
    return Arrays
        .asList(
            "CREATE TEMP TABLE TempTest (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\", intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BOOLEAN, charField VARCHAR(1) COLLATE \"POSIX\", blobField BYTEA, bigIntegerField NUMERIC(19) DEFAULT 12345, clobField TEXT, CONSTRAINT TempTest_PK PRIMARY KEY(id))",
            "COMMENT ON TABLE TempTest IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempTest]'",
            "COMMENT ON COLUMN TempTest.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
            "COMMENT ON COLUMN TempTest.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
            "COMMENT ON COLUMN TempTest.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
            "COMMENT ON COLUMN TempTest.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'",
            "COMMENT ON COLUMN TempTest.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
            "COMMENT ON COLUMN TempTest.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
            "COMMENT ON COLUMN TempTest.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
            "COMMENT ON COLUMN TempTest.charField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
            "COMMENT ON COLUMN TempTest.blobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
            "COMMENT ON COLUMN TempTest.bigIntegerField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'",
            "COMMENT ON COLUMN TempTest.clobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[clobField]/TYPE:[CLOB]'",
            "CREATE UNIQUE INDEX TempTest_NK ON TempTest (stringField)",
            "COMMENT ON INDEX TempTest_NK IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempTest_NK]'",
            "CREATE UNIQUE INDEX TempTest_NK$null0 ON TempTest ((0)) WHERE stringField IS NULL",
            "COMMENT ON INDEX TempTest_NK$null0 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[e069928444ba25d94fe4d5f64e1423ae/cfcd208495d565ef66e7dff9f98764da]'",
            "CREATE INDEX TempTest_1 ON TempTest (intField, floatField)",
            "COMMENT ON INDEX TempTest_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempTest_1]'",
            "CREATE TEMP TABLE TempAlternate (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\", CONSTRAINT TempAlternate_PK PRIMARY KEY(id))",
            "COMMENT ON TABLE TempAlternate IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempAlternate]'",
            "COMMENT ON COLUMN TempAlternate.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
            "COMMENT ON COLUMN TempAlternate.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
            "COMMENT ON COLUMN TempAlternate.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
            "CREATE INDEX TempAlternate_1 ON TempAlternate (stringField)",
            "COMMENT ON INDEX TempAlternate_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempAlternate_1]'",
            "CREATE TEMP TABLE TempNonNull (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\" NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField BOOLEAN NOT NULL, dateField DATE NOT NULL, blobField BYTEA NOT NULL, CONSTRAINT TempNonNull_PK PRIMARY KEY(id))",
            "COMMENT ON TABLE TempNonNull IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempNonNull]'",
            "COMMENT ON COLUMN TempNonNull.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
            "COMMENT ON COLUMN TempNonNull.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
            "COMMENT ON COLUMN TempNonNull.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
            "COMMENT ON COLUMN TempNonNull.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
            "COMMENT ON COLUMN TempNonNull.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
            "COMMENT ON COLUMN TempNonNull.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
            "COMMENT ON COLUMN TempNonNull.blobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays
        .asList("CREATE TABLE testschema."
            + TABLE_WITH_VERY_LONG_NAME
            + " (id NUMERIC(19) NOT NULL, version INTEGER DEFAULT 0, stringField VARCHAR(3) COLLATE \"POSIX\", intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField BOOLEAN, charField VARCHAR(1) COLLATE \"POSIX\", CONSTRAINT " + TABLE_WITH_VERY_LONG_NAME + "_PK PRIMARY KEY(id))",
            "COMMENT ON TABLE testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.id IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.version IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
            "COMMENT ON COLUMN testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation.charField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
            "CREATE UNIQUE INDEX Test_NK ON testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation (stringField)",
            "COMMENT ON INDEX Test_NK IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_NK]'",
            "CREATE UNIQUE INDEX Test_NK$null0 ON testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation ((0)) WHERE stringField IS NULL",
            "COMMENT ON INDEX Test_NK$null0 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[e069928444ba25d94fe4d5f64e1423ae/cfcd208495d565ef66e7dff9f98764da]'",
            "CREATE INDEX Test_1 ON testschema.tableWithANameThatExceedsTwentySevenCharactersToMakeSureSchemaNameDoesNotGetFactoredIntoOracleNameTruncation (intField, floatField)",
            "COMMENT ON INDEX Test_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_1]'"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTableStatements()
   */
  @Override
  protected List<String> expectedDropTableStatements() {
    return Arrays.asList("DROP TABLE testschema.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("DROP TABLE TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("TRUNCATE TABLE testschema.Test");
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
    return Arrays.asList("DELETE FROM testschema.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatement()
   */
  @Override
  protected String expectedParameterisedInsertStatement() {
    return "INSERT INTO testschema.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, 'Escap''d', 7, :floatField, 20100405, TRUE, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithTableInDifferentSchema()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithTableInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, 'Escap''d', 7, :floatField, 20100405, TRUE, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutoGenerateIdStatement()
   */
  @Override
  protected List<String> expectedAutoGenerateIdStatement() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM testschema.Test))",
      "INSERT INTO testschema.Test (version, stringField, id) SELECT version, stringField, (SELECT COALESCE(value, 0) FROM idvalues WHERE (name = 'Test')) + Other.id FROM testschema.Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM testschema.Test))",
      "INSERT INTO testschema.Test (stringField, id, version) SELECT stringField, (SELECT COALESCE(value, 0) FROM idvalues WHERE (name = 'Test')) + Other.id, 0 AS version FROM testschema.Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM idvalues where name = 'Test'",
      "INSERT INTO idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM testschema.Test))",
      "INSERT INTO testschema.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, TRUE, 'X', (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
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
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES ('Escap''d', 7, 11.25, 20100405, TRUE, 'X', (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithNoColumnValues()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithNoColumnValues() {
    return "INSERT INTO testschema.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (:id, :version, :stringField, :intField, :floatField, :dateField, :booleanField, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedEmptyStringInsertStatement()
   */
  @Override
  protected String expectedEmptyStringInsertStatement() {
    return "INSERT INTO testschema.Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT COALESCE(value, 1) FROM idvalues WHERE (name = 'Test')), 0, 0, 0, null, FALSE, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT CONCAT(assetDescriptionLine1, CASE WHEN (taxVariationIndicator = 'Y') THEN exposureCustomerNumber ELSE invoicingCustomerNumber END) AS test FROM testschema.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT CONCAT(assetDescriptionLine1, MAX(scheduleStartDate)) AS test FROM testschema.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT CONCAT('ABC', ' ', 'DEF') AS assetDescription FROM testschema.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */

  SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldReference("field1"), new ConcatenatedField(
      new FieldReference("field2"), new FieldLiteral("XYZ"))).as("test")).from(new TableReference("schedule"));



  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT CONCAT(field1, CONCAT(field2, 'XYZ')) AS test FROM testschema.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT CONCAT(assetDescriptionLine1, ' ', assetDescriptionLine2) AS assetDescription FROM testschema.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT CONCAT(assetDescriptionLine1, 'XYZ', assetDescriptionLine2) AS assetDescription FROM testschema.schedule";
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
    return "CAST(value AS VARCHAR(10)) COLLATE \"POSIX\"";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntCast()
   */
  @Override
  protected String expectedBigIntCast() {
    return "CAST(value AS NUMERIC(19))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntFunctionCast()
   */
  @Override
  protected String expectedBigIntFunctionCast() {
    return "CAST(MIN(value) AS NUMERIC(19))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanCast()
   */
  @Override
  protected String expectedBooleanCast() {
    return "CAST(value AS BOOLEAN)";
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
    return "SELECT stringField FROM testschema.Other UNION SELECT stringField FROM testschema.Test UNION ALL SELECT stringField FROM testschema.Alternate ORDER BY stringField";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedLeftPad()
   */
  @Override
  protected String expectedLeftPad() {
    return "SELECT LPAD(stringField, 10, 'j') FROM testschema.Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanLiteral(boolean) ()
   */
  @Override
  protected String expectedBooleanLiteral(boolean value) {
    return value ? "TRUE" : "FALSE";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBlobLiteral(String)  ()
   */
  @Override
  protected String expectedBlobLiteral(String value) {
    return String.format("E'\\x%s'", value);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN blobField_new BYTEA NULL",
        "COMMENT ON COLUMN testschema.Test.blobField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField_new]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN blobField SET NOT NULL",
        "COMMENT ON COLUMN testschema.Test.blobField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN booleanField SET NOT NULL",
        "COMMENT ON COLUMN testschema.Test.booleanField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN booleanField_new BOOLEAN NULL",
        "COMMENT ON COLUMN testschema.Test.booleanField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[booleanField_new]/TYPE:[BOOLEAN]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN stringField_new VARCHAR(6) COLLATE \"POSIX\" NULL",
        "COMMENT ON COLUMN testschema.Test.stringField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField_new]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN stringField TYPE VARCHAR(6) COLLATE \"POSIX\"",
        "COMMENT ON COLUMN testschema.Test.stringField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN intField_new INTEGER NULL",
        "COMMENT ON COLUMN testschema.Test.intField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField_new]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN intField SET NOT NULL",
        "COMMENT ON COLUMN testschema.Test.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN dateField_new DATE NULL",
        "COMMENT ON COLUMN testschema.Test.dateField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField_new]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN dateField SET NOT NULL",
        "COMMENT ON COLUMN testschema.Test.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN floatField_new DECIMAL(6,3) NULL",
        "COMMENT ON COLUMN testschema.Test.floatField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField_new]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN floatField DROP NOT NULL, ALTER COLUMN floatField TYPE DECIMAL(14,3)",
        "COMMENT ON COLUMN testschema.Test.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN bigIntegerField_new NUMERIC(19) NULL",
        "COMMENT ON COLUMN testschema.Test.bigIntegerField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField_new]/TYPE:[BIG_INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN bigIntegerField DROP DEFAULT",
        "COMMENT ON COLUMN testschema.Test.bigIntegerField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN dateField_new DATE DEFAULT DATE '2010-01-01' NOT NULL",
        "COMMENT ON COLUMN testschema.Test.dateField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField_new]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN dateField SET NOT NULL",
        "COMMENT ON COLUMN testschema.Test.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN floatField TYPE DECIMAL(20,3)",
        "COMMENT ON COLUMN testschema.Test.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN floatField DROP NOT NULL, ALTER COLUMN floatField TYPE DECIMAL(20,3)",
        "COMMENT ON COLUMN testschema.Test.floatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN floatField_new DECIMAL(6,3) DEFAULT 20.33 NULL",
        "COMMENT ON COLUMN testschema.Test.floatField_new IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[floatField_new]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ALTER COLUMN bigIntegerField SET DEFAULT 54321",
        "COMMENT ON COLUMN testschema.Test.bigIntegerField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'"
      );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      "DROP INDEX Test_1",
      "DROP INDEX IF EXISTS Test_1$null0",
      "CREATE INDEX Test_1 ON testschema.Test (intField)",
      "COMMENT ON INDEX Test_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_1]'",
      "ALTER TABLE testschema.Test ALTER COLUMN intField SET NOT NULL",
      "COMMENT ON COLUMN testschema.Test.intField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("CREATE INDEX indexName ON testschema.Test (id)",
                         "COMMENT ON INDEX indexName IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[indexName]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("CREATE INDEX indexName ON testschema.Test (id, version)",
                         "COMMENT ON INDEX indexName IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[indexName]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("CREATE UNIQUE INDEX indexName ON testschema.Test (id)",
                         "COMMENT ON INDEX indexName IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[indexName]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUniqueNullable()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUniqueNullable() {
    return Arrays.asList("CREATE UNIQUE INDEX indexName ON testschema.Test (stringField, intField, floatField, dateField)",
                         "COMMENT ON INDEX indexName IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[indexName]'",
                         "CREATE UNIQUE INDEX indexName$null ON testschema.Test ((stringField ||'§'|| intField ||'§'|| floatField ||'§'|| dateField)) WHERE stringField IS NULL OR intField IS NULL OR dateField IS NULL",
                         "COMMENT ON INDEX indexName$null IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[121e97b2b38f63cc6840445595b4177c]'"
    );
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
    return Arrays.asList("ALTER TABLE testschema.Test DROP CONSTRAINT Test_PK",
        "ALTER TABLE testschema.Test ADD CONSTRAINT Test_PK PRIMARY KEY(id, dateField)",
        "COMMENT ON COLUMN testschema.Test.dateField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList("ALTER TABLE testschema.CompositePrimaryKey DROP CONSTRAINT CompositePrimaryKey_PK",
        "ALTER TABLE testschema.CompositePrimaryKey ALTER COLUMN secondPrimaryKey TYPE VARCHAR(5) COLLATE \"POSIX\"",
        "ALTER TABLE testschema.CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY(id, secondPrimaryKey)",
        "COMMENT ON COLUMN testschema.CompositePrimaryKey.secondPrimaryKey IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return ImmutableList.of(
      "ALTER TABLE testschema.CompositePrimaryKey DROP CONSTRAINT CompositePrimaryKey_PK",
      "ALTER TABLE testschema.CompositePrimaryKey ALTER COLUMN secondPrimaryKey DROP NOT NULL, ALTER COLUMN secondPrimaryKey TYPE VARCHAR(5) COLLATE \"POSIX\"",
      "ALTER TABLE testschema.CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY(id)",
      "COMMENT ON COLUMN testschema.CompositePrimaryKey.secondPrimaryKey IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
        "ALTER TABLE testschema.Test DROP CONSTRAINT Test_PK",
        "ALTER TABLE testschema.Test RENAME id TO renamedId",
        "ALTER TABLE testschema.Test ADD CONSTRAINT Test_PK PRIMARY KEY(renamedId)",
        "COMMENT ON COLUMN testschema.Test.renamedId IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[renamedId]/TYPE:[BIG_INTEGER]'"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList("ALTER TABLE testschema.Other RENAME floatField TO blahField",
        "ALTER TABLE testschema.Other ALTER COLUMN blahField DROP NOT NULL, ALTER COLUMN blahField TYPE DECIMAL(20,3)",
        "COMMENT ON COLUMN testschema.Other.blahField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[blahField]/TYPE:[DECIMAL]'"
      );
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE testschema.Test ADD COLUMN stringField_with_default VARCHAR(6) COLLATE \"POSIX\" DEFAULT 'N' NOT NULL",
        "COMMENT ON COLUMN testschema.Test.stringField_with_default IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[stringField_with_default]/TYPE:[STRING]'");
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewStatements()
   */
  @Override
  protected List<String> expectedCreateViewStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName("Test") + " WHERE (stringField = 'blah'))",
                         "COMMENT ON VIEW TestView IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TestView]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateViewOverUnionSelectStatements()
   */
  @Override
  protected List<String> expectedCreateViewOverUnionSelectStatements() {
    return Arrays.asList(
      "CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah') UNION ALL SELECT stringField FROM " + tableName(OTHER_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah'))",
      "COMMENT ON VIEW TestView IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TestView]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedYYYYMMDDToDate()
   */
  @Override
  protected String expectedYYYYMMDDToDate() {
    return "TO_DATE(('20100101') :: TEXT,'YYYYMMDD')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmdd()
   */
  @Override
  protected String expectedDateToYyyymmdd() {
    return "TO_CHAR(testField,'YYYYMMDD') :: NUMERIC";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "TO_CHAR(testField,'YYYYMMDDHH24MISS') :: NUMERIC";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "NOW()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatement()
   */
  @Override
  protected List<String> expectedDropViewStatements() {
    return Arrays.asList("DROP VIEW IF EXISTS " + tableName("TestView") + " CASCADE");
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
    return "CAST(MIN(field) AS VARCHAR(8)) COLLATE \"POSIX\"";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDaysBetween()
   */
  @Override
  protected String expectedDaysBetween() {
    return "SELECT (dateTwo) - (dateOne) FROM testschema.MyTable";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "INSERT INTO testschema.foo (id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM testschema.somewhere"
        + " ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
    return "INSERT INTO testschema.foo (id, bar)"
        + " SELECT somewhere.newId AS id, join.joinBar AS bar FROM testschema.somewhere INNER JOIN testschema.join ON (somewhere.newId = join.joinId)"
        + " ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "INSERT INTO testschema.foo (id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM MYSCHEMA.somewhere"
        + " ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeTargetInDifferentSchema()
   */
  @Override
  protected String expectedMergeTargetInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.foo (id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM testschema.somewhere"
        + " ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "INSERT INTO testschema.foo (id)"
        + " SELECT somewhere.newId AS id FROM testschema.somewhere"
        + " ON CONFLICT (id) DO NOTHING";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeWithUpdateExpressions()
   */
  @Override
  protected String expectedMergeWithUpdateExpressions() {
    return "INSERT INTO testschema.foo (id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM testschema.somewhere"
        + " ON CONFLICT (id) DO UPDATE SET bar = EXCLUDED.bar + foo.bar";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddDays()
   */
  @Override
  protected String expectedAddDays() {
    return "(((testField) + (-20) * INTERVAL '1 DAY') :: DATE)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddMonths()
   */
  @Override
  protected String expectedAddMonths() {
    return "(((testField) + (-3) * INTERVAL '1 MONTH') :: DATE)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromSimpleKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromSimpleKeyStatements() {
    return Collections.singletonList("ALTER TABLE testschema.Test DROP COLUMN id");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of(
      "ALTER TABLE testschema.Test RENAME TO Renamed",
      "ALTER INDEX testschema.Test_pk RENAME TO Renamed_pk",
      "COMMENT ON INDEX Renamed_pk IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Renamed_pk]'",
      "COMMENT ON TABLE testschema.Renamed IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Renamed]'");
  }


  /**
   * @return the expected statements for renaming a table with a long name.
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of(
      "ALTER TABLE testschema.123456789012345678901234567890XXX RENAME TO Blah",
      "ALTER INDEX testschema.123456789012345678901234567890XXX_pk RENAME TO Blah_pk",
      "COMMENT ON INDEX Blah_pk IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Blah_pk]'",
      "COMMENT ON TABLE testschema.Blah IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Blah]'"
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of("ALTER INDEX testschema.Test_1 RENAME TO Test_2",
                            "COMMENT ON INDEX Test_2 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[Test_2]'",
                            "ALTER INDEX IF EXISTS testschema.Test_1$null0 RENAME TO Test_2$null0"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameTempIndexStatements() {
    return ImmutableList.of("ALTER INDEX TempTest_1 RENAME TO TempTest_2",
                            "COMMENT ON INDEX TempTest_2 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[TempTest_2]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomString()
   */
  @Override
  protected String expectedRandomString() {
    return "UPPER(SUBSTRING(MD5(RANDOM() :: TEXT), 1, (10) :: INT))";
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
      "CREATE TABLE testschema.SomeTable (someField VARCHAR(3) COLLATE \"POSIX\" NOT NULL, otherField DECIMAL(3,0) NOT NULL, CONSTRAINT SomeTable_PK PRIMARY KEY(someField))",
      "COMMENT ON TABLE testschema.SomeTable IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[SomeTable]'",
      "COMMENT ON COLUMN testschema.SomeTable.someField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[someField]/TYPE:[STRING]'",
      "COMMENT ON COLUMN testschema.SomeTable.otherField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[otherField]/TYPE:[DECIMAL]'",
      "CREATE INDEX SomeTable_1 ON testschema.SomeTable (otherField)",
      "COMMENT ON INDEX SomeTable_1 IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[SomeTable_1]'",
      "INSERT INTO testschema.SomeTable SELECT someField, otherField FROM testschema.OtherTable"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT /*+ IndexScan(Foo foo_1) IndexScan(aliased foo_2) */ * FROM SCHEMA2.Foo INNER JOIN testschema.Bar ON (a = b) LEFT OUTER JOIN testschema.Fo ON (a = b) INNER JOIN testschema.Fum Fumble ON (a = b) ORDER BY a";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints2(int)
   */
  @Override
  protected String expectedHints2(int rowCount) {
    return "SELECT /*+ IndexScan(Foo foo_1) */ a, b FROM " + tableName("Foo") + " ORDER BY a FOR UPDATE";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints7()
   */
  @Override
  protected String expectedHints7() {
    return "SELECT /*+ Set(random_page_cost 2.0) */ * FROM SCHEMA2.Foo";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#provideCustomHint()
   */
  @Override
  protected CustomHint provideCustomHint() {
    return new PostgreSQLCustomHint("Set(random_page_cost 2.0)");
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableDropColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return Collections.singletonList("ALTER TABLE testschema.Test DROP COLUMN bigIntegerField");
  }

  /**
   * @return The expected SQL for performing an update with a source table which lives in a different schema.
   */
  @Override
  protected String expectedUpdateUsingSourceTableInDifferentSchema() {
    return "UPDATE " + tableName("FloatingRateRate") + " A SET settlementFrequency = (SELECT settlementFrequency FROM MYSCHEMA.FloatingRateDetail B WHERE (A.floatingRateDetailId = B.id))";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#supportsWindowFunctions()
   */
  @Override
  protected boolean supportsWindowFunctions() {
    return true;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAnalyseTableSql()
   */
  @Override
  protected Collection<String> expectedAnalyseTableSql() {
    return Arrays.asList("ANALYZE TempTest");
  }


  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("ALTER TABLE testschema.Other ALTER COLUMN FloatField TYPE DECIMAL(20,3)",
        "COMMENT ON COLUMN testschema.Other.FloatField IS '"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":[FloatField]/TYPE:[DECIMAL]'");
  }


  @Override
  protected void verifyBlobColumnCallPrepareStatementParameter(SqlParameter blobColumn) throws SQLException {
    verify(callPrepareStatementParameter(blobColumn, null)).setBinaryStream(Mockito.eq(blobColumn), any(InputStream.class));
    verify(callPrepareStatementParameter(blobColumn, "QUJD")).setBinaryStream(Mockito.eq(blobColumn), any(InputStream.class));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#tableName(java.lang.String)
   */
  @Override
  protected String tableName(String baseName) {
    return "testschema." + baseName;
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectSome()
   */
  @Override
  protected String expectedSelectSome() {
    return "SELECT BOOL_OR(booleanField) FROM testschema.Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectEvery()
   */
  @Override
  protected String expectedSelectEvery() {
    return "SELECT BOOL_AND(booleanField) FROM testschema.Test";
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndWhere(String value) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ctid IN (" +
      "SELECT ctid FROM " + tableName(TEST_TABLE) + " WHERE (" + TEST_TABLE + ".stringField = " + stringLiteralPrefix() + value +
      ") LIMIT 1000)";
  };


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndComplexWhere(String value1, String value2) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ctid IN (" +
      "SELECT ctid FROM " + tableName(TEST_TABLE) + " WHERE ((Test.stringField = " + stringLiteralPrefix() + value1 + ") OR (Test.stringField = " + stringLiteralPrefix() + value2 + "))" +
      " LIMIT 1000)";
  };


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitWithoutWhere() {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ctid IN (" +
      "SELECT ctid FROM " + tableName(TEST_TABLE) +
      " LIMIT 1000)";
  };


  @Override
  protected SchemaResource createSchemaResourceForSchemaConsistencyStatements() {
    final List<Table> tables = ImmutableList.of(
      table("TableOne")
        .columns(
          column("id", DataType.BIG_INTEGER),
          column("u", DataType.BIG_INTEGER).nullable(),
          column("v", DataType.BIG_INTEGER).nullable(),
          column("x", DataType.BIG_INTEGER).nullable())
        .indexes(
          index("TableOne_1").columns("u").unique(),
          index("TableOne_2").columns("u", "v", "x").unique(),
          index("TableOne_3").columns("x").unique()
        ),
      table("TableTwo")
        .columns(
          column("id", DataType.BIG_INTEGER),
          column("x", DataType.BIG_INTEGER).nullable())
        .indexes(
          index("TableTwo_3").columns("x").unique()
        )
    );

    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.tables()).thenReturn(tables);
    when(metaDataProvider.getAdditionalConstraintIndexes("tableone_1")).thenReturn(ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableOne_1$null0", "xx/yy").get(),
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableOne_1$null1", "xx/yy").get()
    ));
    when(metaDataProvider.getAdditionalConstraintIndexes("tableone_3")).thenReturn(ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableOne_3$null0", "76e4a5e9bce3c23b4feb0675fb0c8366/cfcd208495d565ef66e7dff9f98764da").get()
    ));
    when(metaDataProvider.getAdditionalConstraintIndexes("tabletwo_3")).thenReturn(ImmutableList.of(
      PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex("TableTwo_3$null0", "76e4a5e9bce3c23b4feb0675fb0c8366/cfcd208495d565ef66e7dff9f98764da").get()
    ));

    final SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getDatabaseMetaDataProvider()).thenReturn(Optional.of(metaDataProvider));

    return schemaResource;
  }

  @Override
  protected org.hamcrest.Matcher<java.lang.Iterable<? extends String>> expectedSchemaConsistencyStatements() {
    return contains(
      "-- Healing table: TableOne",
      "DROP INDEX IF EXISTS tableone_1$null0",
      "DROP INDEX IF EXISTS tableone_1$null1",
      "CREATE UNIQUE INDEX TableOne_1$null0 ON testschema.TableOne ((0)) WHERE u IS NULL",
      "COMMENT ON INDEX TableOne_1$null0 IS 'REALNAME:[6285d92226e5112908a10cd51e129b72/cfcd208495d565ef66e7dff9f98764da]'",
      "CREATE UNIQUE INDEX TableOne_2$null ON testschema.TableOne ((u ||'§'|| v ||'§'|| x)) WHERE u IS NULL OR v IS NULL OR x IS NULL",
      "COMMENT ON INDEX TableOne_2$null IS 'REALNAME:[47d0d3041096c2ac2e8cbb4a8d0e3fd8]'"
    );
  }
}
