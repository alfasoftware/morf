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

package org.alfasoftware.morf.jdbc.oracle;

import static org.alfasoftware.morf.jdbc.oracle.OracleDialect.NULLS_LAST;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.element.Direction.ASCENDING;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.sql.CustomHint;
import org.alfasoftware.morf.sql.OracleCustomHint;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * Test that {@link OracleDialect} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestOracleDialect extends AbstractSqlDialectTest {

  /**
   * Table name truncated to 30 characters.
   */
  private static final String LONG_TABLE_NAME_TRUNCATED_30 = "tableWithANameThatExceedsTwent";

  /**
   * Table name truncated to 27 characters.
   */
  private static final String LONG_TABLE_NAME_TRUNCATED_27 = "tableWithANameThatExceedsTw";

  @SuppressWarnings({"unchecked","rawtypes"})
  private final ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass((Class<List<String>>)(Class)List.class);


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#createTestDialect()
   */
  @Override
  protected SqlDialect createTestDialect() {
    return new OracleDialect("testschema");
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
          "CREATE TABLE TESTSCHEMA.Test (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3), intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField DECIMAL(1,0), charField NVARCHAR2(1), blobField BLOB, bigIntegerField NUMBER(19) DEFAULT 12345, clobField NCLOB, CONSTRAINT Test_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Test_PK ON TESTSCHEMA.Test (id)))",
          "COMMENT ON TABLE TESTSCHEMA.Test IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[Test]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.charField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.blobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.bigIntegerField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Test.clobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[clobField]/TYPE:[CLOB]'",
          "CREATE UNIQUE INDEX TESTSCHEMA.Test_NK ON TESTSCHEMA.Test (stringField)",
          "CREATE UNIQUE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField, floatField)",
          "CREATE TABLE TESTSCHEMA.Alternate (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3), CONSTRAINT Alternate_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Alternate_PK ON TESTSCHEMA.Alternate (id)))",
          "COMMENT ON TABLE TESTSCHEMA.Alternate IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[Alternate]'",
          "COMMENT ON COLUMN TESTSCHEMA.Alternate.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Alternate.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.Alternate.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "CREATE INDEX TESTSCHEMA.Alternate_1 ON TESTSCHEMA.Alternate (stringField)",
          "CREATE TABLE TESTSCHEMA.NonNull (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField DECIMAL(1,0) NOT NULL, dateField DATE NOT NULL, blobField BLOB NOT NULL, CONSTRAINT NonNull_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.NonNull_PK ON TESTSCHEMA.NonNull (id)))",
          "COMMENT ON TABLE TESTSCHEMA.NonNull IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[NonNull]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN TESTSCHEMA.NonNull.blobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
          "CREATE TABLE TESTSCHEMA.CompositePrimaryKey (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3) NOT NULL, secondPrimaryKey NVARCHAR2(3) NOT NULL, CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id, secondPrimaryKey) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.CompositePrimaryKey_PK ON TESTSCHEMA.CompositePrimaryKey (id, secondPrimaryKey)))",
          "COMMENT ON TABLE TESTSCHEMA.CompositePrimaryKey IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[CompositePrimaryKey]'",
          "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.secondPrimaryKey IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'",

          "CREATE TABLE TESTSCHEMA.AutoNumber (intField NUMBER(19), CONSTRAINT AutoNumber_PK PRIMARY KEY (intField) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.AutoNumber_PK ON TESTSCHEMA.AutoNumber (intField)))",

          "DECLARE \n" +
          "  e exception; \n" +
          "  pragma exception_init(e,-4080); \n" +
          "BEGIN \n" +
          "  EXECUTE IMMEDIATE 'DROP TRIGGER TESTSCHEMA.AUTONUMBER_TG'; \n" +
          "EXCEPTION \n" +
          "  WHEN e THEN \n" +
          "    null; \n" +
          "END;",

          "DECLARE \n" +
          "  query CHAR(255); \n" +
          "BEGIN \n" +
          "  select queryField into query from SYS.DUAL D left outer join (\n" +
          "    select concat('drop sequence TESTSCHEMA.', sequence_name) as queryField \n" +
          "    from ALL_SEQUENCES S \n" +
          "    where S.sequence_owner='TESTSCHEMA' AND S.sequence_name = 'AUTONUMBER_SQ' \n" +
          "  ) on 1 = 1; \n" +
          "  IF query is not null THEN \n" +
          "    execute immediate query; \n" +
          "  END IF; \n" +
          "END;",

          "CREATE SEQUENCE TESTSCHEMA.AUTONUMBER_SQ START WITH 5 CACHE 2000",

          "ALTER SESSION SET CURRENT_SCHEMA = testschema",

          "CREATE TRIGGER TESTSCHEMA.AUTONUMBER_TG \n" +
          "BEFORE INSERT ON AutoNumber FOR EACH ROW \n" +
          "BEGIN \n" +
          "  IF (:new.intField IS NULL) THEN \n" +
          "    SELECT AUTONUMBER_SQ.nextval \n" +
          "    INTO :new.intField \n" +
          "    FROM DUAL; \n" +
          "  END IF; \n" +
          "END;",

          "COMMENT ON TABLE TESTSCHEMA.AutoNumber IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[AutoNumber]'",
          "COMMENT ON COLUMN TESTSCHEMA.AutoNumber.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[BIG_INTEGER]/AUTONUMSTART:[5]'"
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
          "CREATE GLOBAL TEMPORARY TABLE TESTSCHEMA.TempTest (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3), intField INTEGER, floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField DECIMAL(1,0), charField NVARCHAR2(1), blobField BLOB, bigIntegerField NUMBER(19) DEFAULT 12345, clobField NCLOB, CONSTRAINT TempTest_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.TempTest_PK ON TESTSCHEMA.TempTest (id))) ON COMMIT PRESERVE ROWS",
          "COMMENT ON TABLE TESTSCHEMA.TempTest IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[TempTest]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.charField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.blobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.bigIntegerField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempTest.clobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[clobField]/TYPE:[CLOB]'",
          "CREATE UNIQUE INDEX TESTSCHEMA.TempTest_NK ON TESTSCHEMA.TempTest (stringField)",
          "CREATE INDEX TESTSCHEMA.TempTest_1 ON TESTSCHEMA.TempTest (intField, floatField)",
          "CREATE GLOBAL TEMPORARY TABLE TESTSCHEMA.TempAlternate (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3), CONSTRAINT TempAlternate_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.TempAlternate_PK ON TESTSCHEMA.TempAlternate (id))) ON COMMIT PRESERVE ROWS",
          "COMMENT ON TABLE TESTSCHEMA.TempAlternate IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[TempAlternate]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempAlternate.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempAlternate.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempAlternate.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "CREATE INDEX TESTSCHEMA.TempAlternate_1 ON TESTSCHEMA.TempAlternate (stringField)",
          "CREATE GLOBAL TEMPORARY TABLE TESTSCHEMA.TempNonNull (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3) NOT NULL, intField DECIMAL(8,0) NOT NULL, booleanField DECIMAL(1,0) NOT NULL, dateField DATE NOT NULL, blobField BLOB NOT NULL, CONSTRAINT TempNonNull_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.TempNonNull_PK ON TESTSCHEMA.TempNonNull (id))) ON COMMIT PRESERVE ROWS",
          "COMMENT ON TABLE TESTSCHEMA.TempNonNull IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[TempNonNull]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN TESTSCHEMA.TempNonNull.blobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedCreateTableStatementsWithLongTableName()
   */
  @Override
  protected List<String> expectedCreateTableStatementsWithLongTableName() {
    return Arrays
        .asList(
          "CREATE TABLE TESTSCHEMA."
              + LONG_TABLE_NAME_TRUNCATED_30
              + " (id NUMBER(19) NOT NULL, version INTEGER DEFAULT 0, stringField NVARCHAR2(3), intField DECIMAL(8,0), floatField DECIMAL(13,2) NOT NULL, dateField DATE, booleanField DECIMAL(1,0), charField NVARCHAR2(1), CONSTRAINT "
              + LONG_TABLE_NAME_TRUNCATED_27 + "_PK PRIMARY KEY (id)"
              + " USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_27 + "_PK ON TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + " (id)))",
          "COMMENT ON TABLE TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + " IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[" + LONG_TABLE_NAME_TRUNCATED_30 + "]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".id IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[id]/TYPE:[BIG_INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".version IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[version]/TYPE:[INTEGER]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30 + ".dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'",
          "COMMENT ON COLUMN TESTSCHEMA." + LONG_TABLE_NAME_TRUNCATED_30
              + ".booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'", "COMMENT ON COLUMN TESTSCHEMA."
              + LONG_TABLE_NAME_TRUNCATED_30 + ".charField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[charField]/TYPE:[STRING]'",
          "CREATE UNIQUE INDEX TESTSCHEMA.Test_NK ON TESTSCHEMA."+LONG_TABLE_NAME_TRUNCATED_30+" (stringField)",
          "CREATE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.tableWithANameThatExceedsTwent (intField, floatField)"
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
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropSingleTable()
   */
  @Override
  protected List<String> expectedDropSingleTable() {
    return Arrays.asList("DROP TABLE TESTSCHEMA.Test");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTables()
   */
  @Override
  protected List<String> expectedDropTables() {
    return Arrays.asList("BEGIN\n" +
            "  FOR T IN (\n" +
            "    SELECT 'TESTSCHEMA.' || TABLE_NAME AS TABLE_NAME\n" +
            "    FROM (SELECT COLUMN_VALUE AS TABLE_NAME from TABLE(SYS.dbms_debug_vc2coll('TEST', 'OTHER')))\n" +
            "  )\n" +
            "  LOOP\n" +
            "    EXECUTE IMMEDIATE 'DROP TABLE ' || T.TABLE_NAME ;\n" +
            "  END LOOP;\n" +
            "END;");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTablesWithParameters()
   */
  @Override
  protected List<String> expectedDropTablesWithParameters() {
    return Arrays.asList("BEGIN\n" +
            "  FOR T IN (\n" +
            "    SELECT 'TESTSCHEMA.' || TABLE_NAME AS TABLE_NAME\n" +
            "    FROM ALL_TABLES\n" +
            "   WHERE TABLE_NAME  IN ('TEST', 'OTHER')\n" +
            "  )\n" +
            "  LOOP\n" +
            "    EXECUTE IMMEDIATE 'DROP TABLE ' || T.TABLE_NAME || ' CASCADE CONSTRAINTS ' ;\n" +
            "  END LOOP;\n" +
            "END;");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropTempTableStatements()
   */
  @Override
  protected List<String> expectedDropTempTableStatements() {
    return Arrays.asList("DROP TABLE TESTSCHEMA.TempTest");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTableStatements() {
    return Arrays.asList("TRUNCATE TABLE TESTSCHEMA.Test REUSE STORAGE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedTruncateTempTableStatements()
   */
  @Override
  protected List<String> expectedTruncateTempTableStatements() {
    return Arrays.asList("TRUNCATE TABLE TESTSCHEMA.TempTest");
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
    return "INSERT INTO TESTSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, N'Escap''d', 7, :floatField, 20100405, 1, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedParameterisedInsertStatementWithTableInDifferentSchema()
   */
  @Override
  protected String expectedParameterisedInsertStatementWithTableInDifferentSchema() {
    return "INSERT INTO MYSCHEMA.Test (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (5, :version, N'Escap''d', 7, :floatField, 20100405, 1, :charField, :blobField, :bigIntegerField, :clobField)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutoGenerateIdStatement()
   */
  @Override
  protected List<String> expectedAutoGenerateIdStatement() {
    return ImmutableList.of(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (version, stringField, id) SELECT version, stringField, (SELECT COALESCE(value, 0) FROM TESTSCHEMA.idvalues WHERE (name = N'Test')) + Other.id FROM TESTSCHEMA.Other"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedInsertWithIdAndVersion()
   */
  @Override
  protected List<String> expectedInsertWithIdAndVersion() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (stringField, id, version) SELECT stringField, (SELECT COALESCE(value, 0) FROM TESTSCHEMA.idvalues WHERE (name = N'Test')) + Other.id, 0 AS version FROM TESTSCHEMA.Other"
    );
  }



  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyPostInsertStatementsInsertingUnderAutonumLimit(org.alfasoftware.morf.jdbc.SqlScriptExecutor, java.sql.Connection)
   */
  @Override
  protected void verifyPostInsertStatementsNotInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor, Connection connection) {
    verify(sqlScriptExecutor,times(2)).execute(listCaptor.capture(),eq(connection));
    assertThat(listCaptor.getAllValues().get(0),contains("DECLARE \n" +
        "  e exception; \n" +
        "  pragma exception_init(e,-4080); \n" +
        "BEGIN \n" +
        "  EXECUTE IMMEDIATE 'DROP TRIGGER TESTSCHEMA.TEST_TG'; \n" +
        "EXCEPTION \n" +
        "  WHEN e THEN \n" +
        "    null; \n" +
        "END;"));
    assertThat(listCaptor.getAllValues().get(1),contains("DECLARE \n" +
        "  e exception; \n" +
        "  pragma exception_init(e,-4080); \n" +
        "BEGIN \n" +
        "  EXECUTE IMMEDIATE 'DROP TRIGGER TESTSCHEMA.AUTONUMBER_TG'; \n" +
        "EXCEPTION \n" +
        "  WHEN e THEN \n" +
        "    null; \n" +
        "END;",

        "DECLARE \n" +
        "  query CHAR(255); \n" +
        "BEGIN \n" +
        "  select queryField into query from SYS.DUAL D left outer join (\n" +
        "    select concat('drop sequence TESTSCHEMA.', sequence_name) as queryField \n" +
        "    from ALL_SEQUENCES S \n" +
        "    where S.sequence_owner='TESTSCHEMA' AND S.sequence_name = 'AUTONUMBER_SQ' \n" +
        "  ) on 1 = 1; \n" +
        "  IF query is not null THEN \n" +
        "    execute immediate query; \n" +
        "  END IF; \n" +
        "END;",

        "DECLARE query CHAR(255); \n" +
        "BEGIN \n" +
        "  SELECT 'CREATE SEQUENCE TESTSCHEMA.AUTONUMBER_SQ START WITH ' || TO_CHAR(GREATEST(5, MAX(id)+1)) || ' CACHE 2000' INTO QUERY FROM \n" +
        "    (SELECT MAX(intField) AS id FROM TESTSCHEMA.AutoNumber UNION SELECT 0 AS id FROM SYS.DUAL); \n" +
        "  EXECUTE IMMEDIATE query; \n" +
        "END;",

        "ALTER SESSION SET CURRENT_SCHEMA = testschema",

        "CREATE TRIGGER TESTSCHEMA.AUTONUMBER_TG \n" +
        "BEFORE INSERT ON AutoNumber FOR EACH ROW \n" +
        "BEGIN \n" +
        "  IF (:new.intField IS NULL) THEN \n" +
        "    SELECT AUTONUMBER_SQ.nextval \n" +
        "    INTO :new.intField \n" +
        "    FROM DUAL; \n" +
        "  END IF; \n" +
        "END;"));

    verifyNoMoreInteractions(sqlScriptExecutor);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#tableName(java.lang.String)
   */
  @Override
  protected String tableName(String baseName) {
    return "TESTSCHEMA." + baseName;
  }


  /**
   * Use N'' prefix for NLS literals. {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#stringLiteralPrefix()
   */
  @Override
  protected String stringLiteralPrefix() {
    return "N";
  }


  /**
   * Use ESCAPE'\' suffix for literals following <a href="http://docs.oracle.com/database/121/SQLRF/conditions007.htm"> Oracle documentation
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#likeEscapeSuffix()
   */
  @Override
  protected String likeEscapeSuffix() {
    return " ESCAPE '\\'";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsert()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsert() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM TESTSCHEMA.Test))",
      "INSERT INTO TESTSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (N'Escap''d', 7, 11.25, 20100405, 1, N'X', (SELECT COALESCE(value, 1) FROM TESTSCHEMA.idvalues WHERE (name = N'Test')), 0, null, 12345, null)"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSpecifiedValueInsertWithTableInDifferentSchema()
   */
  @Override
  protected List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema() {
    return Arrays.asList(
      "DELETE FROM TESTSCHEMA.idvalues where name = 'Test'",
      "INSERT INTO TESTSCHEMA.idvalues (name, value) VALUES('Test', (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM MYSCHEMA.Test))",
      "INSERT INTO MYSCHEMA.Test (stringField, intField, floatField, dateField, booleanField, charField, id, version, blobField, bigIntegerField, clobField) VALUES (N'Escap''d', 7, 11.25, 20100405, 1, N'X', (SELECT COALESCE(value, 1) FROM TESTSCHEMA.idvalues WHERE (name = N'Test')), 0, null, 12345, null)"
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
    return "INSERT INTO TESTSCHEMA.Test (stringField, id, version, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) VALUES (NULL, (SELECT COALESCE(value, 1) FROM TESTSCHEMA.idvalues WHERE (name = N'Test')), 0, 0, 0, null, 0, NULL, null, 12345, null)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation1()
   */
  @Override
  protected String expectedSelectWithConcatenation1() {
    return "SELECT assetDescriptionLine1 || N' ' || assetDescriptionLine2 AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithConcatenation2()
   */
  @Override
  protected String expectedSelectWithConcatenation2() {
    return "SELECT assetDescriptionLine1 || N'XYZ' || assetDescriptionLine2 AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithCase()
   */
  @Override
  protected String expectedConcatenationWithCase() {
    return "SELECT assetDescriptionLine1 || CASE WHEN (taxVariationIndicator = N'Y') THEN exposureCustomerNumber ELSE invoicingCustomerNumber END AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithFunction()
   */
  @Override
  protected String expectedConcatenationWithFunction() {
    return "SELECT assetDescriptionLine1 || MAX(scheduleStartDate) AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedConcatenationWithMultipleFieldLiterals()
   */
  @Override
  protected String expectedConcatenationWithMultipleFieldLiterals() {
    return "SELECT N'ABC' || N' ' || N'DEF' AS assetDescription FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNestedConcatenations()
   */
  @Override
  protected String expectedNestedConcatenations() {
    return "SELECT field1 || field2 || N'XYZ' AS test FROM TESTSCHEMA.schedule";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIsNull()
   */
  @Override
  protected String expectedIsNull() {
    return "nvl(N'A', N'B') ";
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
    return "CAST(value AS NVARCHAR2(10))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntCast()
   */
  @Override
  protected String expectedBigIntCast() {
    return "CAST(value AS NUMBER(19))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBigIntCast()
   */
  @Override
  protected String expectedBigIntFunctionCast() {
    return "CAST(MIN(value) AS NUMBER(19))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBooleanCast()
   */
  @Override
  protected String expectedBooleanCast() {
    return "CAST(value AS DECIMAL(1,0))";
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
    return "SELECT stringField FROM TESTSCHEMA.Other UNION SELECT stringField FROM TESTSCHEMA.Test UNION ALL SELECT stringField FROM TESTSCHEMA.Alternate ORDER BY stringField NULLS FIRST";
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#nullOrder()
   */
  @Override
  protected String nullOrder() {
    return OracleDialect.DEFAULT_NULL_ORDER;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddStringColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (stringField_new NVARCHAR2(6) NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.stringField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField_new]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterStringColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterStringColumnStatement() {
    return Arrays.asList(
      "DROP INDEX TESTSCHEMA.Test_NK",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (stringField NVARCHAR2(6))",
      "CREATE UNIQUE INDEX TESTSCHEMA.Test_NK ON TESTSCHEMA.Test (stringField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_NK NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.stringField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBigIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (bigIntegerField_new NUMBER(19) NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.bigIntegerField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField_new]/TYPE:[BIG_INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBigIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBigIntegerColumnStatement() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.Test MODIFY (bigIntegerField DEFAULT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.bigIntegerField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (blobField_new BLOB NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.blobField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField_new]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBlobColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBlobColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test MODIFY (blobField  NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.blobField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blobField]/TYPE:[BLOB]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnSingleColumn()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnSingleColumn() {
    return Arrays.asList("CREATE INDEX TESTSCHEMA.indexName ON TESTSCHEMA.Test (id) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.indexName NOPARALLEL LOGGING");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsOnMultipleColumns()
   */
  @Override
  protected List<String> expectedAddIndexStatementsOnMultipleColumns() {
    return Arrays.asList("CREATE INDEX TESTSCHEMA.indexName ON TESTSCHEMA.Test (id, version) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.indexName NOPARALLEL LOGGING");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUnique()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUnique() {
    return Arrays.asList("CREATE UNIQUE INDEX TESTSCHEMA.indexName ON TESTSCHEMA.Test (id) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.indexName NOPARALLEL LOGGING");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddIndexStatementsUniqueNullable()
   */
  @Override
  protected List<String> expectedAddIndexStatementsUniqueNullable() {
    return Arrays.asList("CREATE UNIQUE INDEX TESTSCHEMA.indexName ON TESTSCHEMA.Test (stringField, intField, floatField, dateField) PARALLEL NOLOGGING",
        "ALTER INDEX TESTSCHEMA.indexName NOPARALLEL LOGGING");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement() {
    return Arrays.asList(
      "DROP INDEX TESTSCHEMA.Test_1",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (floatField DECIMAL(20,3))",
      "CREATE UNIQUE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField, floatField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test MODIFY (booleanField  NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.booleanField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField]/TYPE:[BOOLEAN]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddBooleanColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddBooleanColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (booleanField_new DECIMAL(1,0) NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.booleanField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[booleanField_new]/TYPE:[BOOLEAN]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddIntegerColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (intField_new INTEGER NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.intField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField_new]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterIntegerColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterIntegerColumnStatement() {
    return Arrays.asList(
      "DROP INDEX TESTSCHEMA.Test_1",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (intField  NOT NULL)",
      "CREATE UNIQUE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField, floatField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDateColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (dateField_new DATE NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.dateField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField_new]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDateColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDateColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test MODIFY (dateField  NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddDecimalColumnStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (floatField_new DECIMAL(6,3) NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.floatField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField_new]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterDecimalColumnStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterDecimalColumnStatement() {
    return Arrays.asList(
      "DROP INDEX TESTSCHEMA.Test_1",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (floatField DECIMAL(14,3) NULL)",
      "CREATE UNIQUE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField, floatField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnNotNullableStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (dateField_new DATE DEFAULT DATE '2010-01-01' NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.dateField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField_new]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNullableToNotNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test MODIFY (dateField  NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnFromNotNullableToNullableStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement() {
    return Arrays.asList(
      "DROP INDEX TESTSCHEMA.Test_1",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (floatField DECIMAL(20,3) NULL)",
      "CREATE UNIQUE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField, floatField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.floatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAddColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAddColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (floatField_new DECIMAL(6,3) DEFAULT 20.33 NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.floatField_new IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[floatField_new]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableAlterColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableAlterColumnWithDefaultStatement() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test MODIFY (bigIntegerField  DEFAULT 54321)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.bigIntegerField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[bigIntegerField]/TYPE:[BIG_INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterTableDropColumnWithDefaultStatement()
   */
  @Override
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return Collections.singletonList("ALTER TABLE TESTSCHEMA.Test SET UNUSED (bigIntegerField)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement()
   */
  @Override
  protected List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement() {
    return Arrays.asList(
      // dropIndexStatements & addIndexStatements
      "DROP INDEX TESTSCHEMA.Test_1",
      "CREATE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (intField) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      // changeColumnStatements
      "DROP INDEX TESTSCHEMA.Test_1",
      "ALTER TABLE TESTSCHEMA.Test MODIFY (intField  NOT NULL)",
      "CREATE INDEX TESTSCHEMA.Test_1 ON TESTSCHEMA.Test (INTFIELD) PARALLEL NOLOGGING",
      "ALTER INDEX TESTSCHEMA.Test_1 NOPARALLEL LOGGING",
      "COMMENT ON COLUMN TESTSCHEMA.Test.intField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[intField]/TYPE:[INTEGER]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedIndexDropStatements()
   */
  @Override
  protected List<String> expectedIndexDropStatements() {
    return Arrays.asList("DROP INDEX TESTSCHEMA.indexName");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnMakePrimaryStatements()
   */
  @Override
  protected List<String> expectedAlterColumnMakePrimaryStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.Test DROP PRIMARY KEY DROP INDEX",
      "ALTER TABLE TESTSCHEMA.Test ADD CONSTRAINT Test_PK PRIMARY KEY (id, dateField) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Test_PK ON TESTSCHEMA.Test (id, dateField))",
      "COMMENT ON COLUMN TESTSCHEMA.Test.dateField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[dateField]/TYPE:[DATE]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey DROP PRIMARY KEY DROP INDEX",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey MODIFY (secondPrimaryKey NVARCHAR2(5))",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id, secondPrimaryKey) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.CompositePrimaryKey_PK ON TESTSCHEMA.CompositePrimaryKey (id, secondPrimaryKey))",
      "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.secondPrimaryKey IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromCompositeKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromCompositeKeyStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey DROP PRIMARY KEY DROP INDEX",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey MODIFY (secondPrimaryKey NVARCHAR2(5) NULL)",
      "ALTER TABLE TESTSCHEMA.CompositePrimaryKey ADD CONSTRAINT CompositePrimaryKey_PK PRIMARY KEY (id) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.CompositePrimaryKey_PK ON TESTSCHEMA.CompositePrimaryKey (id))",
      "COMMENT ON COLUMN TESTSCHEMA.CompositePrimaryKey.secondPrimaryKey IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[secondPrimaryKey]/TYPE:[STRING]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterPrimaryKeyColumnStatements()
   */
  @Override
  protected List<String> expectedAlterPrimaryKeyColumnStatements() {
    return Arrays.asList(
      "ALTER TABLE TESTSCHEMA.Test DROP PRIMARY KEY DROP INDEX",
      "ALTER TABLE TESTSCHEMA.Test RENAME COLUMN id TO renamedId",
      "ALTER TABLE TESTSCHEMA.Test ADD CONSTRAINT Test_PK PRIMARY KEY (renamedId) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Test_PK ON TESTSCHEMA.Test (renamedId))",
      "COMMENT ON COLUMN TESTSCHEMA.Test.renamedId IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[renamedId]/TYPE:[BIG_INTEGER]'"
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnRenamingAndChangingNullability()
   */
  @Override
  protected List<String> expectedAlterColumnRenamingAndChangingNullability() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Other RENAME COLUMN floatField TO blahField",
      "ALTER TABLE TESTSCHEMA.Other MODIFY (blahField DECIMAL(20,3) NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Other.blahField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[blahField]/TYPE:[DECIMAL]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterColumnChangingLengthAndCase()
   */
  @Override
  protected List<String> expectedAlterColumnChangingLengthAndCase() {
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Other MODIFY (FloatField DECIMAL(20,3))",
      "COMMENT ON COLUMN TESTSCHEMA.Other.FloatField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[FloatField]/TYPE:[DECIMAL]'");
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
    return Arrays.asList("ALTER TABLE TESTSCHEMA.Test ADD (stringField_with_default NVARCHAR2(6) DEFAULT N'N' NOT NULL)",
      "COMMENT ON COLUMN TESTSCHEMA.Test.stringField_with_default IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[stringField_with_default]/TYPE:[STRING]'");
  }


  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdate()
   */
  @Override
  protected List<String> expectedAutonumberUpdate() {
    return Arrays.asList("MERGE INTO TESTSCHEMA.Autonumber A USING (SELECT COALESCE(MAX(id) + 1, 1) AS CurrentValue FROM TESTSCHEMA.TestTable) S ON (A.id = 'TestTable') WHEN MATCHED THEN UPDATE SET A.value = S.CurrentValue WHERE A.value < S.CurrentValue WHEN NOT MATCHED THEN INSERT (id, value) VALUES ('TestTable', S.CurrentValue)");
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
    return "TO_DATE(" + stringLiteralPrefix() + "'20100101', 'yyyymmdd')";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmdd()
   */
  @Override
  protected String expectedDateToYyyymmdd() {
    return "TO_NUMBER(TO_CHAR(testField, 'yyyymmdd'))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedBlobLiteral(String) ()
   */
  @Override
  protected String expectedBlobLiteral(String value) {
    return String.format("HEXTORAW(%s)",super.expectedBlobLiteral(value));
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDateToYyyymmddHHmmss()
   */
  @Override
  protected String expectedDateToYyyymmddHHmmss() {
    return "TO_NUMBER(TO_CHAR(testField, 'yyyymmddHH24MISS'))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedNow()
   */
  @Override
  protected String expectedNow() {
    return "SYSTIMESTAMP AT TIME ZONE 'UTC'";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDaysBetween()
   */
  @Override
  protected String expectedDaysBetween() {
    return "SELECT (dateTwo) - (dateOne) FROM " + tableName("MyTable");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedDropViewStatements()
   */
  @Override
  protected List<String> expectedDropViewStatements() {
    return Arrays.asList("BEGIN FOR i IN (SELECT null FROM all_views WHERE OWNER='TESTSCHEMA' AND VIEW_NAME='TESTVIEW') LOOP EXECUTE IMMEDIATE 'DROP VIEW " + tableName("TestView") + "'; END LOOP; END;");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSubstring()
   */
  @Override
  protected String expectedSubstring() {
    return "SELECT SUBSTR(field1, 1, 3) FROM " + tableName("schedule");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAutonumberUpdateForNonIdColumn()
   */
  @Override
  protected List<String> expectedAutonumberUpdateForNonIdColumn() {
    return Arrays.asList("MERGE INTO TESTSCHEMA.Autonumber A USING (SELECT COALESCE(MAX(generatedColumn) + 1, 1) AS CurrentValue FROM TESTSCHEMA.TestTable) S ON (A.id = 'TestTable') WHEN MATCHED THEN UPDATE SET A.value = S.CurrentValue WHERE A.value < S.CurrentValue WHEN NOT MATCHED THEN INSERT (id, value) VALUES ('TestTable', S.CurrentValue)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedStringFunctionCast()
   */
  @Override
  protected String expectedStringFunctionCast() {
    return "CAST(MIN(field) AS NVARCHAR2(8))";
  }


  /**
   * Overrides the standard behaviour to ensure that the prepared statement is
   * set up with a decimal column for booleans
   *
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#verifyBooleanPrepareStatementParameter()
   */
  @Override
  protected void verifyBooleanPrepareStatementParameter() throws SQLException {

    final SqlParameter booleanColumn = parameter(SchemaUtils.column("booleanColumn", DataType.BOOLEAN));

    ArgumentCaptor<SqlParameter> parameterCaptor = ArgumentCaptor.forClass(SqlParameter.class);
    BigDecimal nullCheck = null;
    verify(callPrepareStatementParameter(booleanColumn, null)).setBigDecimal(parameterCaptor.capture(), eq(nullCheck));
    assertEquals("Name of mapped boolean parameter", "booleanColumn", parameterCaptor.getValue().getImpliedName());
    assertEquals("Type of mapped boolean parameter", DataType.DECIMAL, parameterCaptor.getValue().getMetadata().getType());
    assertEquals("Length of mapped boolean parameter", 1, parameterCaptor.getValue().getMetadata().getWidth());

    NamedParameterPreparedStatement mockStatement = callPrepareStatementParameter(booleanColumn, "true");
    ArgumentCaptor<BigDecimal> bigDecimalCapture = ArgumentCaptor.forClass(BigDecimal.class);
    verify(mockStatement).setBigDecimal(any(SqlParameter.class), bigDecimalCapture.capture());
    assertTrue("BigDecimal not correctly set on statement.  Expected 1, was: " + bigDecimalCapture.getValue(), bigDecimalCapture.getValue().compareTo(new BigDecimal(1)) == 0);

    mockStatement = callPrepareStatementParameter(booleanColumn, "false");
    bigDecimalCapture = ArgumentCaptor.forClass(BigDecimal.class);
    verify(mockStatement).setBigDecimal(any(SqlParameter.class), bigDecimalCapture.capture());
    assertTrue("BigDecimal not correctly set on statement.  Expected 0, was: " + bigDecimalCapture.getValue(), bigDecimalCapture.getValue().compareTo(new BigDecimal(0)) == 0);

  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSimple()
   */
  @Override
  protected String expectedMergeSimple() {
    return "MERGE INTO TESTSCHEMA.foo"
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM TESTSCHEMA.somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeComplex()
   */
  @Override
  protected String expectedMergeComplex() {
    return "MERGE INTO TESTSCHEMA.foo"
        + " USING (SELECT somewhere.newId AS id, join.joinBar AS bar FROM TESTSCHEMA.somewhere INNER JOIN TESTSCHEMA.join ON (somewhere.newId = join.joinId)) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeSourceInDifferentSchema()
   */
  @Override
  protected String expectedMergeSourceInDifferentSchema() {
    return "MERGE INTO TESTSCHEMA.foo"
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
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM TESTSCHEMA.somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeForAllPrimaryKeys()
   */
  @Override
  protected String expectedMergeForAllPrimaryKeys() {
    return "MERGE INTO TESTSCHEMA.foo"
        + " USING (SELECT somewhere.newId AS id FROM TESTSCHEMA.somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN NOT MATCHED THEN INSERT (id) VALUES (xmergesource.id)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedMergeWithUpdateExpressions()
   */
  @Override
  protected String expectedMergeWithUpdateExpressions() {
    return "MERGE INTO TESTSCHEMA.foo"
        + " USING (SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM TESTSCHEMA.somewhere) xmergesource"
        + " ON (foo.id = xmergesource.id)"
        + " WHEN MATCHED THEN UPDATE SET bar = xmergesource.bar + foo.bar"
        + " WHEN NOT MATCHED THEN INSERT (id, bar) VALUES (xmergesource.id, xmergesource.bar)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddDays()
   */
  @Override
  protected String expectedAddDays() {
    return "(testField) + (-20)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddMonths()
   */
  @Override
  protected String expectedAddMonths() {
    return "ADD_MONTHS(testField, -3)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAlterRemoveColumnFromSimpleKeyStatements()
   */
  @Override
  protected List<String> expectedAlterRemoveColumnFromSimpleKeyStatements() {
    return Collections.singletonList("ALTER TABLE TESTSCHEMA.Test SET UNUSED (id)");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameTableStatements()
   */
  @Override
  protected List<String> expectedRenameTableStatements() {
    return ImmutableList.of(
      "ALTER TABLE TESTSCHEMA.Test RENAME CONSTRAINT Test_PK TO Renamed_PK",
      "ALTER INDEX TESTSCHEMA.Test_PK RENAME TO Renamed_PK",
      "ALTER TABLE TESTSCHEMA.Test RENAME TO Renamed",
      "COMMENT ON TABLE TESTSCHEMA.Renamed IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[Renamed]'");
  }


  /**
   * @return the expected statements for renaming a table with a long name.
   */
  @Override
  protected List<String> getRenamingTableWithLongNameStatements() {
    return ImmutableList.of(
      "ALTER TABLE TESTSCHEMA.123456789012345678901234567890 RENAME CONSTRAINT 123456789012345678901234567_PK TO Blah_PK",
      "ALTER INDEX TESTSCHEMA.123456789012345678901234567_PK RENAME TO Blah_PK",
      "ALTER TABLE TESTSCHEMA.123456789012345678901234567890 RENAME TO Blah",
      "COMMENT ON TABLE TESTSCHEMA.Blah IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[Blah]'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameIndexStatements() {
    return ImmutableList.of("ALTER INDEX TESTSCHEMA.Test_1 RENAME TO Test_2");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRenameIndexStatements()
   */
  @Override
  protected List<String> expectedRenameTempIndexStatements() {
    return ImmutableList.of("ALTER INDEX TESTSCHEMA.TempTest_1 RENAME TO TempTest_2");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSqlStatementFormat()
   */
  @Override
  protected void expectedSqlStatementFormat() {
    // When
    String statement1 = getTestDialect().formatSqlStatement("END;");
    String statement2 = getTestDialect().formatSqlStatement("BEGIN");
    String statement3 = getTestDialect().formatSqlStatement(Strings.repeat("a", 2498) + " " + Strings.repeat("b", 2497) + " " + Strings.repeat("c", 2497));

    // Then
    assertEquals("The statement separator should be [END;" + System.getProperty("line.separator") + "/]" , "END;" + System.getProperty("line.separator") + "/", statement1);
    assertEquals("The statement separator should be [BEGIN;]" , "BEGIN;", statement2);
    assertLengthOfLinesInStringLessThan2500Characters(statement3);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAnalyseTableSql()
   */
  @Override
  protected Collection<String> expectedAnalyseTableSql() {
    return ImmutableList.of("BEGIN \n" +
        "DBMS_STATS.GATHER_TABLE_STATS(ownname=> 'testschema', "
        + "tabname=>'TempTest', "
            + "cascade=>true, degree=>DBMS_STATS.AUTO_DEGREE, no_invalidate=>false); \n"
            + "END;");
  }


  /**
   * Checks that the length of the lines in the String passed in does not exceed
   * 2499.
   */
  private void assertLengthOfLinesInStringLessThan2500Characters(String lines) {
    if (lines.length() < 2500) {
      return;
    }

    int newLine = lines.indexOf(System.getProperty("line.separator"));
    if (newLine >= 0 && newLine < 2500) {
      assertLengthOfLinesInStringLessThan2500Characters(lines.substring(newLine + 1));
    } else {
      fail("Line length should not be greater than 2499 characters");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedLeftPad()
   */
  @Override
  protected String expectedLeftPad() {
    return "SELECT LPAD(stringField, 10, N'j') FROM TESTSCHEMA.Test";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomFunction()
   */
  @Override
  protected String expectedRandomFunction() {
    return "dbms_random.value";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedRandomString()
   */
  @Override
  protected String expectedRandomString() {
    return "dbms_random.string('A', 10)";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  @Override
  protected String expectedSelectFirstOrderByNullsLastDesc() {
    return "SELECT MIN(stringField) KEEP (DENSE_RANK FIRST ORDER BY stringField DESC NULLS LAST) FROM " + tableName("Alternate");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectLiteralWithWhereClauseString()
   */
  @Override
  protected String expectedSelectLiteralWithWhereClauseString() {
    return "SELECT N'LITERAL' FROM dual WHERE (N'ONE' = N'ONE')";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedAddTableFromStatements()
   */
  @Override
  public List<String> expectedAddTableFromStatements() {
    return ImmutableList.of(
      "CREATE TABLE TESTSCHEMA.SomeTable (someField  NOT NULL, otherField  NOT NULL, CONSTRAINT SomeTable_PK PRIMARY KEY (someField) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.SomeTable_PK ON TESTSCHEMA.SomeTable (someField))) PARALLEL NOLOGGING AS SELECT someField, otherField FROM TESTSCHEMA.OtherTable",
      "ALTER TABLE TESTSCHEMA.SomeTable NOPARALLEL LOGGING",
      "ALTER INDEX TESTSCHEMA.SomeTable_PK NOPARALLEL LOGGING",
      "COMMENT ON TABLE TESTSCHEMA.SomeTable IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[SomeTable]'",
      "COMMENT ON COLUMN TESTSCHEMA.SomeTable.someField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[someField]/TYPE:[STRING]'",
      "COMMENT ON COLUMN TESTSCHEMA.SomeTable.otherField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[otherField]/TYPE:[DECIMAL]'"
    );
  }

  public List<String> expectedReplaceTableFromStatements() {
    return ImmutableList.of(
        "CREATE TABLE TESTSCHEMA.SomeTable2 (someField  NOT NULL, otherField  NOT NULL, CONSTRAINT SomeTable2_PK PRIMARY KEY (someField) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.SomeTable2_PK ON TESTSCHEMA.SomeTable2 (someField))) PARALLEL NOLOGGING AS SELECT CAST(someField AS NVARCHAR2(3)), CAST(otherField AS DECIMAL(3,0)) FROM TESTSCHEMA.SomeTable",
        "ALTER TABLE TESTSCHEMA.SomeTable2 NOPARALLEL LOGGING",
        "ALTER INDEX TESTSCHEMA.SomeTable2_PK NOPARALLEL LOGGING",
        "COMMENT ON TABLE TESTSCHEMA.SomeTable2 IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[SomeTable2]'",
        "COMMENT ON COLUMN TESTSCHEMA.SomeTable2.someField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[someField]/TYPE:[STRING]'",
        "COMMENT ON COLUMN TESTSCHEMA.SomeTable2.otherField IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[otherField]/TYPE:[DECIMAL]'",
        "DROP TABLE TESTSCHEMA.SomeTable CASCADE CONSTRAINTS",
        "ALTER TABLE TESTSCHEMA.SomeTable2 RENAME CONSTRAINT SomeTable2_PK TO SomeTable_PK",
        "ALTER INDEX TESTSCHEMA.SomeTable2_PK RENAME TO SomeTable_PK",
        "ALTER TABLE TESTSCHEMA.SomeTable2 RENAME TO SomeTable",
        "COMMENT ON TABLE TESTSCHEMA.SomeTable IS '"+OracleDialect.REAL_NAME_COMMENT_LABEL+":[SomeTable]'",
        "CREATE INDEX TESTSCHEMA.SomeTable_1 ON TESTSCHEMA.SomeTable (otherField) PARALLEL NOLOGGING",
        "ALTER INDEX TESTSCHEMA.SomeTable_1 NOPARALLEL LOGGING"
    );
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints1(int)
   */
  @Override
  protected String expectedHints1(int rowCount) {
    return "SELECT /*+ ORDERED FIRST_ROWS(" + rowCount + ") INDEX(Foo Foo_1) INDEX(aliased Foo_2) */ * "
         + "FROM SCHEMA2.Foo "
         + "INNER JOIN " + tableName("Bar") + " ON (a = b) "
         + "LEFT OUTER JOIN " + tableName("Fo") + " ON (a = b) "
         + "INNER JOIN " + tableName("Fum") + " Fumble ON (a = b) "
         + "ORDER BY a NULLS FIRST";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints2(int)
   */
  @Override
  protected String expectedHints2(int rowCount) {
    return "SELECT /*+ INDEX(Foo Foo_1) FIRST_ROWS(" + rowCount + ") ORDERED PARALLEL ENABLE_PARALLEL_DML */ a, b FROM " + tableName("Foo") + " ORDER BY a NULLS FIRST FOR UPDATE";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints3
   */
  @Override
  protected String expectedHints3() {
    return "UPDATE /*+ ENABLE_PARALLEL_DML PARALLEL */ " + tableName("Foo") + " SET a = b";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints3
   */
  @Override
  protected String expectedHints3a() {
    return "UPDATE /*+ ENABLE_PARALLEL_DML PARALLEL(5) */ " + tableName("Foo") + " SET a = b";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints4()
   */
  @Override
  protected String expectedHints4() {
    return "INSERT /*+ APPEND */ INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints4a()
   */
  @Override
  protected String expectedHints4a() {
    return "INSERT /*+ NOAPPEND */ INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints4a()
   */
  @Override
  protected String expectedHints4b() {
    return "INSERT /*+ ENABLE_PARALLEL_DML PARALLEL */ INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints4a()
   */
  @Override
  protected String expectedHints4c() {
    return "INSERT /*+ ENABLE_PARALLEL_DML PARALLEL(5) */ INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints6()
   */
  @Override
  protected String expectedHints6() {
    return "SELECT /*+ PARALLEL(5) */ a, b FROM " + tableName("Foo") + " ORDER BY a NULLS FIRST";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints6a()
   */
  @Override
  protected String expectedHints6a() {
    return "SELECT /*+ PARALLEL(5) ENABLE_PARALLEL_DML */ a, b FROM " + tableName("Foo") + " ORDER BY a NULLS FIRST";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedHints7
   */
  @Override
  protected String expectedHints7() {
    return "SELECT /*+ opt_param('optimizer_index_caching',100) opt_param('optimizer_index_cost_adj',50) optimizer_features_enable('12.1.0.2') ) */ * FROM SCHEMA2.Foo";
  }


  @Override
  protected CustomHint provideCustomHint() {
    return new OracleCustomHint("opt_param('optimizer_index_caching',100) opt_param('optimizer_index_cost_adj',50) optimizer_features_enable('12.1.0.2') )");
  }



  @Override
  protected boolean expectedUsesNVARCHARforStrings() {
    return true; // We do!
  }

  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#nullOrderForDirection(org.alfasoftware.morf.sql.element.Direction)
   */
  @Override
  protected String nullOrderForDirection(Direction descending) {
    return ASCENDING.equals(descending) ? nullOrder() : NULLS_LAST;
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndWhere(String value) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE (Test.stringField = " + stringLiteralPrefix() + value + ") AND ROWNUM <= 1000";
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitAndComplexWhere(String value1, String value2) {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ((Test.stringField = " + stringLiteralPrefix() + value1 + ") OR (Test.stringField = " + stringLiteralPrefix() + value2 + ")) AND ROWNUM <= 1000";
  }


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  @Override
  protected String expectedDeleteWithLimitWithoutWhere() {
    return "DELETE FROM " + tableName(TEST_TABLE) + " WHERE ROWNUM <= 1000";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExcept()
   */
  @Override
  protected String expectedSelectWithExcept() {
    return "SELECT stringField FROM TESTSCHEMA.Test MINUS SELECT stringField FROM TESTSCHEMA.Other ORDER BY stringField NULLS FIRST";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithDbLink()
   */
  @Override
  protected String expectedSelectWithDbLink() {
    return "SELECT stringField FROM TESTSCHEMA.Test@MYDBLINKREF";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExceptAndDbLinkFormer()
   */
  @Override
  protected String expectedSelectWithExceptAndDbLinkFormer() {
    return "SELECT stringField FROM TESTSCHEMA.Test@MYDBLINKREF MINUS SELECT stringField FROM TESTSCHEMA.Other ORDER BY stringField NULLS FIRST";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractSqlDialectTest#expectedSelectWithExceptAndDbLinkLatter()
   */
  @Override
  protected String expectedSelectWithExceptAndDbLinkLatter() {
    return "SELECT stringField FROM TESTSCHEMA.Test MINUS SELECT stringField FROM TESTSCHEMA.Other@MYDBLINKREF ORDER BY stringField NULLS FIRST";
  }

}
