/* Copyright 2026 Alfa Financial Software
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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.ChangeColumn;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests that {@link OracleDialect#alterTableChangeColumnStatements(Table, Column, Column)} leaves the indexes
 * over the changed column in place whenever Oracle can keep them valid.
 */
public class TestOracleDialectChangeColumn {

  private static final String TABLE = "Indexed";

  private final SqlDialect dialect = new OracleDialect("TESTSCHEMA");

  private final Schema schema = schema(
    table(TABLE)
      .columns(
        column("code", DataType.STRING, 10).primaryKey(),
        column("stringCol", DataType.STRING, 10).nullable(),
        column("decimalCol", DataType.DECIMAL, 10),
        column("flagCol", DataType.STRING, 1).defaultValue("N"))
      .indexes(
        index("Indexed_1").unique().columns("stringCol", "decimalCol"),
        index("Indexed_2").columns("decimalCol"),
        index("Indexed_3").columns("flagCol")),
    table("Composite")
      .columns(
        column("keyCode", DataType.STRING, 10).primaryKey(),
        column("keyNumber", DataType.DECIMAL, 10).primaryKey(),
        column("valueCol", DataType.STRING, 10).nullable())
      .indexes(
        index("Composite_1").columns("keyNumber", "valueCol")),
    table("Keyless")
      .columns(
        column("keyCol", DataType.STRING, 10),
        column("valueCol", DataType.STRING, 10).nullable())
      .indexes(
        index("Keyless_1").columns("keyCol", "valueCol")),
    table("TempIndexed").temporary()
      .columns(
        column("code", DataType.STRING, 10).primaryKey(),
        column("stringCol", DataType.STRING, 10))
      .indexes(
        index("TempIndexed_1").columns("stringCol")));


  /**
   * Widening an indexed column must not rebuild its indexes.
   */
  @Test
  public void testWidenIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (stringCol NVARCHAR2(20))",
        comment("stringCol", DataType.STRING)),
      changeColumn(
        column("stringCol", DataType.STRING, 10).nullable(),
        column("stringCol", DataType.STRING, 20).nullable()));
  }


  /**
   * Reducing the width of an indexed column must not rebuild its indexes.
   */
  @Test
  public void testReduceWidthOfIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (stringCol NVARCHAR2(5))",
        comment("stringCol", DataType.STRING)),
      changeColumn(
        column("stringCol", DataType.STRING, 10).nullable(),
        column("stringCol", DataType.STRING, 5).nullable()));
  }


  /**
   * Changing the nullability and adding a default to an indexed column must not rebuild its indexes.
   */
  @Test
  public void testChangeNullabilityAndDefaultOfIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (stringCol  DEFAULT N'X' NOT NULL)",
        comment("stringCol", DataType.STRING)),
      changeColumn(
        column("stringCol", DataType.STRING, 10).nullable(),
        column("stringCol", DataType.STRING, 10).defaultValue("X")));
  }


  /**
   * Changing the default of an indexed column must not rebuild its indexes.
   */
  @Test
  public void testChangeDefaultOfIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (flagCol  DEFAULT N'Y')",
        comment("flagCol", DataType.STRING)),
      changeColumn(
        column("flagCol", DataType.STRING, 1).defaultValue("N"),
        column("flagCol", DataType.STRING, 1).defaultValue("Y")));
  }


  /**
   * Removing the default of an indexed column must not rebuild its indexes.
   */
  @Test
  public void testRemoveDefaultOfIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (flagCol DEFAULT NULL)",
        comment("flagCol", DataType.STRING)),
      changeColumn(
        column("flagCol", DataType.STRING, 1).defaultValue("N"),
        column("flagCol", DataType.STRING, 1)));
  }


  /**
   * DECIMAL and BIG_INTEGER are both stored as NUMBER, so the indexes must not be rebuilt.
   */
  @Test
  public void testChangeIndexedDecimalColumnToBigInteger() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (decimalCol NUMBER(19))",
        comment("decimalCol", DataType.BIG_INTEGER)),
      changeColumn(
        column("decimalCol", DataType.DECIMAL, 10),
        column("decimalCol", DataType.BIG_INTEGER)));
  }


  /**
   * Renaming an indexed column while changing its width and nullability must not rebuild its indexes.
   */
  @Test
  public void testRenameAndChangeIndexedColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed RENAME COLUMN stringCol TO renamedStringCol",
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (renamedStringCol NVARCHAR2(20) NOT NULL)",
        comment("renamedStringCol", DataType.STRING)),
      changeColumn(
        column("stringCol", DataType.STRING, 10).nullable(),
        column("renamedStringCol", DataType.STRING, 20)));
  }


  /**
   * Changing only the case of an indexed column's name changes nothing in Oracle except the comment.
   */
  @Test
  public void testChangeCaseOfIndexedColumnName() {
    assertEquals(
      ImmutableList.of(comment("STRINGCOL", DataType.STRING)),
      changeColumn(
        column("stringCol", DataType.STRING, 10).nullable(),
        column("STRINGCOL", DataType.STRING, 10).nullable()));
  }


  /**
   * Widening a primary key column must not recreate the primary key.
   */
  @Test
  public void testWidenPrimaryKeyColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (code NVARCHAR2(20))",
        comment("code", DataType.STRING)),
      changeColumn(
        column("code", DataType.STRING, 10).primaryKey(),
        column("code", DataType.STRING, 20).primaryKey()));
  }


  /**
   * Reducing the width of a primary key column must not recreate the primary key.
   */
  @Test
  public void testReduceWidthOfPrimaryKeyColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (code NVARCHAR2(5))",
        comment("code", DataType.STRING)),
      changeColumn(
        column("code", DataType.STRING, 10).primaryKey(),
        column("code", DataType.STRING, 5).primaryKey()));
  }


  /**
   * Renaming and widening a primary key column at once must not recreate the primary key.
   */
  @Test
  public void testRenameAndWidenPrimaryKeyColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed RENAME COLUMN code TO renamedCode",
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (renamedCode NVARCHAR2(20))",
        comment("renamedCode", DataType.STRING)),
      changeColumn(
        column("code", DataType.STRING, 10).primaryKey(),
        column("renamedCode", DataType.STRING, 20).primaryKey()));
  }


  /**
   * Changing the default of a primary key column must not recreate the primary key.
   */
  @Test
  public void testChangeDefaultOfPrimaryKeyColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed MODIFY (code  DEFAULT N'X')",
        comment("code", DataType.STRING)),
      changeColumn(
        column("code", DataType.STRING, 10).primaryKey(),
        column("code", DataType.STRING, 10).primaryKey().defaultValue("X")));
  }


  /**
   * Changing a column which stays in a composite primary key from DECIMAL to BIG_INTEGER must recreate
   * neither the primary key nor the other indexes over it.
   */
  @Test
  public void testChangeDecimalPrimaryKeyColumnToBigInteger() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Composite MODIFY (keyNumber NUMBER(19))",
        comment("Composite", "keyNumber", DataType.BIG_INTEGER)),
      changeColumn("Composite",
        column("keyNumber", DataType.DECIMAL, 10).primaryKey(),
        column("keyNumber", DataType.BIG_INTEGER).primaryKey()));
  }


  /**
   * Adding an indexed column to the primary key recreates the primary key, but not the other indexes.
   */
  @Test
  public void testAddIndexedColumnToPrimaryKey() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed DROP PRIMARY KEY DROP INDEX",
        "ALTER TABLE TESTSCHEMA.Indexed ADD CONSTRAINT Indexed_PK PRIMARY KEY (code, decimalCol) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Indexed_PK ON TESTSCHEMA.Indexed (code, decimalCol) NOLOGGING PARALLEL)",
        "ALTER INDEX TESTSCHEMA.Indexed_PK NOPARALLEL LOGGING",
        comment("decimalCol", DataType.DECIMAL)),
      changeColumn(
        column("decimalCol", DataType.DECIMAL, 10),
        column("decimalCol", DataType.DECIMAL, 10).primaryKey()));
  }


  /**
   * Removing an indexed column from a composite primary key recreates the primary key without it, but not the other indexes.
   */
  @Test
  public void testRemoveIndexedColumnFromCompositePrimaryKey() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Composite DROP PRIMARY KEY DROP INDEX",
        "ALTER TABLE TESTSCHEMA.Composite ADD CONSTRAINT Composite_PK PRIMARY KEY (keyCode) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Composite_PK ON TESTSCHEMA.Composite (keyCode) NOLOGGING PARALLEL)",
        "ALTER INDEX TESTSCHEMA.Composite_PK NOPARALLEL LOGGING",
        comment("Composite", "keyNumber", DataType.DECIMAL)),
      changeColumn("Composite",
        column("keyNumber", DataType.DECIMAL, 10).primaryKey(),
        column("keyNumber", DataType.DECIMAL, 10)));
  }


  /**
   * Removing the only primary key column drops the primary key, and there is no primary key left to create.
   */
  @Test
  public void testRemoveOnlyPrimaryKeyColumn() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Indexed DROP PRIMARY KEY DROP INDEX",
        comment("code", DataType.STRING)),
      changeColumn(
        column("code", DataType.STRING, 10).primaryKey(),
        column("code", DataType.STRING, 10)));
  }


  /**
   * Adding a column to the primary key of a table without one creates the primary key, with nothing to drop first.
   */
  @Test
  public void testAddColumnToPrimaryKeyOfKeylessTable() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.Keyless ADD CONSTRAINT Keyless_PK PRIMARY KEY (keyCol) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.Keyless_PK ON TESTSCHEMA.Keyless (keyCol) NOLOGGING PARALLEL)",
        "ALTER INDEX TESTSCHEMA.Keyless_PK NOPARALLEL LOGGING",
        comment("Keyless", "keyCol", DataType.STRING)),
      changeColumn("Keyless",
        column("keyCol", DataType.STRING, 10),
        column("keyCol", DataType.STRING, 10).primaryKey()));
  }


  /**
   * Widening a primary key column of a temporary table must not recreate the primary key either.
   */
  @Test
  public void testWidenPrimaryKeyColumnOfTemporaryTable() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.TempIndexed MODIFY (code NVARCHAR2(20))",
        comment("TempIndexed", "code", DataType.STRING)),
      changeColumn("TempIndexed",
        column("code", DataType.STRING, 10).primaryKey(),
        column("code", DataType.STRING, 20).primaryKey()));
  }


  /**
   * The primary key of a temporary table is recreated without NOLOGGING PARALLEL, so there is nothing to reset afterwards.
   */
  @Test
  public void testAddIndexedColumnToPrimaryKeyOfTemporaryTable() {
    assertEquals(
      ImmutableList.of(
        "ALTER TABLE TESTSCHEMA.TempIndexed DROP PRIMARY KEY DROP INDEX",
        "ALTER TABLE TESTSCHEMA.TempIndexed ADD CONSTRAINT TempIndexed_PK PRIMARY KEY (code, stringCol) USING INDEX (CREATE UNIQUE INDEX TESTSCHEMA.TempIndexed_PK ON TESTSCHEMA.TempIndexed (code, stringCol))",
        comment("TempIndexed", "stringCol", DataType.STRING)),
      changeColumn("TempIndexed",
        column("stringCol", DataType.STRING, 10),
        column("stringCol", DataType.STRING, 10).primaryKey()));
  }


  /**
   * Indexes survive any change which keeps the data type.
   */
  @Test
  public void testIndexesSurviveChangeWithinDataType() {
    for (DataType dataType : DataType.values()) {
      assertTrue(dataType.toString(), indexesSurvive(dataType, dataType));
    }
  }


  /**
   * Indexes survive changes between data types which are all stored as NUMBER.
   */
  @Test
  public void testIndexesSurviveChangeBetweenNumberTypes() {
    assertTrue(indexesSurvive(DataType.DECIMAL, DataType.BIG_INTEGER));
    assertTrue(indexesSurvive(DataType.INTEGER, DataType.BIG_INTEGER));
    assertTrue(indexesSurvive(DataType.BOOLEAN, DataType.DECIMAL));
  }


  /**
   * Indexes are assumed not to survive a change of storage type.
   */
  @Test
  public void testIndexesDoNotSurviveChangeOfStorageType() {
    assertFalse(indexesSurvive(DataType.STRING, DataType.CLOB));
    assertFalse(indexesSurvive(DataType.STRING, DataType.DECIMAL));
    assertFalse(indexesSurvive(DataType.DECIMAL, DataType.STRING));
    assertFalse(indexesSurvive(DataType.DATE, DataType.STRING));
    assertFalse(indexesSurvive(DataType.BIG_INTEGER, DataType.DATE));
    assertFalse(indexesSurvive(DataType.BLOB, DataType.CLOB));
  }


  private List<String> changeColumn(Column fromColumn, Column toColumn) {
    return changeColumn(TABLE, fromColumn, toColumn);
  }


  private List<String> changeColumn(String tableName, Column fromColumn, Column toColumn) {
    Table table = new ChangeColumn(tableName, fromColumn, toColumn).apply(schema).getTable(tableName);
    return ImmutableList.copyOf(dialect.alterTableChangeColumnStatements(table, fromColumn, toColumn));
  }


  private static boolean indexesSurvive(DataType fromType, DataType toType) {
    return OracleDialect.indexesSurviveColumnChange(column("col", fromType), column("col", toType));
  }


  private static String comment(String columnName, DataType dataType) {
    return comment(TABLE, columnName, dataType);
  }


  private static String comment(String tableName, String columnName, DataType dataType) {
    return "COMMENT ON COLUMN TESTSCHEMA." + tableName + "." + columnName + " IS '" + SqlDialect.REAL_NAME_COMMENT_LABEL + ":[" + columnName + "]/TYPE:[" + dataType + "]'";
  }
}
