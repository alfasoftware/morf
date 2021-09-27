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

package org.alfasoftware.morf.integration;

import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.alfasoftware.morf.metadata.DataSetUtils.dataSetProducer;
import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.copy;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.dataset.TableDataHomology;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddBasicTable;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddColumn;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddColumnDropDefaultValue;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddColumnWithoutDropDefaultValue;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddDataToAutonumberedColumn;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddDataToIdColumn;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.AddPrimaryKeyColumns;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ChangeColumnDataType;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ChangeColumnLengthAndCase;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ChangePrimaryKeyColumnOrder;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ChangePrimaryKeyColumns;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.CorrectPrimaryKeyOrder;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.CorrectPrimaryKeyOrderNoOp;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.CreateTableAsSelect;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.DropLurkingDefaultValue;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.DropPrimaryKey;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ReduceStringColumnWidth;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RemoveAutoNumbered;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RemoveColumnFromCompositePrimaryKey;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RemoveColumnWithDefault;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RemovePrimaryKeyColumns;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RemoveSimpleColumn;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RenameIndex;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RenameKeylessTable;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RenameTable;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.RepeatedAdditionOfTable;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ReplacePrimaryKey;
import org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0.ReplaceTableWithView;
import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.CollectingDifferenceWriter;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradePathFinder.NoUpgradePathExistsException;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Provider;

import net.jcip.annotations.NotThreadSafe;

/**
 * Tests that the various SQL statement representations can be converted by the
 * SQL DSL to generate SQL that is valid for all supported database platforms.
 * <p>
 * This test will setup a basic {@link Schema} which can then be used to run
 * tests against.
 * </p>
 * <p>
 * Note that this test is actually testing the output of the relevant
 * {@link SqlDialect} is syntactically correct by running the SQL against the
 * target database platform. Verification of the expected SQL should really be
 * added to the {@link AbstractSqlDialectTest}.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2012v
 */
@NotThreadSafe
public class TestDatabaseUpgradeIntegration {

  /***/
  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  private Locale defaultLocale;

  @Inject
  private Provider<DatabaseDataSetConsumer> databaseDataSetConsumer;

  @Inject
  private Provider<DatabaseDataSetProducer> databaseDataSetProducer;

  @Inject
  private Provider<DatabaseSchemaManager> schemaManager;

  @Inject
  private ConnectionResources connectionResources;

  @Inject
  private SqlScriptExecutorProvider sqlScriptExecutorProvider;

  @Inject
  private DataSource dataSource;


  /**
   * The test schema.
   */
  private final Schema schema = schema(
    schema(
      deployedViewsTable(),
      upgradeAuditTable(),

      table("BasicTable")
        .columns(
          column("stringCol", DataType.STRING, 20).primaryKey(),
          column("nullableStringCol", DataType.STRING, 10).nullable(),
          column("decimalTenZeroCol", DataType.DECIMAL, 10),
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
          column("bigIntegerCol", DataType.BIG_INTEGER),
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()),

      table("WithDefaultValue")
        .columns(
          column("id", DataType.STRING, 3).primaryKey(),
          column("version", DataType.INTEGER).defaultValue("0")),

      table("CompositeKeyTable")
        .columns(
          column("keyCol1", DataType.STRING, 20).primaryKey(),
          column("keyCol2", DataType.STRING, 20).primaryKey(),
          column("valCol", DataType.STRING, 20)),

      table("KeylessTable")
        .columns(
          column("keyCol1", DataType.STRING, 20),
          column("keyCol2", DataType.STRING, 20),
          column("valCol", DataType.STRING, 20)),

      table("BasicTableWithIndex")
        .columns(
          column("stringCol", DataType.STRING, 20).primaryKey(),
          column("nullableStringCol", DataType.STRING, 10).nullable(),
          column("decimalTenZeroCol", DataType.DECIMAL, 10),
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
          column("bigIntegerCol", DataType.BIG_INTEGER),
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable())
        .indexes(
          index("WrongIndexName_1").columns("bigIntegerCol")),

      table("AutoNumTable")
        .columns(
          column("autonum", DataType.BIG_INTEGER).primaryKey().autoNumbered(123),
          column("keyCol1", DataType.STRING, 20),
          column("keyCol2", DataType.STRING, 20),
          column("valCol", DataType.STRING, 20)),

      table("IdTable")
        .columns(
          idColumn(),
          column("someValue", DataType.STRING, 20))
    ),
    schema(
      view("view4", select(field("valCol"), field("keyCol1")).from("view2"), "view3"),
      view("view1", select(field("valCol"), field("keyCol1")).from("BasicTable").crossJoin(tableRef("KeylessTable"))),
      view("view3", select(field("valCol"), field("keyCol1")).from("view2"), "view2"),
      view("view2", select(field("valCol"), field("keyCol1")).from("view1"), "view1"),
      view("viewId", select(field("id"), field("someValue")).from("IdTable"))
    )
  );


  /**
   * The test dataset
   */
  private final DataSetProducer dataSet = dataSetProducer(schema)
    .table(deployedViewsTable().getName())
    .table(upgradeAuditTable().getName())
    .table("BasicTable",
      record()
        .setString("stringCol", "hello world AA")
        .setString("nullableStringCol", "not null")
        .setBigDecimal("decimalTenZeroCol", new BigDecimal("9817236"))
        .setBigDecimal("decimalNineFiveCol", new BigDecimal("278.231"))
        .setLong("bigIntegerCol", 1234567890123456L)
        .setLong("nullableBigIntegerCol", 56732L),
      record()
        .setString("stringCol", "hello world BB")
        .setString("nullableStringCol", "sd")
        .setBigDecimal("decimalTenZeroCol", new BigDecimal("32"))
        .setBigDecimal("decimalNineFiveCol", new BigDecimal("378.231"))
        .setLong("bigIntegerCol", 98237L)
        .setLong("nullableBigIntegerCol", 892375L)
    )
    .table("WithDefaultValue",
      record()
        .setString("id", "1")
        .setInteger("version", 6),
      record()
        .setString("id", "2")
        .setInteger("version", 6)
    )
    .table("CompositeKeyTable",
      record()
        .setString("keyCol1", "1")
        .setString("keyCol2", "2")
        .setString("valCol",  "x"),
      record()
        .setString("keyCol1", "2")
        .setString("keyCol2", "3")
        .setString("valCol",  "y")
    )
    .table("KeylessTable",
      record()
        .setString("keyCol1", "1")
        .setString("keyCol2", "2")
        .setString("valCol",  "x"),
      record()
        .setString("keyCol1", "2")
        .setString("keyCol2", "3")
        .setString("valCol",  "y")
    )
    .table("BasicTableWithIndex",
      record()
        .setString("stringCol", "hello world AA")
        .setString("nullableStringCol", "not null")
        .setBigDecimal("decimalTenZeroCol", new BigDecimal("9817236"))
        .setBigDecimal("decimalNineFiveCol", new BigDecimal("278.231"))
        .setLong("bigIntegerCol", 1234567890123456L)
        .setLong("nullableBigIntegerCol", 56732L),
      record()
        .setString("stringCol", "hello world BB")
        .setString("nullableStringCol", "sd")
        .setBigDecimal("decimalTenZeroCol", new BigDecimal("32"))
        .setBigDecimal("decimalNineFiveCol", new BigDecimal("378.231"))
        .setLong("bigIntegerCol", 98237L)
        .setLong("nullableBigIntegerCol", 892375L)
    )
    .table("AutoNumTable")
    .table("IdTable");


  /**
   * Setup the schema for the tests.
   */
  @Before
  public void before() {
    defaultLocale = Locale.getDefault();
    Locale.setDefault(new Locale("en", "GB"));
    schemaManager.get().dropAllViews();
    schemaManager.get().dropAllTables();
    schemaManager.get().mutateToSupportSchema(schema, TruncationBehavior.ALWAYS);
    new DataSetConnector(dataSet, databaseDataSetConsumer.get()).connect();
  }


  /**
   * Invalidate the schema manager cache.
   */
  @After
  public void after() {
    schemaManager.get().invalidateCache();
    if (defaultLocale != null) {
      Locale.setDefault(defaultLocale);
    }
  }


  /**
   * Test that dropping a column with a default works, SQL Server is odd here.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testRemoveColumnWithDefault() throws SQLException {
    Schema expectedSchema = replaceTablesInSchema(
      table("WithDefaultValue")
      .columns(
        column("id", DataType.STRING, 3).primaryKey()
      )
    );

    verifyUpgrade(expectedSchema, RemoveColumnWithDefault.class);
  }


  /**
   * Test that renaming a table works
   */
  @Test
  public void testRenameTable() {

    Table newTable = table("BasicTableRenamed")
    .columns(
      column("stringCol", DataType.STRING, 20).primaryKey(),
      column("nullableStringCol", DataType.STRING, 10).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 10),
      column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
      column("bigIntegerCol", DataType.BIG_INTEGER),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );

    Map<String, Table> newTables = Maps.newHashMap();

    for (Table table : schema.tables()) {
      newTables.put(table.getName(), table);
    }

    newTables.remove("BasicTable");
    newTables.put("BasicTableRenamed", newTable);

    verifyUpgrade(schema(newTables.values()), RenameTable.class);
  }


  /**
   * Test that renaming a table works
   */
  @Test
  public void testRenameTableWithNoPrimaryKey() {

    Table newTable = table("RenamedKeylessTable")
    .columns(
      column("keyCol1", DataType.STRING, 20),
      column("keyCol2", DataType.STRING, 20),
      column("valCol", DataType.STRING, 20)
    );

    Map<String, Table> newTables = Maps.newHashMap();

    for (Table table : schema.tables()) {
      newTables.put(table.getName(), table);
    }

    newTables.remove("KeylessTable");
    newTables.put("RenamedKeylessTable", newTable);

    verifyUpgrade(schema(newTables.values()), RenameKeylessTable.class);
  }



  /**
   * Test that changing primary key columns works
   */
  @Test
  public void testChangePrimaryKeyColumns() {

    Table newTable = table("BasicTable")
    .columns(
      column("stringCol", DataType.STRING, 20),
      column("nullableStringCol", DataType.STRING, 10).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 10).primaryKey(),
      column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
      column("bigIntegerCol", DataType.BIG_INTEGER).primaryKey(),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );

    Schema expected = replaceTablesInSchema(newTable);

    verifyUpgrade(expected, ChangePrimaryKeyColumns.class);
  }

  /**
   * Test that changing primary key column ordering works.
   */
  @Test
  public void testChangePrimaryKeyColumnOrder() {

    TableBuilder newTable = table("CompositeKeyTable")
    .columns(
      column("keyCol2", DataType.STRING, 20).primaryKey(),
      column("keyCol1", DataType.STRING, 20).primaryKey(), // swapped order from the initial schema
      column("valCol", DataType.STRING, 20)
    );

    Schema expected = replaceTablesInSchema(newTable);

    verifyUpgrade(expected, ChangePrimaryKeyColumnOrder.class);
  }


  /**
   * Test that removing the existing primary key columns works
   */
  @Test
  public void testRemovePrimaryKeyColumns() {

    Table tableWithNoPrimaryKey = table("BasicTable")
    .columns(
      column("stringCol", DataType.STRING, 20),
      column("nullableStringCol", DataType.STRING, 10).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 10),
      column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
      column("bigIntegerCol", DataType.BIG_INTEGER),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );

    Schema removed = replaceTablesInSchema(tableWithNoPrimaryKey);

    verifyUpgrade(removed, RemovePrimaryKeyColumns.class);
  }


  /**
   * Test that adding primary key columns to a table with no primary key columns work
   */
  @Test
  public void testAddPrimaryKeyColumns() {
    Table tableWithNewPrimaryKey = table("KeylessTable")
      .columns(
        column("keyCol1", DataType.STRING, 20).primaryKey(),
        column("keyCol2", DataType.STRING, 20).primaryKey(),
        column("valCol", DataType.STRING, 20)
      );

    Schema reAdded = replaceTablesInSchema(tableWithNewPrimaryKey);

    verifyUpgrade(reAdded, AddPrimaryKeyColumns.class);
  }


  /**
   * Test that removing a column from a composite primary key works.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testRemoveColumnFromCompositePrimaryKey() throws SQLException {

    Schema expectedSchema = replaceTablesInSchema(
      table("CompositeKeyTable")
      .columns(
        column("keyCol1", DataType.STRING, 20).primaryKey(),
        column("keyCol2", DataType.STRING, 20),
        column("valCol", DataType.STRING, 20)
      )
    );

    verifyUpgrade(expectedSchema, RemoveColumnFromCompositePrimaryKey.class);
  }


  /**
   * Test that replacing a primary key with another works.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testReplacePrimaryKey() throws SQLException {

    Schema expectedSchema = replaceTablesInSchema(
      table("BasicTable")
      .columns(
        column("decimalTenZeroCol", DataType.DECIMAL, 10).primaryKey(),
        column("nullableStringCol", DataType.STRING, 10).nullable(),
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("bigIntegerCol", DataType.BIG_INTEGER),
        column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
      )
    );

    verifyUpgrade(expectedSchema, ReplacePrimaryKey.class);
  }


  /**
   * Test removing a simple column
   */
  @Test
  public void testRemoveSimpleColumn() {

    Schema expectedSchema = replaceTablesInSchema(
      table("BasicTable")
      .columns(
        column("stringCol", DataType.STRING, 20).primaryKey(),
        column("nullableStringCol", DataType.STRING, 10).nullable(),
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("bigIntegerCol", DataType.BIG_INTEGER),
        column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
      )
    );

    verifyUpgrade(expectedSchema, RemoveSimpleColumn.class);
  }


  /**
   * Test adding and removing the same table repeatedly
   */
  @Test
  public void testRepeatedAdditionOfTable() {
    Table anotherTableDifferentCase = table("AnotherTable").columns(
      column("somekey", DataType.DECIMAL, 10).primaryKey()
    );

    Schema expectedSchema = replaceTablesInSchema(anotherTableDifferentCase);

    verifyUpgrade(expectedSchema, RepeatedAdditionOfTable.class);
    verifyUpgrade(expectedSchema, RepeatedAdditionOfTable.class);
  }


  /**
   * Test:
   *   1. Rename BasicTable to BasicTableRenamed
   *   2. Add BasicTable
   *
   * This tests that everything from BasicTable has been correctly renamed.
   */
  @Test
  public void testRenameFollowedByAdditionUsingOldName() {
    Table newTable = table("BasicTableRenamed").columns(
      column("stringCol", DataType.STRING, 20).primaryKey(),
      column("nullableStringCol", DataType.STRING, 10).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 10),
      column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
      column("bigIntegerCol", DataType.BIG_INTEGER),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );

    List<Table> tables = Lists.newArrayList(schema.tables());
    tables.add(newTable);

    verifyUpgrade(schema(tables), ImmutableList.<Class<? extends UpgradeStep>>of(RenameTable.class, AddBasicTable.class));
  }


  /**
   * Tests changing the primary key of a table and changing the column type of the new primary
   * key column in one step.
   */
  @Test
  public void testChangeColumnDataTypeAndChangeToPrimaryKey() {
    Table tableWithExtraBigIntegerColumn = table("BasicTable")
    .columns(
      column("nullableStringCol", DataType.STRING, 10).nullable(),
      column("decimalTenZeroCol", DataType.BIG_INTEGER).primaryKey(),
      column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
      column("bigIntegerCol", DataType.BIG_INTEGER),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );

    Schema expected = replaceTablesInSchema(tableWithExtraBigIntegerColumn);

    verifyUpgrade(expected, ChangeColumnDataType.class);
  }


  /**
   * Tests changing a column's size and the case (from decimalNineFiveCol to decimalninefivecol).
   */
  @Test
  public void testChangeColumnTypeAndCase() {
    Table tableWithUpdatedDecimalNineFiveCol = table("BasicTable")
        .columns(
          column("stringCol", DataType.STRING, 20).primaryKey(),
          column("nullableStringCol", DataType.STRING, 10).nullable(),
          column("decimalTenZeroCol", DataType.DECIMAL, 10),
          column("decimalninefivecol", DataType.DECIMAL, 10, 6), // Column being changed
          column("bigIntegerCol", DataType.BIG_INTEGER),
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
      );

    Schema expected = replaceTablesInSchema(tableWithUpdatedDecimalNineFiveCol);

    verifyUpgrade(expected, ChangeColumnLengthAndCase.class);
  }


  /**
   * Tests dropping the primary key of a table.
   */
  @Test
  public void testDropPrimaryKey() {
    Table tableWithExtraBigIntegerColumn = table("WithDefaultValue")
        .columns(column("version", DataType.INTEGER).defaultValue("0"));

    Schema expected = replaceTablesInSchema(tableWithExtraBigIntegerColumn);

    verifyUpgrade(expected, DropPrimaryKey.class);
  }


  /**
   * Tests reducing the width of a column
   */
  @Test
  public void testReduceStringColumnWidth() {
    Table tableWithReducedWidth = table("BasicTable")
      .columns(
        column("stringCol", DataType.STRING, 20).primaryKey(),
        column("nullableStringCol", DataType.STRING, 8).nullable(),  // <-- this is the one we are changing
        column("decimalTenZeroCol", DataType.DECIMAL, 10),
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("bigIntegerCol", DataType.BIG_INTEGER),
        column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
      );
    Schema expected = replaceTablesInSchema(tableWithReducedWidth);

    verifyUpgrade(expected, ReduceStringColumnWidth.class);
  }


  /**
   * Tests renaming an index.
   */
  @Test
  public void testRenameIndex() {
    Table tableWithIndex = table("BasicTableWithIndex")
      .columns(
          column("stringCol", DataType.STRING, 20).primaryKey(),
          column("nullableStringCol", DataType.STRING, 10).nullable(),
          column("decimalTenZeroCol", DataType.DECIMAL, 10),
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
          column("bigIntegerCol", DataType.BIG_INTEGER),
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
      ).indexes(
        index("BasicTableWithIndex_1").columns("bigIntegerCol")
      );

    Schema expected = replaceTablesInSchema(tableWithIndex);

    verifyUpgrade(expected, RenameIndex.class);
  }


  /**
   * Tests removing autoNumber from a column
   */
  @Test
  public void testRemoveAutoNumbered() {
    Table newTable = table("AutoNumTable")
      .columns(
        column("autonum", DataType.BIG_INTEGER).primaryKey(),
        column("keyCol1", DataType.STRING, 20),
        column("keyCol2", DataType.STRING, 20),
        column("valCol", DataType.STRING, 20)
      );

    Map<String, Table> newTables = Maps.newHashMap();

    for (Table table : schema.tables()) {
      newTables.put(table.getName(), table);
    }

    newTables.remove("AutoNumTable");
    newTables.put("AutoNumTable", newTable);

    verifyUpgrade(schema(newTables.values()), RemoveAutoNumbered.class);
  }


  /**
   * Tests inserting data into a column with an autonumber property
   * works correctly
   *
   * @throws SQLException
   */
  @Test
  public void testAddDataToAutonumberedColumn() throws SQLException {
    verifyUpgrade(schema, AddDataToAutonumberedColumn.class);

    // Check the autonumbering works correctly
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    SelectStatement select = select(field("autonum")).from(tableRef("AutoNumTable"));
    Connection connection = dataSource.getConnection();
    try {
      String sql = connectionResources.sqlDialect().convertStatementToSQL(select);

       Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
        @Override
        public Integer process(ResultSet resultSet) throws SQLException {
          int last = 122;
          while (resultSet.next()) {
            assertTrue("Autonumbering not correct", ++last == resultSet.getInt("autonum"));
          }
          return last-122;
        }
      });
      assertEquals("Should be exactly three records", 3, numberOfRecords.intValue());
    } finally {
      connection.close();
    }
  }


  /**
   * Tests inserting data into a idColoumn works correctly
   *
   * @throws SQLException
   */
  @Test
  public void testAddDataToIdColumn() throws SQLException {
    verifyUpgrade(schema, AddDataToIdColumn.class);

    // Check the autonumbering works correctly
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    SelectStatement select = select(field("id")).from(tableRef("IdTable"));
    Connection connection = dataSource.getConnection();
    try {
      String sql = connectionResources.sqlDialect().convertStatementToSQL(select);

      Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
        @Override
        public Integer process(ResultSet resultSet) throws SQLException {
          int last = 0;
          while (resultSet.next()) {
            assertTrue("Id numbering not correct", ++last == resultSet.getInt("id"));
          }
          return last;
        }
      });
      assertEquals("Should be exactly three records", 3, numberOfRecords.intValue());
    } finally {
      connection.close();
    }
  }

  /**
   * Tests correcting primary key in the case it is required.
   *
   * @throws SQLException
   */
  @Test
  public void testCorrectPrimaryKeyOrderWhenRequired() throws SQLException {
    Table tableWithIndex = table("CompositeKeyTable")
        .columns(
                 column("keyCol2", DataType.STRING, 20).primaryKey(),
                 column("keyCol1", DataType.STRING, 20).primaryKey(),
                 column("valCol", DataType.STRING, 20)
             );
    Schema expected = replaceTablesInSchema(tableWithIndex);

    verifyUpgrade(expected, CorrectPrimaryKeyOrder.class);
  }


  /**
   * Tests correcting primary key in the case it is required.
   *
   * @throws SQLException
   */
  @Test
  public void testCorrectPrimaryKeyOrderWhenNotRequired() throws SQLException {
    verifyUpgrade(schema, CorrectPrimaryKeyOrderNoOp.class);
  }


  /**
   * Tests that it is possible to create and populate a table with a single
   * statement (CTAS - Create Table As Select).
   */
  @Test
  public void testCreateTableAsSelect() {
    List<Table> tables = Lists.newArrayList(schema.tables());
    tables.add(CreateTableAsSelect.tableToAdd());

    verifyUpgrade(schema(tables), CreateTableAsSelect.class);

    DataSetProducer expectedRecords = dataSetProducer(schema(tables))
        .table("TableAsSelect",
          record()
          .setString("stringCol", "hello world AA")
          .setString("stringColNullable", "hello world AA")
          .setBigDecimal("decimalTenZeroCol", new BigDecimal("9817236"))
          .setBigDecimal("nullableBigIntegerCol", new BigDecimal("56732")),
        record()
          .setString("stringCol", "hello world BB")
          .setString("stringColNullable", "hello world BB")
          .setBigDecimal("decimalTenZeroCol", new BigDecimal("32"))
          .setBigDecimal("nullableBigIntegerCol", new BigDecimal("892375"))
        );

    compareTableRecords("TableAsSelect", expectedRecords.records("TableAsSelect"));
  }


  /**
   * Tests adding non-nullable column to the table.
   * Verifies that default value is dropped after column is added.
   */
  @Test
  public void testAddColumnWithImplicitDropDefault() throws SQLException {
    doAddColumn(AddColumn.class);
  }


  /**
   * Tests adding non-nullable column to the table.
   * Verifies that default value is dropped after column is added.
   */
  @Test
  public void testAddColumnWithExplicitDropDefault() throws SQLException {
    doAddColumn(AddColumnDropDefaultValue.class);
  }


  private void doAddColumn(Class<? extends UpgradeStep> upgradeStep) throws SQLException {
    Schema expected = replaceTablesInSchema(
      table("WithDefaultValue")
        .columns(
          column("id", DataType.STRING, 3).primaryKey(),
          column("version", DataType.INTEGER).defaultValue("0"),
          column("anotherValue", DataType.STRING, 10)
        )
      );

    verifyUpgrade(expected, upgradeStep);

    DataSetProducer expectedRecords = dataSetProducer(schema(expected.getTable("WithDefaultValue")))
        .table("WithDefaultValue",
          record()
            .setString("id", "1")
            .setInteger("version", 6)
            .setString("anotherValue", "OLD"),
          record()
            .setString("id", "2")
            .setInteger("version", 6)
            .setString("anotherValue", "OLD")
        );

    compareTableRecords("WithDefaultValue", expectedRecords.records("WithDefaultValue"));

    // --
    // ensure the default value has been dropped

    DatabaseDataSetProducer producer = databaseDataSetProducer.get();
    producer.open();
    try {
      Column column = producer.getSchema().getTable("WithDefaultValue").columns().stream()
        .filter(c -> c.getName().equalsIgnoreCase("anotherValue"))
        .findAny()
        .get();

      assertTrue(StringUtils.isBlank(column.getDefaultValue()));
    }
    finally {
      producer.close();
    }

    //
    // try to insert some values into the table

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    try (Connection connection = dataSource.getConnection()) {
      InsertStatement insertStatement = insert()
          .into(tableRef("WithDefaultValue"))
          .fields(field("id"), field("anotherValue"))
          .from(select(
            literal(7).as("id"),
            literal("NEW").as("anotherValue") // can insert NEW
          ));

      executor.execute(connectionResources.sqlDialect().convertStatementToSQL(insertStatement), connection);
    }

    try (Connection connection = dataSource.getConnection()) {
      InsertStatement insertStatement = insert()
          .into(tableRef("WithDefaultValue"))
          .fields(field("id"))
          .from(select(
            literal(8).as("id")
          ));

      executor.execute(connectionResources.sqlDialect().convertStatementToSQL(insertStatement), connection);

      fail("Should not be able to insert the NULL");
    }
    catch (RuntimeSqlException e) {
      // crude attempt to ensure the failure was due to the NULL into non-NULL column
      String message = e.getCause().getMessage().trim();
      assertTrue(
        "Exception message: [" + message + "]",
        message.matches(
          "(?is)(" + "NULL not allowed for column \"ANOTHERVALUE\"" + ".*)" // H2
            + "|(" + "Field 'anotherValue' doesn't have a default value" + ".*)" // MySQL
            + "|(" + "ORA-01400: cannot insert NULL into \\(.*ANOTHERVALUE.*\\)" + ".*)" // Oracle
            + "|(" + "ERROR: null value in column \"anothervalue\" violates not-null constraint" + ".*)" // PgSQL
        ));
    }
  }


  /**
   * Tests adding non-nullable column to the table,
   * and leaving a default value behind.
   */
  @Test
  public void testAddColumnWithoutDropDefaultAsIfDropWasForgotten() throws SQLException {
    Schema expected = replaceTablesInSchema(
      table("WithDefaultValue")
        .columns(
          column("id", DataType.STRING, 3).primaryKey(),
          column("version", DataType.INTEGER).defaultValue("0"),
          column("anotherValue", DataType.STRING, 10)
        )
      );

    try {
      verifyUpgrade(expected, AddColumnWithoutDropDefaultValue.class);

      fail ("Should not upgrade because of forgotten DEFAULT value");
    }
    catch (NoUpgradePathExistsException e) { /* happy path */ }
  }


  /**
   * Tests adding non-nullable column to the table,
   * and leaving a default value behind.
   */
  @Test
  public void testAddColumnWithoutDropDefaultAsIfNoDropWasIntended() throws SQLException {
    Schema expected = replaceTablesInSchema(
      table("WithDefaultValue")
        .columns(
          column("id", DataType.STRING, 3).primaryKey(),
          column("version", DataType.INTEGER).defaultValue("0"),
          column("anotherValue", DataType.STRING, 10).defaultValue("OLD")
        )
      );

    try {
      verifyUpgrade(expected, AddColumnWithoutDropDefaultValue.class);

      fail ("Should not upgrade because of forgotten DEFAULT value");
    }
    catch (java.lang.AssertionError e) {
      assertThat(e.getMessage(), equalToIgnoringCase("[Column [anotherValue] on table [WithDefaultValue] default value does not match: [OLD] in expected, [] in actual]"));
    }
  }


  /**
   * Tests adding non-nullable column to the table,
   * and leaving a default value behind.
   */
  @Test
  public void testDropDefaultValue() throws SQLException {
    Schema expected = replaceTablesInSchema(
      table("CompositeKeyTable")
        .columns(
          column("keyCol1", DataType.STRING, 20).primaryKey(),
          column("keyCol2", DataType.STRING, 20).primaryKey(),
          column("valCol", DataType.STRING, 20))
      );

    verifyUpgrade(expected, DropLurkingDefaultValue.class);
  }


  /**
   * Test that renaming a table works
   */
  @Test
  public void testReplaceTableWithView() {

    Table originalTable = schema.getTable("BasicTable");

    Table newTable = table("BasicTableTmp")
      .columns(originalTable.columns().toArray(new Column[0]));

    View newView = view("BasicTable", select(literal(1).as("one")).from("BasicTableTmp"), "viewId"); // add dependency to entangle the new view with the old views, to engage in topology sorting

    FluentIterable<Table> newSchemaTables = FluentIterable.from(schema.tables())
      .filter(compose(not(equalTo("BasicTable")), Table::getName))
      .append(newTable);

    List<View> newSchemaViews = FluentIterable.from(schema.views())
      .filter(v -> !v.getName().equalsIgnoreCase("view1")) // re-add view1 now dependent on BasicTable view
      .append(view("view1", select(field("valCol"), field("keyCol1")).from("BasicTable").crossJoin(tableRef("KeylessTable")), "BasicTable"))
      .append(newView) // and add the new BasicTable view
      .toList();

    Schema expectedSchema = schema(schema(newSchemaTables), schema(newSchemaViews));

    verifyUpgrade(expectedSchema, ReplaceTableWithView.class);
  }


  /**
   * Helper to manipulate the test schema - replaces the tables in it with the ones provided. (By name)
   *
   * @param replacementTables The tables to use as replacements.
   * @return The modified schema.
   */
  private final Schema replaceTablesInSchema(Table... replacementTables) {
    Map<String, Table> newTables = Maps.newHashMap();

    for (Table table : schema.tables()) {
      newTables.put(table.getName(), table);
    }
    for (Table table : replacementTables) {
      newTables.put(table.getName(), table);
    }

    return schema(schema(newTables.values()), schema(schema.views()));
  }


  /**
   * Verify that the upgrade step supplied, results in the test schema being correctly upgraded to the expected schema.
   *
   * @param expectedSchema
   * @param upgradeStep The upgrade step to test
   */
  private void verifyUpgrade(Schema expectedSchema, Class<? extends UpgradeStep> upgradeStep) {
    verifyUpgrade(expectedSchema, singletonList(upgradeStep));
  }


  /**
   * Verify that the upgrade step supplied, results in the test schema being correctly upgraded to the expected schema.
   *
   * @param expectedSchema
   * @param upgradeSteps The upgrade steps to test
   */
  private void verifyUpgrade(Schema expectedSchema, List<Class<? extends UpgradeStep>> upgradeSteps) {
    Upgrade.performUpgrade(expectedSchema, upgradeSteps, connectionResources);
    compareSchema(expectedSchema);
  }


  private void compareSchema(Schema expectedSchema) {

    DatabaseDataSetProducer producer = databaseDataSetProducer.get();
    producer.open();
    try {
      Schema actualSchema = producer.getSchema();

      CollectingDifferenceWriter differences = new CollectingDifferenceWriter();
      SchemaHomology schemaHomology = new SchemaHomology(differences, "expected", "actual");

      boolean schemasMatch = schemaHomology.schemasMatch(expectedSchema, actualSchema, Collections.emptySet());

      assertTrue(differences.differences().toString(), schemasMatch);

      assertEquals(
        expectedSchema.views().stream().map(View::getName).map(String::toLowerCase).collect(toSet()),
        actualSchema.views().stream().map(View::getName).map(String::toLowerCase).collect(toSet()));

    } finally {
      producer.close();
    }
  }


  private void compareTableRecords(String tableName, Iterable<Record> expectedRecords) {

    DatabaseDataSetProducer producer = databaseDataSetProducer.get();
    producer.open();
    try {
      Schema actualSchema = producer.getSchema();
      Iterable<Record> actualRecords = producer.records(tableName);

      TableDataHomology tableDataHomology = new TableDataHomology();

      tableDataHomology.compareTable(copy(actualSchema.getTable(tableName)), actualRecords, expectedRecords);

      if (!tableDataHomology.getDifferences().isEmpty()) {
        fail("Table differences: " + tableDataHomology.getDifferences());
      }
    } finally {
      producer.close();
    }
  }
}
