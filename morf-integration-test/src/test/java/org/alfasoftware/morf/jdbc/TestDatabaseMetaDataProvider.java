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

package org.alfasoftware.morf.jdbc;

import static java.util.stream.Collectors.toList;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

/**
 * Tests for {@link DatabaseMetaDataProvider}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
@NotThreadSafe
public class TestDatabaseMetaDataProvider {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject
  private DatabaseSchemaManager schemaManager;

  @Inject
  private ConnectionResources database;


  private final Schema schema = schema(
    schema(
      table("WithTypes")
        .columns(
          // strings
          column("stringCol", DataType.STRING, 20),
          column("primaryStringCol", DataType.STRING, 15).primaryKey(),
          column("nullableStringCol", DataType.STRING, 10).nullable(),
          // decimals
          column("decimalElevenCol", DataType.DECIMAL, 11),
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
          column("nullableDecimalFiveTwoCol", DataType.DECIMAL, 5, 2).nullable(),
          // bigint
          column("bigIntegerCol", DataType.BIG_INTEGER),
          column("primaryBigIntegerCol", DataType.BIG_INTEGER).primaryKey(),
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable(),
          // bool
          column("booleanCol", DataType.BOOLEAN),
          column("nullableBooleanCol", DataType.BOOLEAN).nullable(),
          // integer
          column("integerTenCol", DataType.INTEGER),
          column("nullableIntegerCol", DataType.INTEGER).nullable(),
          // date
          column("dateCol", DataType.DATE),
          column("nullableDateCol", DataType.DATE).nullable())
        .indexes(
          index("WithTypes_1").columns("booleanCol", "dateCol"),
          index("NaturalKey").columns("decimalElevenCol").unique()),
      table("WithLobs")
        .columns(
          SchemaUtils.autonumber("autonumfield", 17),
          column("blobColumn", DataType.BLOB),
          column("clobColumn", DataType.CLOB)),
      table("WithDefaults")
        .columns(
          SchemaUtils.idColumn(),
          SchemaUtils.versionColumn(),
          column("stringCol", DataType.STRING, 20).defaultValue("one"),
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5).defaultValue("11.7"),
          column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("8"),
          column("booleanCol", DataType.BOOLEAN).defaultValue("1"),
          column("integerTenCol", DataType.INTEGER).defaultValue("17"),
          column("dateCol", DataType.DATE).defaultValue("2020-01-01"))
    ),
    schema(
      view("ViewWithTypes", select(field("primaryStringCol"), field("id")).from("WithTypes").innerJoin(tableRef("WithDefaults"))),
      view("View2", select(field("primaryStringCol"), field("id")).from("ViewWithTypes"), "ViewWithTypes")
    )
  );

  private static String databaseType;
  private static Boolean mysqlLowerCaseTableNames;
  private static Boolean mysqlLowerCaseViewNames;


  @Before
  public void before() throws SQLException {
    databaseType = database.getDatabaseType();

    schemaManager.dropAllViews();
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(schema, TruncationBehavior.ALWAYS);

    try (Connection connection = database.getDataSource().getConnection()) {
      String schema = Strings.isNullOrEmpty(database.getSchemaName()) ? "" : database.getSchemaName() + ".";
      connection.createStatement().executeUpdate("CREATE TABLE " + schema + "WithTimestamp (special TIMESTAMP)");
    }

    readMysqlLowerCaseTableNames();
  }

  @After
  public void after() throws SQLException {
    // must cleanup, otherwise other tests could try to read this table and fail
    try (Connection connection = database.getDataSource().getConnection()) {
      String schema = Strings.isNullOrEmpty(database.getSchemaName()) ? "" : database.getSchemaName() + ".";
      connection.createStatement().executeUpdate("DROP TABLE " + schema + "WithTimestamp");
    }

    schemaManager.invalidateCache();
  }


  @Test
  public void testViewsAndTables() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {

      assertFalse(schemaResource.isEmptyDatabase());

      assertThat(schemaResource.viewNames(), containsInAnyOrder(ImmutableList.of(
        viewNameEqualTo("ViewWithTypes"),
        viewNameEqualTo("View2")
      )));


      assertThat(schemaResource.views(), containsInAnyOrder(ImmutableList.of(
        viewNameMatcher("ViewWithTypes"),
        viewNameMatcher("View2")
      )));


      assertThat(schemaResource.tableNames(), containsInAnyOrder(ImmutableList.of(
        tableNameEqualTo("WithTypes"),
        tableNameEqualTo("WithDefaults"),
        tableNameEqualTo("WithLobs"),
        equalToIgnoringCase("WithTimestamp") // can read table names even if they contain unsupported columns
      )));


      assertThat(schemaResource.tables(), containsInAnyOrder(ImmutableList.of(
        tableNameMatcher("WithTypes"),
        tableNameMatcher("WithDefaults"),
        tableNameMatcher("WithLobs"),
        propertyMatcher(Table::getName, "name", equalToIgnoringCase("WithTimestamp")) // can read table names even if they contain unsupported columns
      )));
    }
  }

  @Test
  public void testTableWithTypes() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.tableExists("WithTypes"));
      Table table = schemaResource.getTable("WithTypes");

      assertThat(table.isTemporary(), is(false));
      assertThat(table.getName(), tableNameEqualTo("WithTypes"));

      assertThat(table.columns(), containsColumns(ImmutableList.of(
          // strings
        columnMatcher(
          column("stringCol", DataType.STRING, 20)),
        columnMatcher(
          column("primaryStringCol", DataType.STRING, 15).primaryKey()),
        columnMatcher(
          column("nullableStringCol", DataType.STRING, 10).nullable()),
          // decimals
        columnMatcher(
          column("decimalElevenCol", DataType.DECIMAL, 11)),
        columnMatcher(
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5)),
        columnMatcher(
          column("nullableDecimalFiveTwoCol", DataType.DECIMAL, 5, 2).nullable()),
          // bigint
        columnMatcher(
          column("bigIntegerCol", DataType.BIG_INTEGER)),
        columnMatcher(
          column("primaryBigIntegerCol", DataType.BIG_INTEGER).primaryKey()),
        columnMatcher(
          column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()),
          // bool
        columnMatcher(
          column("booleanCol", DataType.BOOLEAN)),
        columnMatcher(
          column("nullableBooleanCol", DataType.BOOLEAN).nullable()),
          // integer
        columnMatcher(
          column("integerTenCol", DataType.INTEGER)),
        columnMatcher(
          column("nullableIntegerCol", DataType.INTEGER).nullable()),
          // date
        columnMatcher(
          column("dateCol", DataType.DATE)),
        columnMatcher(
          column("nullableDateCol", DataType.DATE).nullable()))
      ));

      assertThat(table.primaryKey(), contains(ImmutableList.of(
        columnMatcher(
          column("primaryStringCol", DataType.STRING, 15).primaryKey()),
        columnMatcher(
          column("primaryBigIntegerCol", DataType.BIG_INTEGER).primaryKey()))
      ));

      assertThat(table.indexes(), containsInAnyOrder(ImmutableList.of(
        indexMatcher(
          index("WithTypes_1").columns("booleanCol", "dateCol")),
        indexMatcher(
          index("NaturalKey").columns("decimalElevenCol").unique()))
      ));
    }
  }


  @Test
  public void testTableWithLobs() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.tableExists("WithLobs"));
      Table table = schemaResource.getTable("WithLobs");

      assertThat(table.isTemporary(), is(false));
      assertThat(table.getName(), tableNameEqualTo("WithLobs"));

      assertThat(table.columns(), containsColumns(ImmutableList.of(
        columnMatcher(
          SchemaUtils.autonumber("autonumfield", 17)),
        columnMatcher(
          column("blobColumn", DataType.BLOB)),
        columnMatcher(
          column("clobColumn", DataType.CLOB)))
      ));

      assertThat(table.primaryKey(), contains(ImmutableList.of(
        columnMatcher(
          SchemaUtils.autonumber("autonumfield", 17)))
      ));

      assertThat(table.indexes(), empty());
    }
  }


  @Test
  public void testTableWithDefaults() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.tableExists("WithDefaults"));
      Table table = schemaResource.getTable("WithDefaults");

      assertThat(table.isTemporary(), is(false));
      assertThat(table.getName(), tableNameEqualTo("WithDefaults"));

      assertThat(table.columns(), containsColumns(ImmutableList.of(
        columnMatcher(
          SchemaUtils.idColumn()),
        columnMatcher(
          SchemaUtils.versionColumn()),
        columnMatcher(
          column("stringCol", DataType.STRING, 20).defaultValue("")),
        columnMatcher(
          column("decimalNineFiveCol", DataType.DECIMAL, 9, 5).defaultValue("")),
        columnMatcher(
          column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("")),
        columnMatcher(
          column("booleanCol", DataType.BOOLEAN).defaultValue("")),
        columnMatcher(
          column("integerTenCol", DataType.INTEGER).defaultValue("")),
        columnMatcher(
          column("dateCol", DataType.DATE).defaultValue("")))
      ));

      assertThat(table.primaryKey(), contains(ImmutableList.of(
        columnMatcher(
          SchemaUtils.idColumn()))
      ));

      assertThat(table.indexes(), empty());
    }
  }


  @Test
  public void testTableWithTimestamp() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.tableExists("WithTimestamp"));
      Table table = schemaResource.getTable("WithTimestamp");

      assertThat(table.isTemporary(), is(false));
      assertThat(table.getName(), equalToIgnoringCase("WithTimestamp"));

      assertThat(table.columns(), containsColumns(ImmutableList.of(
        propertyMatcher(Column::getName, "name", equalToIgnoringCase("special")) // can read column names even if they contain unsupported data types
      )));

      try {
        Iterables.getOnlyElement(table.columns()).getType();
        fail("Exception expected");
      }
      catch (DatabaseMetaDataProvider.UnexpectedDataTypeException e) { /* expected*/ }

      assertThat(table.indexes(), empty()); // can read indexes, as long as unsupported data types are not involved
    }
  }


  @Test
  public void testViewWithTypes() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.viewExists("ViewWithTypes"));
      View view = schemaResource.getView("ViewWithTypes");

      assertThat(view.getName(), viewNameEqualTo("ViewWithTypes"));

      assertThat(view.knowsSelectStatement(), equalTo(false));
      assertThat(view.knowsDependencies(), equalTo(false));

      try {
        SelectStatement selectStatement = view.getSelectStatement();
        fail("Expected UnsupportedOperationException, got " + selectStatement);
      } catch (UnsupportedOperationException e) {
        assertThat(e.getMessage(), equalToIgnoringCase("Cannot return SelectStatement as [ViewWithTypes] has been loaded from the database"));
      }

      try {
        String[] dependencies = view.getDependencies();
        fail("Expected UnsupportedOperationException, got " + dependencies);
      } catch (UnsupportedOperationException e) {
        assertThat(e.getMessage(), equalToIgnoringCase("Cannot return dependencies as [ViewWithTypes] has been loaded from the database"));
      }
    }
  }


  @Test
  public void testView2() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      assertTrue(schemaResource.viewExists("View2"));
      View view = schemaResource.getView("View2");

      assertThat(view.getName(), viewNameEqualTo("View2"));

      assertThat(view.knowsSelectStatement(), equalTo(false));
      assertThat(view.knowsDependencies(), equalTo(false));
    }
  }


  @Test
  public void testCopySchema() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      SchemaUtils.copy(schemaResource, ImmutableList.of("(?i:WithTimestamp)"));
    }
  }


  @Test
  public void testCopySchemaFailure() throws SQLException {
    try(SchemaResource schemaResource = database.openSchemaResource()) {
      SchemaUtils.copy(schemaResource, ImmutableList.of(""));
      fail("Exception expected");
    }
    catch (RuntimeException e) {
      assertThat(e.getMessage(), equalToIgnoringCase("Exception copying table [WITHTIMESTAMP]"));
    }
  }


  private static Matcher<? super Table> tableNameMatcher(String tableName) {
    Matcher<Table> subMatchers = Matchers.allOf(ImmutableList.of(
      propertyMatcher(Table::getName, "name", tableNameEqualTo(tableName))
    ));

    return Matchers.describedAs("%0", subMatchers, tableName);
  }


  private static Matcher<? super View> viewNameMatcher(String viewName) {
    Matcher<View> subMatchers = Matchers.allOf(ImmutableList.of(
      propertyMatcher(View::getName, "name", viewNameEqualTo(viewName))
    ));

    return Matchers.describedAs("%0", subMatchers, viewName);
  }


  private static Matcher<? super Column> columnMatcher(Column column) {
    Matcher<Column> subMatchers = Matchers.allOf(ImmutableList.of(
      propertyMatcher(Column::getName, "name", columnNameEqualTo(column.getName())),
      propertyMatcher(Column::getType, "type", equalTo(column.getType())),
      propertyMatcher(Column::getWidth, "width", equalTo(column.getWidth())),
      propertyMatcher(Column::getScale, "scale", equalTo(column.getScale())),
      propertyMatcher(Column::isNullable, "null", equalTo(column.isNullable())),
      propertyMatcher(Column::isPrimaryKey, "PK", equalTo(column.isPrimaryKey())),
      propertyMatcher(Column::getDefaultValue, "default", equalTo(column.getDefaultValue())),
      propertyMatcher(Column::isAutoNumbered, "autonum", equalTo(column.isAutoNumbered())),
      propertyMatcher(Column::getAutoNumberStart, "from", equalTo(column.getAutoNumberStart()))
    ));

    return Matchers.describedAs("%0", subMatchers, column.toString());
  }


  private static Matcher<? super Index> indexMatcher(Index index) {
    Matcher<Index> subMatchers = Matchers.allOf(ImmutableList.of(
      propertyMatcher(Index::getName, "name", indexNameEqualTo(index.getName())),
      propertyMatcher(Index::columnNames, "columns", contains(indexColumnNamesEqualTo(index.columnNames()))),
      propertyMatcher(Index::isUnique, "unique", equalTo(index.isUnique()))
    ));

    return Matchers.describedAs("%0", subMatchers, index.toString());
  }

  private static List<Matcher<? super String>> indexColumnNamesEqualTo(List<String> columnNames) {
    return columnNames.stream()
        .map(columnName -> indexColumnNameEqualTo(columnName))
        .collect(toList());
  }


  private static <T,U> Matcher<T> propertyMatcher(Function<T,U> propertyExtractor, String propertyName, Matcher<? super U> propertyMatcher) {
    return new FeatureMatcher<T,U> (propertyMatcher, "expected " + propertyName, propertyName) {
      @Override
      protected U featureValueOf(T actual) {
        return propertyExtractor.apply(actual);
      }
    };
  }


  private static Matcher<Iterable<? extends Column>> containsColumns(List<Matcher<? super Column>> columnMatchers) {
    switch (databaseType) {
      case "ORACLE":
        return containsInAnyOrder(columnMatchers);
      default:
        return contains(columnMatchers);
    }
  }


  private static Matcher<String> viewNameEqualTo(String viewName) {
    switch (databaseType) {
      case "H2":
      case "ORACLE":
        return equalTo(viewName.toUpperCase());
      case "MY_SQL":
        return mysqlLowerCaseViewNames
            ? equalTo(viewName.toLowerCase())
            : equalTo(viewName);
      default:
        return equalTo(viewName);
    }
  }


  private static Matcher<String> tableNameEqualTo(String tableName) {
    switch (databaseType) {
      case "H2":
        return equalTo(tableName.toUpperCase());
      case "MY_SQL":
        return mysqlLowerCaseTableNames
            ? equalTo(tableName.toLowerCase())
            : equalTo(tableName);
      default:
        return equalTo(tableName);
    }
  }


  private static Matcher<String> columnNameEqualTo(String columnName) {
    switch (databaseType) {
      case "H2":
        return equalTo(columnName.toUpperCase());
      default:
        return equalTo(columnName);
    }
  }


  private static Matcher<String> indexNameEqualTo(String indexName) {
    switch (databaseType) {
      case "H2":
        return equalTo(indexName.toUpperCase());
      case "ORACLE":
        return either(equalTo(indexName)).or(equalTo(indexName.toUpperCase()));
      default:
        return equalTo(indexName);
    }
  }


  private static Matcher<String> indexColumnNameEqualTo(String columnName) {
    switch (databaseType) {
      case "H2":
        return equalTo(columnName.toUpperCase());
      default:
        return equalTo(columnName);
    }
  }


  /**
   * For MySQL only, tries to determine whether table names and view names keep their CamelCase names.
   *
   * Originally, this was determined via a simple "SELECT @@GLOBAL.lower_case_table_names", however,
   * different versions and different distributions ended up providing rather unreliable answers. As
   * far as this test is concerned, what we really need is some form of consistency.
   */
  private void readMysqlLowerCaseTableNames() throws SQLException {
    if ("MY_SQL".equals(databaseType)) {
      try(SchemaResource schemaResource = database.openSchemaResource()) {
        Table table = schemaResource.getTable("WithTypes");
        mysqlLowerCaseTableNames = table.getName().equals("withtypes");

        View view = schemaResource.getView("ViewWithTypes");
        mysqlLowerCaseViewNames = view.getName().equals("viewwithtypes");
      }
    }
  }
}
