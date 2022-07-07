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

import static org.alfasoftware.morf.metadata.DataSetUtils.dataSetProducer;
import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.caseStatement;
import static org.alfasoftware.morf.sql.SqlUtils.concat;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.nullLiteral;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.when;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.not;
import static org.alfasoftware.morf.sql.element.Criterion.or;
import static org.alfasoftware.morf.sql.element.Function.average;
import static org.alfasoftware.morf.sql.element.Function.averageDistinct;
import static org.alfasoftware.morf.sql.element.Function.count;
import static org.alfasoftware.morf.sql.element.Function.countDistinct;
import static org.alfasoftware.morf.sql.element.Function.floor;
import static org.alfasoftware.morf.sql.element.Function.greatest;
import static org.alfasoftware.morf.sql.element.Function.least;
import static org.alfasoftware.morf.sql.element.Function.leftPad;
import static org.alfasoftware.morf.sql.element.Function.length;
import static org.alfasoftware.morf.sql.element.Function.lowerCase;
import static org.alfasoftware.morf.sql.element.Function.max;
import static org.alfasoftware.morf.sql.element.Function.min;
import static org.alfasoftware.morf.sql.element.Function.mod;
import static org.alfasoftware.morf.sql.element.Function.power;
import static org.alfasoftware.morf.sql.element.Function.round;
import static org.alfasoftware.morf.sql.element.Function.substring;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.alfasoftware.morf.sql.element.Function.sumDistinct;
import static org.alfasoftware.morf.sql.element.Function.trim;
import static org.alfasoftware.morf.sql.element.Function.upperCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;

import net.jcip.annotations.NotThreadSafe;

/**
 * Tests NULL behaviour on various platforms, cementing known expected behaviour,
 * even if said behaviour differs from platform to platform.
 *
 * @author Copyright (c) Alfa Financial Software 2021
 */
@NotThreadSafe
public class TestSqlNulls {

  @Rule
  public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject
  private Provider<DatabaseDataSetConsumer> databaseDataSetConsumer;

  @Inject
  private Provider<DatabaseSchemaManager> schemaManager;

  @Inject
  private ConnectionResources connectionResources;

  @Inject
  private SqlScriptExecutorProvider sqlScriptExecutorProvider;


  private final Schema schema = schema(
    table("SimpleTypes")
      .columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("name", DataType.STRING, 10).nullable(),
        column("val", DataType.DECIMAL, 9, 5).nullable()
      )
  );

  private final DataSetProducer dataSet = dataSetProducer(schema)
    .table("SimpleTypes",
      record()
        .setLong("id", 1L)
        .setString("name", "A")
        .setBigDecimal("val", new BigDecimal("3.5")),
      record()
        .setLong("id", 2L)
        .setString("name", "N")
        .setBigDecimal("val", null),
      record()
        .setLong("id", 3L)
        .setString("name", null)
        .setBigDecimal("val", BigDecimal.ONE),
      record()
        .setLong("id", 4L)
        .setString("name", "X")
        .setBigDecimal("val", null)
    );

  private String databaseType;


  @Before
  public void before() throws SQLException {
    // no need to truncate the tables, the connector does that anyway
    schemaManager.get().invalidateCache();
    schemaManager.get().mutateToSupportSchema(schema, TruncationBehavior.ONLY_ON_TABLE_CHANGE);
    new DataSetConnector(dataSet, databaseDataSetConsumer.get()).connect();

    databaseType = connectionResources.getDatabaseType();
  }


  /**
   * Simple test verifying the test setup.
   */
  @Test
  public void testSimpleSelect() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(field("id"), field("name"), field("val"), literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(1));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(4, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(1L, resultSet.getInt(1));
        assertEquals("A", resultSet.getString(2));
        assertEquals(3.5, resultSet.getDouble(3), 0.01);
        // last column
        assertEquals(-7, resultSet.getInt(4));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testStringManipulationsWithoutNulls() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          field("id"),
          field("name"),
          length(field("name")),
          lowerCase(field("name")),
          upperCase(field("name")),
          trim(field("name")),
          leftPad(field("name"), literal(3), literal("x")),
          substring(field("name"), literal(1), literal(1)),
          concat(field("name"), field("name")),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(1));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(10, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(1L, resultSet.getInt(1));
        assertEquals("A", resultSet.getString(2));
        assertEquals(BigDecimal.ONE, resultSet.getBigDecimal(3));
        assertEquals("a", resultSet.getString(4));
        assertEquals("A", resultSet.getString(5));
        assertEquals("A", resultSet.getString(6));
        assertEquals("xxA", resultSet.getString(7));
        assertEquals("A", resultSet.getString(8));
        assertEquals("AA", resultSet.getString(9));
        // last column
        assertEquals(-7, resultSet.getInt(10));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testStringManipulationsWithNulls() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          field("id"),
          field("name"),
          length(field("name")),
          lowerCase(field("name")),
          upperCase(field("name")),
          trim(field("name")),
          // leftpad
          leftPad(field("name"), literal(3), literal("x")),
          leftPad(literal("x"), nullLiteral(), literal("x")),
          leftPad(literal("x"), literal(3), field("name")),
          // substring
          substring(field("name"), literal(3), literal(3)),
          substring(literal("x"), nullLiteral(), literal(3)),
          substring(literal("x"), literal(3), nullLiteral()),
          // concat
          concat(field("name"), field("name")),
          concat(literal("x"), field("name")),
          concat(ImmutableList.of(field("name"), literal("x"))),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(3));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(16, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(3L, resultSet.getInt(1));
        assertEquals(null, resultSet.getString(2));
        assertEquals(null, resultSet.getBigDecimal(3));
        assertEquals(null, resultSet.getString(4));
        assertEquals(null, resultSet.getString(5));
        assertEquals(null, resultSet.getString(6));
        // leftpad
        assertEquals(null, resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertEquals(null, resultSet.getString(9));
        // substring
        assertEquals(null, resultSet.getString(10));
        assertEquals(null, resultSet.getString(11));
        assertEquals(null, resultSet.getString(12));
        // concat
        assertEquals(expectedConcatOfTwoNulls(), resultSet.getString(13));
        assertEquals("x", resultSet.getString(14));
        assertEquals("x", resultSet.getString(15));
        // last column
        assertEquals(-7, resultSet.getInt(16));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  private Object expectedConcatOfTwoNulls() {
    switch (databaseType) {
      case "ORACLE":
        return null;

      default:
        return "";
    }
  }


  @Test
  public void testNumericManipulationsWithNulls() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          field("id"),
          field("val"),
          // least, greatest
          least(field("val"), field("val")),
          least(field("val"), literal(5)),
          greatest(field("val"), field("val")),
          greatest(field("val"), literal(1)),
          // round, floor
          round(field("val"), literal(2)),
          round(literal(2.2222), nullLiteral()),
          floor(field("val")),
          // mod, power
          mod(field("val"), literal(2)),
          mod(literal(2.2222), nullLiteral()),
          power(field("val"), literal(2)),
          power(literal(2.2222), nullLiteral()),
          // algebra
          field("val").plus(literal(2)),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(2));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(15, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(2L, resultSet.getInt(1));
        assertEquals(null, resultSet.getBigDecimal(2));
        // least, greatest
        assertEquals(null, resultSet.getBigDecimal(3));
        assertEquals(expectedLeastOfSomeNulls(), resultSet.getBigDecimal(4));
        assertEquals(null, resultSet.getBigDecimal(5));
        assertEquals(expectedGreatestOfSomeNulls(), resultSet.getBigDecimal(6));
        // round, floor
        assertEquals(null, resultSet.getBigDecimal(7));
        assertEquals(null, resultSet.getBigDecimal(8));
        assertEquals(null, resultSet.getBigDecimal(9));
        // mod, power
        assertEquals(null, resultSet.getBigDecimal(10));
        assertEquals(null, resultSet.getBigDecimal(11));
        assertEquals(null, resultSet.getBigDecimal(12));
        assertEquals(null, resultSet.getBigDecimal(13));
        // algebra
        assertEquals(null, resultSet.getBigDecimal(14));
        // last column
        assertEquals(-7, resultSet.getInt(15));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  private BigDecimal expectedLeastOfSomeNulls() {
    switch (databaseType) {
      case "ORACLE":
      case "MY_SQL":
        return null;

      default:
        return new BigDecimal(5);
    }
  }


  private BigDecimal expectedGreatestOfSomeNulls() {
    switch (databaseType) {
      case "ORACLE":
      case "MY_SQL":
        return null;

      default:
        return new BigDecimal(1);
    }
  }


  @Test
  public void testAggregationsWithNoRows() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          count(),
          count(field("val")),
          countDistinct(field("val")),
          // min, max, avg
          min(field("val")),
          max(field("val")),
          average(field("val")),
          averageDistinct(field("val")),
          // sum
          sum(field("val")),
          sumDistinct(field("val")),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(-1));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(10, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(BigDecimal.ZERO, resultSet.getBigDecimal(1));
        assertEquals(BigDecimal.ZERO, resultSet.getBigDecimal(2));
        assertEquals(BigDecimal.ZERO, resultSet.getBigDecimal(3));
        // min, max, avg
        assertEquals(null, resultSet.getBigDecimal(4));
        assertEquals(null, resultSet.getBigDecimal(5));
        assertEquals(null, resultSet.getBigDecimal(6));
        assertEquals(null, resultSet.getBigDecimal(7));
        // sum
        assertEquals(null, resultSet.getBigDecimal(8));
        assertEquals(null, resultSet.getBigDecimal(9));
        // last column
        assertEquals(-7, resultSet.getInt(10));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testAggregationsWithOnlyNullRows() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          count(),
          count(field("val")),
          countDistinct(field("val")),
          // min, max, avg
          min(field("val")),
          max(field("val")),
          average(field("val")),
          averageDistinct(field("val")),
          // sum
          sum(field("val")),
          sumDistinct(field("val")),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").in(2, 4));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(10, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(new BigDecimal(2), resultSet.getBigDecimal(1));
        assertEquals(new BigDecimal(0), resultSet.getBigDecimal(2));
        assertEquals(new BigDecimal(0), resultSet.getBigDecimal(3));
        // min, max, avg
        assertEquals(null, resultSet.getBigDecimal(4));
        assertEquals(null, resultSet.getBigDecimal(5));
        assertEquals(null, resultSet.getBigDecimal(6));
        assertEquals(null, resultSet.getBigDecimal(7));
        // sum
        assertEquals(null, resultSet.getBigDecimal(8));
        assertEquals(null, resultSet.getBigDecimal(9));
        // last column
        assertEquals(-7, resultSet.getInt(10));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testAggregationsWithSomeNullRows() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          count(),
          count(field("val")),
          countDistinct(field("val")),
          // min, max, avg
          min(field("val")),
          max(field("val")),
          average(field("val")),
          averageDistinct(field("val")),
          // sum
          sum(field("val")),
          sumDistinct(field("val")),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").in(1, 2, 3, 4));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(10, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(new BigDecimal(4), resultSet.getBigDecimal(1));
        assertEquals(new BigDecimal(2), resultSet.getBigDecimal(2));
        assertEquals(new BigDecimal(2), resultSet.getBigDecimal(3));
        // min, max, avg
        assertEquals(1.0, resultSet.getDouble(4), 0.01);
        assertEquals(3.5, resultSet.getDouble(5), 0.01);
        assertEquals(2.25, resultSet.getDouble(6), 0.01);
        assertEquals(2.25, resultSet.getDouble(7), 0.01);
        // sum
        assertEquals(4.5, resultSet.getDouble(8), 0.01);
        assertEquals(4.5, resultSet.getDouble(9), 0.01);
        // last column
        assertEquals(-7, resultSet.getInt(10));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testCriteriaWithNulls() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          field("id"),
          field("name"),
          // name = NULL vs name IS NULL
          caseStatement(when(field("name").eq(nullLiteral())).then(1)).otherwise(0),
          caseStatement(when(field("name").isNull()).then(1)).otherwise(0),
          // not
          caseStatement(when(not(field("name").neq(nullLiteral()))).then(1)).otherwise(0),
          caseStatement(when(not(field("name").isNotNull())).then(1)).otherwise(0),
          // or
          caseStatement(when(or(field("name").eq(nullLiteral()), field("name").isNull())).then(1)).otherwise(0),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(3));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(8, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(3, resultSet.getInt(1));
        assertEquals(null, resultSet.getString(2));
        // name = NULL vs name IS NULL
        assertEquals(0, resultSet.getInt(3));
        assertEquals(1, resultSet.getInt(4));
        // not
        assertEquals(0, resultSet.getInt(5));
        assertEquals(1, resultSet.getInt(6));
        // or
        assertEquals(1, resultSet.getInt(7));
        // last column
        assertEquals(-7, resultSet.getInt(8));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }


  @Test
  public void testInNotInWithNulls() {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement statement =
        select(
          field("id"),
          field("name"),
          // in
          caseStatement(when(literal(1).in(1, 7, 13)).then(1)).otherwise(0),
          caseStatement(when(or(literal(1).eq(1), literal(1).eq(7), literal(1).eq(13))).then(1)).otherwise(0),
          caseStatement(when(literal(1).in(1, 7, 13, nullLiteral())).then(1)).otherwise(0),
          caseStatement(when(or(literal(1).eq(1), literal(1).eq(7), literal(1).eq(13), literal(1).eq(nullLiteral()))).then(1)).otherwise(0),
          // not in
          caseStatement(when(not(literal(5).in(1, 7, 13))).then(1)).otherwise(0),
          caseStatement(when(not(or(literal(5).eq(1), literal(5).eq(7), literal(5).eq(13)))).then(1)).otherwise(0),
          caseStatement(when(not(literal(5).in(1, 7, 13, nullLiteral()))).then(1)).otherwise(0),
          caseStatement(when(not(or(literal(5).eq(1), literal(5).eq(7), literal(5).eq(13), literal(5).eq(nullLiteral())))).then(1)).otherwise(0),
          caseStatement(when(and(literal(5).neq(1), literal(5).neq(7), literal(5).neq(13), literal(5).neq(nullLiteral()))).then(1)).otherwise(0),
          // last column
          literal(-7))
          .from(tableRef("SimpleTypes"))
          .where(field("id").eq(3));

    executor.executeQuery(statement).processWith(resultSet -> {
      assertEquals(12, resultSet.getMetaData().getColumnCount());
      if (resultSet.next()) {
        assertEquals(3, resultSet.getInt(1));
        assertEquals(null, resultSet.getString(2));
        // in
        assertEquals(1, resultSet.getInt(3));
        assertEquals(1, resultSet.getInt(4));
        assertEquals(1, resultSet.getInt(5));
        assertEquals(1, resultSet.getInt(6));
        // not in
        assertEquals(1, resultSet.getInt(7));
        assertEquals(1, resultSet.getInt(8));
        assertEquals(0, resultSet.getInt(9));
        assertEquals(0, resultSet.getInt(10));
        assertEquals(0, resultSet.getInt(11));
        // last column
        assertEquals(-7, resultSet.getInt(12));
      }
      assertFalse(resultSet.next());
      return null;
    });
  }
}
