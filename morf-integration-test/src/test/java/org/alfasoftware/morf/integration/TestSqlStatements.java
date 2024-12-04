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
import static org.alfasoftware.morf.metadata.SchemaUtils.autonumber;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.blobLiteral;
import static org.alfasoftware.morf.sql.SqlUtils.caseStatement;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.concat;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.isEmpty;
import static org.alfasoftware.morf.sql.SqlUtils.isNotEmpty;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.merge;
import static org.alfasoftware.morf.sql.SqlUtils.nullLiteral;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.selectDistinct;
import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.truncate;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.SqlUtils.when;
import static org.alfasoftware.morf.sql.SqlUtils.windowFunction;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.eq;
import static org.alfasoftware.morf.sql.element.Criterion.in;
import static org.alfasoftware.morf.sql.element.Criterion.like;
import static org.alfasoftware.morf.sql.element.Criterion.not;
import static org.alfasoftware.morf.sql.element.Criterion.or;
import static org.alfasoftware.morf.sql.element.Function.addDays;
import static org.alfasoftware.morf.sql.element.Function.average;
import static org.alfasoftware.morf.sql.element.Function.averageDistinct;
import static org.alfasoftware.morf.sql.element.Function.blobLength;
import static org.alfasoftware.morf.sql.element.Function.coalesce;
import static org.alfasoftware.morf.sql.element.Function.count;
import static org.alfasoftware.morf.sql.element.Function.countDistinct;
import static org.alfasoftware.morf.sql.element.Function.dateToYyyyMMddHHmmss;
import static org.alfasoftware.morf.sql.element.Function.dateToYyyymmdd;
import static org.alfasoftware.morf.sql.element.Function.daysBetween;
import static org.alfasoftware.morf.sql.element.Function.every;
import static org.alfasoftware.morf.sql.element.Function.floor;
import static org.alfasoftware.morf.sql.element.Function.greatest;
import static org.alfasoftware.morf.sql.element.Function.isnull;
import static org.alfasoftware.morf.sql.element.Function.lastDayOfMonth;
import static org.alfasoftware.morf.sql.element.Function.least;
import static org.alfasoftware.morf.sql.element.Function.leftPad;
import static org.alfasoftware.morf.sql.element.Function.leftTrim;
import static org.alfasoftware.morf.sql.element.Function.length;
import static org.alfasoftware.morf.sql.element.Function.lowerCase;
import static org.alfasoftware.morf.sql.element.Function.max;
import static org.alfasoftware.morf.sql.element.Function.mod;
import static org.alfasoftware.morf.sql.element.Function.monthsBetween;
import static org.alfasoftware.morf.sql.element.Function.now;
import static org.alfasoftware.morf.sql.element.Function.power;
import static org.alfasoftware.morf.sql.element.Function.random;
import static org.alfasoftware.morf.sql.element.Function.randomString;
import static org.alfasoftware.morf.sql.element.Function.rightPad;
import static org.alfasoftware.morf.sql.element.Function.rightTrim;
import static org.alfasoftware.morf.sql.element.Function.some;
import static org.alfasoftware.morf.sql.element.Function.substring;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.alfasoftware.morf.sql.element.Function.sumDistinct;
import static org.alfasoftware.morf.sql.element.Function.trim;
import static org.alfasoftware.morf.sql.element.Function.upperCase;
import static org.alfasoftware.morf.sql.element.Function.yyyymmddToDate;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.AbstractSqlDialectTest;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement.ParseResult;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.LocalDate;
import org.joda.time.Months;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
 * @author Copyright (c) Alfa Financial Software 2012
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING) // This should be removed - see WEB-22433
@NotThreadSafe
public class TestSqlStatements { //CHECKSTYLE:OFF

  private static final String TEST_ONLY_RUN_WITH_WINDOW_FUNCTION_SUPPORT = "This test is only run for dialects that support window functions";

  private static final Log log = LogFactory.getLog(TestSqlStatements.class);

  private static final String BLOB1_VALUE = "A Blob named One";
  private static final String BLOB2_VALUE = "A Blob named Two";

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject
  private Provider<DatabaseDataSetConsumer> databaseDataSetConsumer;

  @Inject
  private Provider<DatabaseSchemaManager> schemaManager;

  @Inject
  private ConnectionResources connectionResources;

  @Inject
  private DataSource dataSource;

  @Inject
  private SqlScriptExecutorProvider sqlScriptExecutorProvider;


  /**
   * The test schema.
   */
  private final Schema schema = schema(
    table("SimpleTypes")
      .columns(
        column("stringCol", DataType.STRING, 20).primaryKey(),
        column("nullableStringCol", DataType.STRING, 10).nullable(),
        column("decimalTenZeroCol", DataType.DECIMAL, 10),
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("bigIntegerCol", DataType.BIG_INTEGER),
        column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable(),
        column("blobCol", DataType.BLOB).nullable()
      ),
    table("DateTable")
      .columns(
        column("alfaDate1", DataType.BIG_INTEGER),
        column("alfaDate2", DataType.BIG_INTEGER)
      ),
    table("LastDayOfMonthTable")
      .columns(
        column("alfaDate1", DataType.BIG_INTEGER)
      ),
    table("CoalesceTable")
      .columns(
        column("column1", DataType.DECIMAL, 10).nullable(),
        column("column2", DataType.DECIMAL, 10).nullable(),
        column("column3", DataType.DECIMAL, 10).nullable(),
        column("column4", DataType.STRING, 20).nullable(),
        column("column5", DataType.STRING, 20).nullable()
      ),
    table("BooleanTable")
      .columns(
        column("column1", DataType.BOOLEAN).nullable(),
        column("column2", DataType.BOOLEAN).nullable()
      ),
    table("BlobTable")
      .columns(
        column("column1", DataType.BLOB).nullable(),
        column("column2", DataType.BLOB).nullable()
      ),
    table("AccumulateBooleanTable")
      .columns(
        column("column1", DataType.BOOLEAN).nullable(),
        column("column2", DataType.BOOLEAN).nullable()
      ),
    table("LeftAndRightTrimTable")
      .columns(
        column("indexColumn", DataType.INTEGER).primaryKey(),
        column("stringColumn", DataType.STRING, 30)
      ),
    table("LeftPaddingTable")
      .columns(
        column("id", DataType.INTEGER).primaryKey(),
        column("invoiceNumber", DataType.STRING, 30)
      ),
    table("RightPaddingTable")
      .columns(
        column("id", DataType.INTEGER).primaryKey(),
        column("invoiceNumber", DataType.STRING, 30)
      ),
    table("OrderByNullsLastTable")
      .columns(
        column("field1", DataType.INTEGER).nullable(),
        column("field2", DataType.STRING, 30).nullable()
      ),
    table("SelectFirstTable")
      .columns(
        column("field1", DataType.INTEGER),
        column("field2", DataType.STRING, 30).nullable(),
        column("field3", DataType.INTEGER).nullable()
      ),
    table("AutoNumbered")
      .columns(
        autonumber("surrogateKey", 10),
        column("column2", DataType.STRING, 3).nullable()
      ),
    table("WithDefaultValue")
      .columns(
        column("id", DataType.STRING, 3).primaryKey(),
        column("version", DataType.STRING, 3).defaultValue("0")
      ),
    table("ActualDates")
      .columns(
        column("actualDate", DataType.DATE),
        column("actualDateNullable", DataType.DATE).nullable()
      ),
    table("MergeTable")
      .columns(
        column("column1", DataType.INTEGER).primaryKey(),
        column("column2", DataType.INTEGER).nullable()
      ),
    table("MergeSource")
      .columns(
        column("columnA", DataType.INTEGER).primaryKey(),
        column("columnB", DataType.INTEGER).nullable()
      ),
    table("MergeTableMultipleKeys")
      .columns(
        column("autoNum", DataType.INTEGER).autoNumbered(101).primaryKey(),
        column("column1", DataType.INTEGER),
        column("column2", DataType.INTEGER),
        column("column3", DataType.STRING, 10).nullable(),
        column("column4", DataType.STRING, 10).nullable()
      )
      .indexes(
        index("Index_1").columns("column1", "column2").unique()
      ),
    table("MergeSourceMultipleKeys")
      .columns(
        column("columnA", DataType.INTEGER).primaryKey(),
        column("columnB", DataType.INTEGER).primaryKey(),
        column("columnC", DataType.STRING, 10).nullable()
      ),
    table("MergeSourceJoinTable")
      .columns(
        column("field1", DataType.INTEGER).primaryKey(),
        column("field2", DataType.INTEGER).nullable(),
        column("field3", DataType.STRING, 10).nullable()
      ),
     table("ParameterTable")
      .columns(
        column("parameterCode", DataType.STRING, 10).primaryKey(),
        column("parameterValue", DataType.INTEGER)
      ),
     table("LowerAndUpperTable")
      .columns(
        column("id", DataType.INTEGER).primaryKey(),
        column("firstName", DataType.STRING, 25),
        column("lastName", DataType.STRING, 25)
      ),
     table("MergeAllKeys")
        .columns(
          column("key1", DataType.INTEGER).primaryKey(),
          column("key2", DataType.INTEGER).primaryKey()),
     table("ParamStatementsTest")
        .columns(
          column("one", DataType.INTEGER).primaryKey(),
          column("two", DataType.STRING, 10).primaryKey()),
     table("LikeTest")
        .columns(
          column("column1", DataType.STRING, 10).primaryKey()),
     table("MergeSelectDistinctTable")
        .columns(
          column("column1", DataType.STRING, 10).primaryKey(),
          column("column2", DataType.INTEGER).primaryKey()),
     table("SelectDistinctTable")
        .columns(
          column("id", DataType.INTEGER).primaryKey(),
          column("column1", DataType.STRING, 10),
          column("column2", DataType.INTEGER))
        .indexes(index("SelectDistinctTable_1").columns("column1")),
     table("SelectDistinctJoinTable")
        .columns(
          column("id", DataType.INTEGER).primaryKey(),
          column("column1", DataType.INTEGER),
          column("foreignKeyId", DataType.INTEGER)),
     table("InsertSelectDistinctTable")
        .columns(
          column("column1", DataType.STRING, 10).primaryKey(),
          column("column2", DataType.INTEGER).primaryKey()),
     table("NumericTable")
        .columns(
          column("decimalColumn", DataType.DECIMAL, 13, 2).nullable(),
          column("integerColumn", DataType.INTEGER).nullable()),
     table("InsertTargetTable")
        .columns(
          column("id", DataType.INTEGER).primaryKey(),
          column("column1", DataType.STRING, 10),
          column("column2", DataType.INTEGER)),
     table("WindowFunctionTable")
        .columns(
          column("id", DataType.INTEGER).primaryKey(),
          column("partitionValue1", DataType.STRING, 1),
          column("partitionValue2", DataType.STRING, 1),
          column("aggregationValue", DataType.DECIMAL,13,2))
  );


  /**
   * The test dataset
   */
  private final DataSetProducer dataSet = dataSetProducer(schema)
    .table("SimpleTypes",
      record()
        .setString("stringCol", "hello world AA")
        .setString("nullableStringCol", "not null")
        .setString("decimalTenZeroCol", "9817236")
        .setString("decimalNineFiveCol", "278.231")
        .setLong("bigIntegerCol", 1234567890123456L)
        .setLong("nullableBigIntegerCol", 56732L)
        .setByteArray("blobCol", "hello world BLOB".getBytes())
    )
    .table("DateTable",
      record()
        .setInteger("alfaDate1", 20040609)
        .setInteger("alfaDate2", 20040813), // 65 days difference
        record()
        .setInteger("alfaDate1", 20040609)
        .setInteger("alfaDate2", 20040609), // 0 days difference
        record()
        .setInteger("alfaDate1", 20040609)
        .setInteger("alfaDate2", 20040610), // 1 day difference
      record()
        .setInteger("alfaDate1", 20050813)
        .setInteger("alfaDate2", 20040813), // -365 days difference
      record()
        .setInteger("alfaDate1", 20040213)
        .setInteger("alfaDate2", 20060424) // 801 days difference
    )
    .table("LastDayOfMonthTable",
      record()
      .setInteger("alfaDate1", 20090701), // 31 day month
    record()
      .setInteger("alfaDate1", 20090615), // 30 day month
    record()
      .setInteger("alfaDate1", 20090131), // last day of month
    record()
      .setInteger("alfaDate1", 20080201), // leap year
    record()
      .setInteger("alfaDate1", 20000201), // leap year (divisible by 100 but also divisible by 400)
    record()
      .setInteger("alfaDate1", 21000201) // not leap year (divisible by 100 but not divisible by 400)
    )
    .table("CoalesceTable",
      record()
        .setString("column1", null)
        .setString("column2", null)
        .setString("column3", "5")
        .setString("column4", "Pumpkin")
        .setString("column5", null),
      record()
        .setString("column1", null)
        .setString("column2", "7")
        .setString("column3", "3")
        .setString("column4", "Green")
        .setString("column5", "Man")
    )
    .table("BooleanTable",
      record()
        .setBoolean("column1", false)
        .setBoolean("column2", true)
    )
    .table("BlobTable",
      record()
        .setByteArray("column1", BLOB1_VALUE.getBytes())
        .setByteArray("column2", BLOB2_VALUE.getBytes())
    )
    .table("AccumulateBooleanTable",
      record()
        .setBoolean("column1", false)
        .setBoolean("column2", true),
      record()
        .setBoolean("column1", true)
        .setBoolean("column2", true)
    )
    .table("LeftAndRightTrimTable",
      record()
        .setInteger("indexColumn", 1)
        .setString("stringColumn", "hello world"),
      record()
        .setInteger("indexColumn", 2)
        .setString("stringColumn", "test string     "),
      record()
        .setInteger("indexColumn", 3)
        .setString("stringColumn", "     purple flowers     "),
      record()
        .setInteger("indexColumn", 4)
        .setString("stringColumn", "     pancakes")
    )
    .table("OrderByNullsLastTable",
      record()
        .setInteger("field1", 1)
        .setString("field2", null),
      record()
        .setInteger("field1", 1)
        .setString("field2", "2"),
      record()
        .setInteger("field1", null)
        .setString("field2", "3"),
      record()
        .setInteger("field1", null)
        .setString("field2", null),
      record()
        .setInteger("field1", 3)
        .setString("field2", "3"),
      record()
        .setInteger("field1", 3)
        .setString("field2", "4")
    )
    .table("SelectFirstTable",
      record()
        .setInteger("field1", 1)
        .setString("field2", "2"),
      record()
        .setInteger("field1", 2)
        .setString("field2", "2"),
      record()
        .setInteger("field1", 2)
        .setString("field2", "3"),
      record()
        .setInteger("field1", 3)
        .setString("field2", "3"),
      record()
        .setInteger("field1", 3)
        .setString("field2", "4"),
      record()
        .setInteger("field1", 5)
        .setString("field2", "4")
    )
    .table("LeftPaddingTable",
      record()
        .setInteger("id", 1)
        .setString("invoiceNumber", "Invoice100"),
      record()
        .setInteger("id", 2)
        .setString("invoiceNumber", "BigInvoiceNumber1000"),
      record()
        .setInteger("id", 3)
        .setString("invoiceNumber", "ExactFifteeeeen")
    )
    .table("RightPaddingTable",
      record()
        .setInteger("id", 1)
        .setString("invoiceNumber", "Invoice100"),
      record()
        .setInteger("id", 2)
        .setString("invoiceNumber", "BigInvoiceNumber1000"),
      record()
        .setInteger("id", 3)
        .setString("invoiceNumber", "ExactFifteeeeen")
    )
    .table("AutoNumbered",
      record()
        .setInteger("surrogateKey", 3)
        .setString("column2", "c"),
      record()
        .setInteger("surrogateKey", 2)
        .setString("column2", "d")
    )
    .table("WithDefaultValue",
      record()
        .setInteger("id", 1)
        .setInteger("version", 6),
      record()
        .setInteger("id", 2)
        .setInteger("version", 6)
    )
    .table("ActualDates",
      record()
        .setDate("actualDate", java.sql.Date.valueOf("1899-01-01"))
        .setDate("actualDateNullable", java.sql.Date.valueOf("9999-12-31")),
      record()
        .setDate("actualDate", java.sql.Date.valueOf("1995-10-23")),
      record()
        .setDate("actualDate", java.sql.Date.valueOf("2020-01-31")),
      record()
        .setDate("actualDate", java.sql.Date.valueOf("2100-12-31"))
        )
    .table("MergeSource",
      record()
        .setInteger("columnA", 100)
        .setInteger("columnB", 200),
      record()
        .setInteger("columnA", 500)
        .setInteger("columnB", 999)
     )
    .table("MergeTable",
      record()
        .setInteger("column1", 500)
        .setInteger("column2", 800)
    )
    .table("MergeSourceMultipleKeys",
      record()
        .setInteger("columnA", 100)
        .setInteger("columnB", 200)
        .setString("columnC", "Inserted"),
      record()
        .setInteger("columnA", 500)
        .setInteger("columnB", 800)
        .setString("columnC", "Updated")
     )
    .table("MergeTableMultipleKeys",
      record()
        .setInteger("autoNum", 33)
        .setInteger("column1", 500)
        .setInteger("column2", 800)
        .setString("column3", "Incorrect")
     )
    .table("MergeSourceJoinTable",
      record()
        .setInteger("field1", 100)
        .setInteger("field2", 999)
        .setString("field3", "Asset")
     )
    .table("ParameterTable",
      record()
        .setString("parameterCode", "Test")
        .setInteger("parameterValue", 0)
     )
    .table("LowerAndUpperTable",
      record()
        .setInteger("id", 1)
        .setString("firstName", "LuDWig vAn")
        .setString("lastName", "BEEthoven"),
      record()
        .setInteger("id", 2)
        .setString("firstName", "WolfGANG amaDEUS")
        .setString("lastName", "MoZArt"),
      record()
        .setInteger("id", 3)
        .setString("firstName", "joHANN sEbAsTiAn")
        .setString("lastName", "Bach")
     )
     .table("MergeAllKeys",
       record()
       .setInteger("key1", 1)
       .setInteger("key2", 2),
      record()
        .setInteger("key1", 100)
        .setInteger("key2", 200)
     )
     .table("ParamStatementsTest",
       record()
       .setInteger("one", 0)
       .setString("two", "bla"))
    .table("LikeTest",
      record().setString("column1", "xxxxxxx"),
      record().setString("column1", "1xxxxxx"),
      record().setString("column1", "xxxxxx2"),
      record().setString("column1", "xxx3xxx"),
      record().setString("column1", "4xxxxx5"),
      record().setString("column1", "xxx*xxx"),
      record().setString("column1", "xxx%xxx"))
    .table("MergeSelectDistinctTable",
      record()
        .setString("column1", "<None>")
        .setInteger("column2", -100))
    .table("SelectDistinctTable",
      record()
        .setInteger("id", 1)
        .setString("column1", "TEST1")
        .setInteger("column2", 1),
      record()
        .setInteger("id", 2)
        .setString("column1", "TEST2")
        .setInteger("column2", 1))
    .table("SelectDistinctJoinTable",
      record()
        .setInteger("id", 1)
        .setInteger("column1", 10)
        .setInteger("foreignKeyId", 1),
      record()
        .setInteger("id", 2)
        .setInteger("column1", 11)
        .setInteger("foreignKeyId", 1),
      record()
        .setInteger("id", 3)
        .setInteger("column1", 12)
        .setInteger("foreignKeyId", 1),
      record()
        .setInteger("id", 4)
        .setInteger("column1", 5)
        .setInteger("foreignKeyId", 2))
     .table("InsertSelectDistinctTable",
      record()
        .setString("column1", "<None>")
        .setInteger("column2", -100))
     .table("NumericTable",
       record()
         .setString("decimalColumn", "923764237.23")
         .setInteger("integerColumn", 232131),
       record()
         .setString("decimalColumn", "123456789.3")
         .setInteger("integerColumn", 2132131),
       record()
         .setString("decimalColumn", "4237.43")
         .setInteger("integerColumn", 212131),
       record()
         .setString("decimalColumn", "4237.43")
         .setInteger("integerColumn", 212131),
       record()
         .setString("decimalColumn", "92337.29")
         .setInteger("integerColumn", 21323),
         record()
         .setString("decimalColumn", null)
         .setInteger("integerColumn", null),
       record()
         .setString("decimalColumn", "92376427.13")
         .setInteger("integerColumn", 213231)
      )
     .table("InsertTargetTable")
     .table("WindowFunctionTable",
       record()
         .setInteger("id", 1)
         .setString("partitionValue1", "A")
         .setString("partitionValue2", "Z")
         .setString("aggregationValue", "2.1"),
       record()
         .setInteger("id", 2)
         .setString("partitionValue1", "A")
         .setString("partitionValue2", "Y")
         .setString("aggregationValue", "3.2"),
       record()
         .setInteger("id", 6)
         .setString("partitionValue1", "B")
         .setString("partitionValue2", "Z")
         .setString("aggregationValue", "3.4"),
       record()
         .setInteger("id", 3)
         .setString("partitionValue1", "B")
         .setString("partitionValue2", "Z")
         .setString("aggregationValue", "5.7"),
       record()
         .setInteger("id", 4)
         .setString("partitionValue1", "A")
         .setString("partitionValue2", "Y")
         .setString("aggregationValue", "3.8"),
       record()
         .setInteger("id", 5)
         .setString("partitionValue1", "A")
         .setString("partitionValue2", "Z")
         .setString("aggregationValue", "1.9"),
       record()
         .setInteger("id", 7)
         .setString("partitionValue1", "B")
         .setString("partitionValue2", "Y")
         .setString("aggregationValue", "10.2")
       );


  private Connection connection;

  /**
   * Setup the schema for the tests.
   */
  @Before
  public void before() throws SQLException {
    // We don't want to inherit some old sequence numbers on existing tables
    // therefore we simply drop any tables with auto-numbering on them
    schemaManager.get().invalidateCache();
    schemaManager.get().dropTablesIfPresent(ImmutableSet.of("Autonumbered", "MergeTableMultipleKeys"));
    // no need to truncate the tables, the connector does that anyway
    schemaManager.get().mutateToSupportSchema(schema, TruncationBehavior.ONLY_ON_TABLE_CHANGE);
    new DataSetConnector(dataSet, databaseDataSetConsumer.get()).connect();

    connection = dataSource.getConnection();
  }


  @After
  public void after() throws SQLException {
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }


  /**
   * Ensures that we attempt a toString on any statements when we parse them, just to make
   * sure that method doesn't break in response to various combinations of SQL elements.
   *
   * @param statement
   * @return
   */
  private String convertStatementToSQL(SelectStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }
  private String convertStatementToSQL(SelectFirstStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }
  private String convertStatementToSQL(TruncateStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }
  private String convertStatementToSQL(MergeStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }
  private List<String> convertStatementToSQL(InsertStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }
  private List<String> convertStatementToSQL(InsertStatement statement, Schema schema, Table table) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement, schema, table);
  }
  private String convertStatementToSQL(UpdateStatement statement) {
    String string = statement.toString(); // Don't condition this. We definitely always want to do it.
    log.debug(string);
    return connectionResources.sqlDialect().convertStatementToSQL(statement);
  }


  /**
   * Verifies that the {@link MergeStatement} can be used and provide
   * outputs valid SQL for all database platforms.
   * @throws SQLException
   */
  @Test
  public void AtestMergeStatementSimple() throws SQLException {

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement testSelectForInsert = select(field("column1"), field("column2"))
                                          .from(tableRef("MergeTable"))
                                           .where(eq(field("column1"), literal(100)));

    SelectStatement testSelectForUpdate = select(field("column1"), field("column2"))
                                          .from(tableRef("MergeTable"))
                                          .where(eq(field("column1"), literal(500)));

    TableReference mergeSource = tableRef("MergeSource");

    SelectStatement sourceStmt = select(mergeSource.field("columnA").as("column1"),
                                        mergeSource.field("columnB").as("column2"))
                                 .from(mergeSource);

    TableReference mergeTable = tableRef("MergeTable");

    MergeStatement mergeStmt = merge()
                                .into(mergeTable)
                                .tableUniqueKey(mergeTable.field("column1"))
                                .from(sourceStmt);

    executor.execute(ImmutableList.of(convertStatementToSQL(mergeStmt)), connection);

    // Check result for Inserted record
     String sqlForInsertedRecord = convertStatementToSQL(testSelectForInsert);

     Integer numberOfInsertedRecords = executor.executeQuery(sqlForInsertedRecord, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 value not correctly set/returned after merge", 100, resultSet.getInt(1));
          assertEquals("column2 value value not correctly set/returned after merge", 200, resultSet.getInt(2));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfInsertedRecords.intValue());

    // Check result for Updated record
    String sqlForUpdatedRecord = convertStatementToSQL(testSelectForUpdate);

    Integer numberOfUpdatedRecords = executor.executeQuery(sqlForUpdatedRecord, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 value not correctly set/returned after merge", 500, resultSet.getInt(1));
          assertEquals("column2 value value not correctly set/returned after merge", 999, resultSet.getInt(2));
        }
      return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfUpdatedRecords.intValue());
  }


  /**
   * Verifies that the {@link MergeStatement} can be used for multiple keys
   * @throws SQLException
   */
  @Test
  public void BtestMergeStatementWithMultipleKeys() throws SQLException {

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement testSelectForUpdate = select(field("autoNum"), field("column1"), field("column2"), field("column3"))
                                            .from(tableRef("MergeTableMultipleKeys"))
                                            .where(
                                              and(
                                                eq(field("column1"), literal(100)),
                                                eq(field("column2"), literal(200))));

    SelectStatement testSelectForInsert = select(field("autoNum"), field("column1"), field("column2"), field("column3"))
                                            .from(tableRef("MergeTableMultipleKeys"))
                                            .where(
                                              and(
                                                eq(field("column1"), literal(500)),
                                                eq(field("column2"), literal(800))));


    TableReference mergeSourceMultipleKeys = tableRef("MergeSourceMultipleKeys");

    SelectStatement sourceStmt = select(mergeSourceMultipleKeys.field("columnA").as("column1"),
                                        mergeSourceMultipleKeys.field("columnB").as("column2"),
                                        mergeSourceMultipleKeys.field("columnC").as("column3"))
                                 .from(mergeSourceMultipleKeys)
                                 .alias("xxx");

    TableReference mergeTableMultipleKeys = tableRef("MergeTableMultipleKeys");

    MergeStatement mergeStmt = merge()
                                .into(mergeTableMultipleKeys)
                                .tableUniqueKey(mergeTableMultipleKeys.field("column1"),
                                                mergeTableMultipleKeys.field("column2"))
                                .from(sourceStmt);

    executor.execute(ImmutableList.of(convertStatementToSQL(mergeStmt)), connection);

    // Check result for inserted
    String sqlForInsertedRecord = convertStatementToSQL(testSelectForUpdate);

    Integer numberOfInsertedRecords = executor.executeQuery(sqlForInsertedRecord, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("autoNum value should be inserted as 101", 101, resultSet.getInt(1));
          assertEquals("column1 value not correctly set/returned after merge", 100, resultSet.getInt(2));
          assertEquals("column2 value value not correctly set/returned after merge", 200, resultSet.getInt(3));
          assertEquals("column3 value value not correctly set/returned after merge", "Inserted", resultSet.getString(4));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfInsertedRecords.intValue());

    // Check result for updated
    String sqlForUpdatedRecord = convertStatementToSQL(testSelectForInsert);

    Integer numberOfUpdatedRecords = executor.executeQuery(sqlForUpdatedRecord, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("autoNum value should not be updated and remain as 33", 33, resultSet.getInt(1));
          assertEquals("column1 value not correctly set/returned after merge", 500, resultSet.getInt(2));
          assertEquals("column2 value value not correctly set/returned after merge", 800, resultSet.getInt(3));
          assertEquals("column3 value value not correctly set/returned after merge", "Updated", resultSet.getString(4));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfUpdatedRecords.intValue());
  }


  /**
   * Verifies that the {@link MergeStatement} works for complex sql statements
   * @throws SQLException
   */
  @Test
  public void CtestMergeStatementComplex() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement testSelect = select(field("column1"), field("column2"), field("column3"))
                                 .from(tableRef("MergeTableMultipleKeys"))
                                 .where(
                                   and(
                                     eq(field("column1"), literal(100)),
                                     eq(field("column2"), literal(200))
                                   )
                                 );

    TableReference mergeSource = tableRef("MergeSource");
    TableReference mergeSourceMultipleKeys = tableRef("MergeSourceMultipleKeys");
    TableReference mergeSourceJoinTable = tableRef("MergeSourceJoinTable");
    TableReference mergeTableMultipleKeys = tableRef("MergeTableMultipleKeys");


    SelectStatement subSelect = select(mergeSource.field("columnA"),
                                      mergeSource.field("columnB"),
                                      mergeSourceJoinTable.field("field3"))
                                .from(mergeSource)
                                .innerJoin(mergeSourceJoinTable, eq(mergeSource.field("columnA"), mergeSourceJoinTable.field("field1")))
                                .alias("subSelect");

    TableReference subSelectTable = subSelect.asTable().as("subSelect");

    SelectStatement selectStmt = select(mergeSourceMultipleKeys.field("columnA").as("column1"),
                                        mergeSourceMultipleKeys.field("columnB").as("column2"),
                                        subSelectTable.field("field3").as("column3"))
                                 .from(mergeSourceMultipleKeys)
                                 .innerJoin(subSelect,
                                                 and(
                                                   eq(mergeSourceMultipleKeys.field("columnA"), subSelectTable.field("columnA")),
                                                   eq(mergeSourceMultipleKeys.field("columnB"), subSelectTable.field("columnB"))))
                                 .alias("xxx");

    MergeStatement mergeStmt = merge()
                              .into(mergeTableMultipleKeys)
                              .tableUniqueKey(mergeTableMultipleKeys.field("column1"), mergeTableMultipleKeys.field("column2"))
                              .from(selectStmt);

    executor.execute(ImmutableList.of(convertStatementToSQL(mergeStmt)), connection);

    // Check result
    String sql = convertStatementToSQL(testSelect);

     Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 value not correctly set/returned after merge", 100, resultSet.getInt(1));
          assertEquals("column2 value value not correctly set/returned after merge", 200, resultSet.getInt(2));
          assertEquals("column3 value value not correctly set/returned after merge", "Asset", resultSet.getString(3));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfRecords.intValue());
  }


  /**
   * Verifies that the {@link MergeStatement} can be used with aggregate functions
   * @throws SQLException
   */
  @Test
  public void DtestMergeStatementWithAggregateFunctions() throws SQLException {

   SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

   TableReference parameterTable = tableRef("ParameterTable");
   TableReference mergeSource = tableRef("MergeSource");

   SelectStatement testSelect = select(field("parameterCode"), field("parameterValue"))
                                  .from(tableRef("ParameterTable"))
                                   .where(
                                   eq(field("parameterCode"), literal("aggregate"))
                                   );

   SelectStatement select = select(new FieldLiteral("aggregate").as("parameterCode"),
                              max(mergeSource.field("columnA")).as("parameterValue"))
                              .from(mergeSource);

   MergeStatement merge =  merge()
                             .into(parameterTable)
                             .tableUniqueKey(parameterTable.field("parameterCode"))
                             .from(select);

   executor.execute(ImmutableList.of(convertStatementToSQL(merge)), connection);

   // Check result
   String sql = convertStatementToSQL(testSelect);

    Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("code not correctly set/returned after merge", "aggregate", resultSet.getString(1));
          assertEquals("value not correctly set/returned after merge", 500, resultSet.getInt(2));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfRecords.intValue());
  }


  /**
   * Verifies that the {@link InsertStatement} can be used with an INSERT...
   * SELECT.. HAVING(...) query and outputs valid SQL for all database
   * platforms.
   */
  @Test
  public void EtestInsertStatementWithSelectHaving() {
    // Key value
    final String primaryKeyValue = "hello world AA";

    // Tables.
    final TableReference simpleTypes = new TableReference("SimpleTypes");

    // Where clause for SELECT.
    final Criterion whereStringCol = eq(new FieldReference(simpleTypes, "stringCol"), primaryKeyValue);

    // Sub-selects.
    final Criterion havingCriteria = eq(count(), 0);
    final SelectStatement selectFromSimpleTypes = new SelectStatement(
      new FieldLiteral(primaryKeyValue).as("stringCol"),
      new FieldLiteral("not\\'null'").as("nullableStringCol"),
      new FieldLiteral(9817236).as("decimalTenZeroCol"),
      new FieldLiteral(278.231).as("decimalNineFiveCol"),
      new FieldLiteral("1234567890123456", DataType.DECIMAL).as("bigIntegerCol"),
      new FieldLiteral("56732", DataType.DECIMAL).as("nullableBigIntegerCol"),
      nullLiteral().as("nullableBlobCol")
    ).from(simpleTypes)
    .where(whereStringCol)
    .groupBy(field("stringCol"))
    .having(havingCriteria);

    // Insert.
    final InsertStatement insertIntoSimpleTypes = new InsertStatement()
      .into(simpleTypes)
      .from(selectFromSimpleTypes);

    // Run the SQL
    sqlScriptExecutorProvider.get().execute(
      convertStatementToSQL(insertIntoSimpleTypes, schema, schema.getTable("simpleTypes")));

    // Check there is still just a single record
    assertRecordsInTable(1, "SimpleTypes");
  }


  /**
   * Verifies that the truncate statement works on the various platforms.
   */
  @Test
  public void testTruncateTable() {
    sqlScriptExecutorProvider.get().execute(Collections.singletonList(convertStatementToSQL(truncate(tableRef("SimpleTypes")))));
    assertRecordsInTable(0, "SimpleTypes");
  }


  /**
   * Test the lastDayOfMonth SQL function against all {@linkplain SqlDialect}s
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testLastDayOfMonth() throws SQLException {

    /*
    * Source data

    .table("LastDayOfMonthTable",
      record()
      .value("alfaDate1", "20090701"), // 31 day month
    record()
      .value("alfaDate1", "20090615"), // 30 day month
    record()
      .value("alfaDate1", "20090131"), // last day of month
    record()
      .value("alfaDate1", "20080201"), // leap year
    record()
      .value("alfaDate1", "20000201"), // leap year (divisible by 100 but also divisible by 400)
    record()
      .value("alfaDate1", "21000201") // not leap year (divisible by 100 but not divisible by 400)
      */

    List<java.sql.Date> expectedLastDays = ImmutableList.of(java.sql.Date.valueOf("2009-07-31"),
                                                            java.sql.Date.valueOf("2009-06-30"),
                                                            java.sql.Date.valueOf("2009-01-31"),
                                                            java.sql.Date.valueOf("2008-02-29"),
                                                            java.sql.Date.valueOf("2000-02-29"),
                                                            java.sql.Date.valueOf("2100-02-28"));

    SelectStatement testStatement = select(
        lastDayOfMonth(yyyymmddToDate(new Cast(field("alfaDate1"), DataType.STRING, 8))))
        .from(tableRef("LastDayOfMonthTable"));

    // Run the SQL
    Statement stmt = connection.createStatement();
    try {
      ResultSet rs = stmt.executeQuery(convertStatementToSQL(testStatement));
      try {
        int counter = 0;
        while (rs.next()) {
          assertEquals(expectedLastDays.get(counter++), rs.getDate(1));
        }
      } finally {
        rs.close();
      }
    } finally {
      stmt.close();
    }
  }


  /**
   * Test the daysBetween SQL function against all {@linkplain SqlDialect}s
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void FtestDaysBetween() throws SQLException {
    int counter = 0;
    List<Integer> expectedDaysLate = ImmutableList.of(65, 0, 1, -365, 801);

    SelectStatement testStatement = select(
        daysBetween(yyyymmddToDate(new Cast(field("alfaDate1"), DataType.STRING, 8)),
                    yyyymmddToDate(new Cast(field("alfaDate2"), DataType.STRING, 8))))
        .from(tableRef("DateTable"));

    // Run the SQL
    Statement stmt = connection.createStatement();
    try {
      ResultSet rs = stmt.executeQuery(convertStatementToSQL(testStatement));
      try {
        while (rs.next()) {
          assertEquals(expectedDaysLate.get(counter++).intValue(), rs.getInt(1));
        }
      } finally {
        rs.close();
      }
    } finally {
      stmt.close();
    }
  }


  /**
   * Test the coalesce SQL function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void GtestCoalesce() throws SQLException {
    final List<Integer> expectedInt = ImmutableList.of(5, 7);
    final List<String> expectedString = ImmutableList.of("Pumpkin", "Green");

    SelectStatement testStatement1 = select(coalesce(field("column1"), field("column2"), field("column3")))
                                      .from(tableRef("CoalesceTable"));
    SelectStatement testStatement2 = select(coalesce(field("column4"), field("column5")))
                                      .from(tableRef("CoalesceTable"));

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    executor.executeQuery(convertStatementToSQL(testStatement1), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int counter = 0;
        while (resultSet.next()) {
          assertEquals(expectedInt.get(counter++).intValue(), resultSet.getInt(1));
        }
        return null;
      }
    });

    executor.executeQuery(convertStatementToSQL(testStatement2), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int counter = 0;
        while (resultSet.next()) {
          assertEquals(expectedString.get(counter++), resultSet.getString(1));
        }
        return null;
      }
    });
  }


  /**
   * Tests selecting values from nowhere.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void UtestNoTableSelect() throws SQLException {
    SelectStatement select = select(literal(1), literal("foo"));

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    executor.executeQuery(convertStatementToSQL(select), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        assertEquals("Integer value", 1, resultSet.getInt(1));
        assertEquals("String value", "foo", resultSet.getString(2));
        assertFalse("More than one record", resultSet.next());
        return null;
      }
    });
  }


  /**
   * Tests auto numbering of records by the RDBMS
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void HtestAutoNumber() throws SQLException {

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    InsertStatement insertStatement = insert()
       .into(tableRef("AutoNumbered"))
       .fields(
          field("column2")
       )
       .values(
          literal("a").as("column2")
       );
    List<String> insertSql = convertStatementToSQL(insertStatement);
    executor.execute(insertSql);

    insertStatement = insert()
        .into(tableRef("AutoNumbered"))
        .fields(
           field("column2")
        )
        .values(
           literal("b").as("column2")
        );
    insertSql = convertStatementToSQL(insertStatement, schema, null);
    executor.execute(insertSql);

    SelectStatement select = select(field("surrogateKey"), field("column2"))
                            .from(tableRef("AutoNumbered"))
                            .orderBy(field("surrogateKey"));


    final List<Long> expectedAutonumber = Arrays.asList(2L,3L,10L);
    executor.executeQuery(convertStatementToSQL(select), new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int counter = 0;
        while (resultSet.next()) {
          switch (counter) {
            case 0:
              assertEquals("Data set record 1 - long", (long)expectedAutonumber.get(counter), resultSet.getLong(1));
              assertEquals("Data set record 1 - string", "d", resultSet.getString(2));
              break;
            case 1:
              assertEquals("Data set record 2 - long", (long)expectedAutonumber.get(counter), resultSet.getLong(1));
              assertEquals("Data set record 2 - string", "c", resultSet.getString(2));
              break;
            case 2:
              assertEquals("Inserted record 1 - long", (long)expectedAutonumber.get(counter), resultSet.getLong(1));
              assertEquals("Inserted record 1 - string", "a", resultSet.getString(2));
              break;
            case 3:
              //AutoNumber cannot be expected to be sequential.
              assertFalse("Inserted record 2, long, should be unique", expectedAutonumber.contains(resultSet.getLong(1)));
              assertTrue("Inserted record 2, long, should be greater than the autonumber start", resultSet.getLong(1) > 10L);
              assertEquals("Inserted record 2 - string", "b", resultSet.getString(2));
              break;
            default:
             fail("More records returned than expected");
          }
          counter++;
        }
        return null;
      }
    });
  }


  /**
   * Test the behaviour of SELECTs, INSERTs and UPDATEs of boolean fields.  In the process
   * we test a lot of {@link SqlScriptExecutor}'s statement handling capabilities
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void ItestBooleanFields() throws SQLException {
    ImmutableList<Boolean> listOfTrueAndFalse = ImmutableList.of(true, false); // deliberately not a List<Object>

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    // Set up queries
    InsertStatement insertStatement = insert()
                                     .into(tableRef("BooleanTable"))
                                     .fields(field("column1"), field("column2"))
                                     .values(literal(false).as("column1"), literal(true).as("column2"));
    SelectStatement selectStatement = select(field("column1"), field("column2"))
                                     .from(tableRef("BooleanTable"))
                                     .where(or(
                                       field("column1").eq(true),
                                       field("column1").eq(false),
                                       field("column1").eq(literal(true)),
                                       field("column1").eq(literal(false)),
                                       field("column1").in(true, false),
                                       field("column1").in(literal(true), literal(false)),
                                       field("column1").in(listOfTrueAndFalse)
                                     ));
    UpdateStatement updateStatement = update(tableRef("BooleanTable"))
                                     .set(literal(true).as("column1"), literal(false).as("column2"));

    // Insert
    executor.execute(convertStatementToSQL(insertStatement, schema, null), connection);

    // Check result - note that this is deliberately not tidy - we are making sure that results get
    // passed back up to this scope correctly.
    String sql = convertStatementToSQL(selectStatement);
    Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 boolean value not correctly set/returned after insert", false, resultSet.getBoolean(1));
          assertEquals("column2 boolean value not correctly set/returned after insert", true, resultSet.getBoolean(2));
        }
        return result;
      }
    });
    assertEquals("Should be exactly two records", 2, numberOfRecords.intValue());

    // Update
    executor.execute(ImmutableList.of(convertStatementToSQL(updateStatement)), connection);

    // Check result- note that this is deliberately not tidy - we are making sure that results get
    // passed back up to this scope correctly.
    numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 boolean value not correctly set/returned after insert", true, resultSet.getBoolean(1));
          assertEquals("column2 boolean value not correctly set/returned after insert", false, resultSet.getBoolean(2));
        }
        return result;
      }
    });
    assertEquals("Should be exactly two records", 2, numberOfRecords.intValue());
  }


  /**
   * Test the behaviour of SELECTs, INSERTs and UPDATEs of true Date fields.  In the process
   * we test a lot of {@link SqlScriptExecutor}'s statement handling capabilities
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void JtestDateFields() throws SQLException {

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor() {
      @Override
      public void afterExecute(String sql, long numberOfRowsUpdated) {
        // we run some huge queries in this test, and loggers do not like that very much
        String substring = sql.length() > 2048 ? sql.substring(0, 2048) + "..." : sql;
        super.afterExecute(substring, numberOfRowsUpdated);
      }
    });

    // Set up queries
    InsertStatement insertStatement = insert()
                                     .into(tableRef("ActualDates"))
                                     .fields(
                                       field("actualDate")
                                      )
                                     .values(
                                       literal(new LocalDate(1999, 12, 31)).as("actualDate")
                                      );
    SelectStatement selectStatement = select(field("actualDate"), field("actualDateNullable"))
                                     .from(tableRef("ActualDates"))
                                     .orderBy(field("actualDate"));
    UpdateStatement updateStatement = update(tableRef("ActualDates"))
                                     .set(
                                       literal(new LocalDate(2000, 1, 1)).as("actualDate"),
                                       literal(new LocalDate(1998, 1, 1)).as("actualDateNullable")
                                      )
                                      .where(
                                        field("actualDate").in(new LocalDate(1999, 12, 31), literal(new LocalDate(5000, 12, 31)))
                                      );

    // Insert
    executor.execute(convertStatementToSQL(insertStatement, schema, null), connection);

    // Check result
    String sql = convertStatementToSQL(selectStatement);
    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          switch (result) {
            case 1:
              assertEquals("actualDate row 0 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("1899-01-01"), resultSet.getDate(1));
              assertEquals("actualDateNullable row 0 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("9999-12-31"), resultSet.getDate(2));
              break;
            case 2:
              assertEquals("actualDate row 1 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("1995-10-23"), resultSet.getDate(1));
              assertNull("actualDateNullable row 1 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            case 3:
              assertEquals("actualDate date value not correctly set/returned after insert", java.sql.Date.valueOf("1999-12-31"), resultSet.getDate(1));
              assertNull("actualDateNullable date value not correctly set/returned after insert", resultSet.getDate(2));
              break;
            case 4:
              assertEquals("actualDate row 4 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("2020-01-31"), resultSet.getDate(1));
              assertNull("actualDateNullable row 4 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            case 5:
              assertEquals("actualDate row 5 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("2100-12-31"), resultSet.getDate(1));
              assertNull("actualDateNullable row 5 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            default: fail("Should be exactly 5 records");
          }
        }
        return null;
      }
    });

    // Update
    executor.execute(ImmutableList.of(convertStatementToSQL(updateStatement)), connection);

    // Check result
    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          switch (result) {
            case 1:
              assertEquals("actualDate row 0 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("1899-01-01"), resultSet.getDate(1));
              assertEquals("actualDateNullable row 0 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("9999-12-31"), resultSet.getDate(2));
              break;
            case 2:
              assertEquals("actualDate row 1 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("1995-10-23"), resultSet.getDate(1));
              assertNull("actualDateNullable row 1 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            case 3:
              assertEquals("actualDate date value not correctly set/returned after update", java.sql.Date.valueOf("2000-01-01"), resultSet.getDate(1));
              assertEquals("actualDateNullable date value not correctly set/returned after update", java.sql.Date.valueOf("1998-01-01"), resultSet.getDate(2));
              break;
            case 4:
              assertEquals("actualDate row 4 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("2020-01-31"), resultSet.getDate(1));
              assertNull("actualDateNullable row 4 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            case 5:
              assertEquals("actualDate row 5 date value not correctly set/returned after dataset load", java.sql.Date.valueOf("2100-12-31"), resultSet.getDate(1));
              assertNull("actualDateNullable row 5 date value not correctly set/returned after dataset load", resultSet.getDate(2));
              break;
            default: fail("Should be exactly 5 records");
          }
        }
        return null;
      }
    });

    // Month between tests
    ImmutableList.Builder<LocalDate> fromDates = ImmutableList.builder();
    fromDates.add(new LocalDate(1995, 10, 31))
             .add(new LocalDate(2021, 7, 27))
             .add(new LocalDate(2101, 11, 1))
             .add(LocalDate.now().plusYears(new Random().nextInt(10)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40)))
             .add(LocalDate.now().plusYears(new Random().nextInt(20)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40)))
             .add(LocalDate.now().plusYears(new Random().nextInt(40)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40)))
             .add(LocalDate.now().plusYears(new Random().nextInt(70)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40)))
             .add(LocalDate.now().plusYears(new Random().nextInt(100)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40)));
   for (final LocalDate fromDate : fromDates.build()) {

    final List<Function> monthBetweenListSql = new ArrayList<>();
    final List<Integer> monthBetweenListComp = new ArrayList<>();
    final StringBuilder failures = new StringBuilder();
    final MutableBoolean haveFailures = new MutableBoolean(false);

    for (int i = -366; i < 366; i++) {
      LocalDate toDate = fromDate.plusDays(i);
      monthBetweenListSql.add(monthsBetween(literal(fromDate), literal(toDate)));
      monthBetweenListComp.add(Months.monthsBetween(fromDate, toDate).getMonths());
    }

    for (int i = -50; i < 50; i++) {
      LocalDate toDate = fromDate.plusMonths(i);
      monthBetweenListSql.add(monthsBetween(literal(fromDate), literal(toDate)));
      monthBetweenListComp.add(Months.monthsBetween(fromDate, toDate).getMonths());
    }

    for (int i = 0; i < 100; i++) {
      LocalDate toDate = LocalDate.now().plusYears(new Random().nextInt(10)).plusMonths(new Random().nextInt(20)).plusDays(new Random().nextInt(40));
      monthBetweenListSql.add(monthsBetween(literal(fromDate), literal(toDate)));
      monthBetweenListComp.add(Months.monthsBetween(fromDate, toDate).getMonths());
    }

    SelectStatement monthBetweenSelect = select(monthBetweenListSql)
        .from("ActualDates")
        .where(field("actualDate").eq(new LocalDate(1995, 10, 23)));

    executor.executeQuery(convertStatementToSQL(monthBetweenSelect), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          switch (result) {
            case 1:
              for (int i = 0; i < monthBetweenListComp.size(); i++) {
                int actual = resultSet.getInt(i + 1);
                int expected = monthBetweenListComp.get(i).intValue();
                if (expected != actual) {
                  String callDescription = "monthsBetween(" +
                      ((FieldLiteral)monthBetweenListSql.get(i).getArguments().get(1)).getValue() + ", " +
                      ((FieldLiteral)monthBetweenListSql.get(i).getArguments().get(0)).getValue() + ")";
                  failures.append(callDescription +
                      " expected = " + expected +
                      " actual = " + actual + "\n");
                  haveFailures.setValue(true);
                }
              }
              break;
            default: fail("Should be exactly 1 record");
          }
        }
        return null;
      }
    });
    assertFalse(failures.toString(), haveFailures.booleanValue());
   }
  }


    /**
     * Test the behaviour of SELECTs, INSERTs and UPDATEs of blob fields.  In the process
     * we test a lot of {@link SqlScriptExecutor}'s statement handling capabilities
     *
     * @throws SQLException if something goes wrong.
     */
    @Test
    public void testBlobFields() throws SQLException {

        SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

        // Set up queries
        InsertStatement insertStatement = insert()
                .into(tableRef("BlobTable"))
                .fields(field("column1"), field("column2"))
                .values(blobLiteral(BLOB1_VALUE).as("column1"), blobLiteral(BLOB2_VALUE.getBytes()).as("column2"));
        SelectStatement selectStatementAfterInsert = select(field("column1"), field("column2"))
                .from(tableRef("BlobTable"))
                .where(or(
                        field("column1").eq(blobLiteral(BLOB1_VALUE.getBytes())),
                        field("column1").eq(blobLiteral(BLOB1_VALUE))
                ));
        // this update fails to work as an update without a WHERE clause - it strangely inserts a duplicate row on Postgres without a where clause
        UpdateStatement updateStatement = update(tableRef("BlobTable"))
                .set(blobLiteral(BLOB1_VALUE + " Updated").as("column1"), blobLiteral((BLOB2_VALUE + " Updated").getBytes()).as("column2"))
                .where(
                        field("column1").eq(blobLiteral((BLOB1_VALUE).getBytes()))
                );
        SelectStatement selectStatementAfterUpdate = select(field("column1"), field("column2"))
                .from(tableRef("BlobTable"))
                .where(or(
                        field("column1").eq(blobLiteral((BLOB1_VALUE + " Updated").getBytes())),
                        field("column1").eq(blobLiteral(BLOB1_VALUE + " Updated"))
                ));

        // Insert
        executor.execute(convertStatementToSQL(insertStatement, schema, null), connection);

        // Check result - note that this is deliberately not tidy - we are making sure that results get
        // passed back up to this scope correctly.
        String sql = convertStatementToSQL(selectStatementAfterInsert);
        AtomicBoolean isFirstValueHex = new AtomicBoolean(false);
        Integer numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
            @Override
            public Integer process(ResultSet resultSet) throws SQLException {
                int result = 0;
                while (resultSet.next()) {
                    result++;
                    byte[] bytesFromFirst = resultSet.getBytes("column1");

                    if (bytesFromFirst[1] == 32) { // if second char is a space then it isn't hex encoded
                      assertEquals("column1 blob value not correctly set/returned after insert", BLOB1_VALUE, new String(resultSet.getBytes(1)));
                      assertEquals("column2 blob value not correctly set/returned after insert", BLOB2_VALUE, new String(resultSet.getBytes(2)));
                    } else {
                      isFirstValueHex.set(true);
                      assertEquals("column1 blob value not correctly set/returned after insert", BLOB1_VALUE, decodeBlobHexFromBytesToText(resultSet.getBytes(1)));
                      assertEquals("column2 blob value not correctly set/returned after insert", BLOB2_VALUE, decodeBlobHexFromBytesToText(resultSet.getBytes(2)));
                    }
                }
                return result;
            }
        });
        if (isFirstValueHex.get()) {
          assertEquals("Should be exactly one record", 1, numberOfRecords.intValue());
        } else {
          assertEquals("Should be exactly two records", 2, numberOfRecords.intValue());
        }

        // Update
        executor.execute(ImmutableList.of(convertStatementToSQL(updateStatement)), connection);

        // Check result- note that this is deliberately not tidy - we are making sure that results get
        // passed back up to this scope correctly.
        sql = convertStatementToSQL(selectStatementAfterUpdate);
        AtomicBoolean isUpdateFirstValueHex = new AtomicBoolean(false);
        numberOfRecords = executor.executeQuery(sql, connection, new ResultSetProcessor<Integer>() {
            @Override
            public Integer process(ResultSet resultSet) throws SQLException {
                int result = 0;
                while (resultSet.next()) {
                    result++;
                  byte[] bytesFromFirst = resultSet.getBytes("column1");
                  if (bytesFromFirst[1] == 32) { // if second char is a space then it isn't hex encoded
                    assertEquals("column1 blob value not correctly set/returned after update", BLOB1_VALUE + " Updated", new String(resultSet.getBytes(1)));
                    assertEquals("column2 blob value not correctly set/returned after update", BLOB2_VALUE + " Updated", new String(resultSet.getBytes(2)));
                  } else {
                    isUpdateFirstValueHex.set(true);
                    assertEquals("column1 blob value not correctly set/returned after update", BLOB1_VALUE + " Updated", decodeBlobHexFromBytesToText(resultSet.getBytes(1)));
                    assertEquals("column2 blob value not correctly set/returned after update", BLOB2_VALUE + " Updated", decodeBlobHexFromBytesToText(resultSet.getBytes(2)));
                  }
                }
                return result;
            }
        });
        if (isUpdateFirstValueHex.get()) {
          assertEquals("Should be exactly one records", 1, numberOfRecords.intValue());
        } else {
          assertEquals("Should be exactly two records", 2, numberOfRecords.intValue());
        }
    }

    private static String decodeBlobHexFromBytesToText(byte[] bytSrc) throws SQLException {
        String blobStringResult;
        Hex hexUtil = new Hex();
        try {
            int lenSrc = bytSrc.length;
            char[] charBlob = new char[lenSrc];
            byte[] bytBlob = new byte[charBlob.length >> 1];
            for (int i = 0; i < bytSrc.length; i++) {
                charBlob[i] = (char) bytSrc[i];
            }
            hexUtil.decodeHex(charBlob, bytBlob, 0);

            blobStringResult = new String(bytBlob);
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
        return blobStringResult;
    }


    /**
   * Asserts that the number of records in the table are as expected.
   *
   * @param numberOfRecords The number of records expected.
   * @param tableName The table to check.
   */

  private void assertRecordsInTable(int numberOfRecords, String tableName) {
    String sql = convertStatementToSQL(select(count()).from(tableName));
    int actualRecordCount = sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        return resultSet.getInt(1);
      }
    });
    assertEquals(String.format("Should still have [%d] records in table [%s]", numberOfRecords, tableName), numberOfRecords, actualRecordCount);
  }


  /**
   * Test the behaviour of the Substring SQL function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void KtestSubstring() throws SQLException {
    SelectStatement testStatement1 = select(
      concat(
        substring(field("stringCol"), literal(2), literal(7)),
        literal("\\''\\'")
      )).from(tableRef("SimpleTypes"));
    String sql = convertStatementToSQL(testStatement1);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("ello wo\\''\\'", resultSet.getString(1));
        }
        return null;
      }
    });
  }


  /**
   * Test the behaviour of the {@link org.alfasoftware.morf.sql.SqlUtils#isEmpty(AliasedField)}
   * and {@link org.alfasoftware.morf.sql.SqlUtils#isNotEmpty(AliasedField)} SQL criteria against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void LtestIsEmpty() throws SQLException {
    SelectStatement testStatement1 = select(
        // isEmpty positives
        caseStatement(when(isEmpty(nullLiteral())).then(1)).otherwise(0),
        caseStatement(when(isEmpty(literal(""))).then(1)).otherwise(0),
        caseStatement(when(isEmpty(literal(" "))).then(1)).otherwise(0),
        caseStatement(when(isEmpty(literal("  "))).then(1)).otherwise(0),
        // isNotEmpty negatives
        caseStatement(when(not(isNotEmpty(nullLiteral()))).then(1)).otherwise(0),
        caseStatement(when(not(isNotEmpty(literal("")))).then(1)).otherwise(0),
        caseStatement(when(not(isNotEmpty(literal(" ")))).then(1)).otherwise(0),
        caseStatement(when(not(isNotEmpty(literal("  ")))).then(1)).otherwise(0),
        // isEmpty negatives
        caseStatement(when(not(isEmpty(literal("a")))).then(1)).otherwise(0),
        caseStatement(when(not(isEmpty(literal(" a ")))).then(1)).otherwise(0),
        caseStatement(when(not(isEmpty(literal("?")))).then(1)).otherwise(0),
        caseStatement(when(not(isEmpty(literal("\t")))).then(1)).otherwise(0),  // note the tab is not a space!
        caseStatement(when(not(isEmpty(literal("\n")))).then(1)).otherwise(0),  // note the newline is not a space!
        // isNotEmpty positives
        caseStatement(when(isNotEmpty(literal("a"))).then(1)).otherwise(0),
        caseStatement(when(isNotEmpty(literal(" a "))).then(1)).otherwise(0),
        caseStatement(when(isNotEmpty(literal("?"))).then(1)).otherwise(0),
        caseStatement(when(isNotEmpty(literal("\t"))).then(1)).otherwise(0),   // note the tab is not a space!
        caseStatement(when(isNotEmpty(literal("\n"))).then(1)).otherwise(0),   // note the newline is not a space!
        // making sure we check all fields
        literal(7)
      );

    String sql = convertStatementToSQL(testStatement1);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          final int answers = 19;
          assertEquals(answers, resultSet.getMetaData().getColumnCount());
          for (int i = 1; i < answers; i++) {
            assertEquals("Answer " + i + " not as expected", 1, resultSet.getInt(i));
          }
          assertEquals("Last answer not as expected", 7, resultSet.getInt(answers));
        }
        return null;
      }
    });
  }


  /**
   * Test the behaviour of the trim SQL functions against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void LtestTrimSpaces() throws SQLException {
    SelectStatement testStatement1 = select(
                                       leftTrim(field("stringColumn")),
                                       rightTrim(field("stringColumn")),
                                       rightTrim(leftTrim(field("stringColumn"))),
                                       trim(field("stringColumn"))
                                     )
                                     .from(tableRef("LeftAndRightTrimTable"))
                                     .orderBy(field("indexColumn"));

    String sql = convertStatementToSQL(testStatement1);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedStringL = ImmutableList.of("hello world", "test string     ", "purple flowers     ", "pancakes");
        List<String> expectedStringR = ImmutableList.of("hello world", "test string", "     purple flowers", "     pancakes");
        List<String> expectedStringLR = ImmutableList.of("hello world", "test string", "purple flowers", "pancakes");
        int counter = 0;
        while (resultSet.next()) {
          assertEquals(expectedStringL.get(counter), resultSet.getString(1));
          assertEquals(expectedStringR.get(counter), resultSet.getString(2));
          assertEquals(expectedStringLR.get(counter), resultSet.getString(3));
          assertEquals(expectedStringLR.get(counter), resultSet.getString(4));
          counter++;
        }
        return null;
      }
    });
  }


  /**
   * Tests the behaviour of Left_pad function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testLeftPadding() throws SQLException {
    SelectStatement leftPadStat = select( leftPad(field("invoiceNumber"), literal(15), literal("j"))).from(tableRef("LeftPaddingTable")).orderBy(field("id"));

    String sql = convertStatementToSQL(leftPadStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResult = ImmutableList.of("jjjjjInvoice100", "BigInvoiceNumbe", "ExactFifteeeeen");
        //List<String> expectedResult = ImmutableList.of("jjjjjInvoice100", "BigInvoiceNumbe", "ExactFifteeeeen");
        int count = 0;
        while (resultSet.next()) {
          assertEquals(expectedResult.get(count), resultSet.getString(1));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the behaviour of Left_pad function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testLeftPaddingConvenientMethod() throws SQLException {
    SelectStatement leftPadStat = select( leftPad(field("invoiceNumber"), 15, "j")).from(tableRef("LeftPaddingTable")).orderBy(field("id"));

    String sql = convertStatementToSQL(leftPadStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResult = ImmutableList.of("jjjjjInvoice100", "BigInvoiceNumbe", "ExactFifteeeeen");
        int count = 0;
        while (resultSet.next()) {
          assertEquals(expectedResult.get(count), resultSet.getString(1));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the behaviour of right_pad function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testRightPadding() throws SQLException {
    SelectStatement rightPadStat = select( rightPad(field("invoiceNumber"), literal(15), literal("j"))).from(tableRef("RightPaddingTable")).orderBy(field("id"));

    String sql = convertStatementToSQL(rightPadStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResult = ImmutableList.of("Invoice100jjjjj", "BigInvoiceNumbe", "ExactFifteeeeen");
        int count = 0;
        while (resultSet.next()) {
          assertEquals(expectedResult.get(count), resultSet.getString(1));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the behaviour of Left_pad function against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testrightPaddingConvenientMethod() throws SQLException {
    SelectStatement leftPadStat = select( rightPad(field("invoiceNumber"), 15, "j")).from(tableRef("RightPaddingTable")).orderBy(field("id"));

    String sql = convertStatementToSQL(leftPadStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResult = ImmutableList.of("Invoice100jjjjj", "BigInvoiceNumbe", "ExactFifteeeeen");
        int count = 0;
        while (resultSet.next()) {
          assertEquals(expectedResult.get(count), resultSet.getString(1));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the select order by statement (with nulls last) against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testSelectOrderByNullsFirstAscNullsFirstDesc() throws SQLException {
    SelectStatement selectOrderByNullsLastStat = select( field("field1"), field("field2")).from(tableRef("OrderByNullsLastTable")).orderBy(field("field1").nullsFirst(),field("field2").desc().nullsFirst());

    String sql = convertStatementToSQL(selectOrderByNullsLastStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResultField1 = Lists.newArrayList(null,null,"1", "1","3","3");
        List<String> expectedResultField2 = Lists.newArrayList(null,"3", null,"2","4","3");

        int count = 0;
        while (resultSet.next()) {
          assertEquals("count:"+count,expectedResultField1.get(count), resultSet.getString(1));
          assertEquals("count:"+count,expectedResultField2.get(count), resultSet.getString(2));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the select order by statement (with nulls last) against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testSelectOrderByNullsLastAscNullsLastAsc() throws SQLException {
    SelectStatement selectOrderByNullsLastStat = select( field("field1"), field("field2")).from(tableRef("OrderByNullsLastTable")).orderBy(field("field1").nullsLast(),field("field2").nullsLast());

    String sql = convertStatementToSQL(selectOrderByNullsLastStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResultField1 = Lists.newArrayList("1","1","3","3",null,null);
        List<String> expectedResultField2 = Lists.newArrayList("2",null,"3","4","3",null);

        int count = 0;
        while (resultSet.next()) {
          assertEquals("count:"+count,expectedResultField1.get(count), resultSet.getString(1));
          assertEquals("count:"+count,expectedResultField2.get(count), resultSet.getString(2));
          count++;
        }
        return null;
      };
    });
  }


  /**
   * Tests the select order by statement (with nulls last) against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testSelectFirstOrderByNullsLastAscNullsLastAsc() throws SQLException {
    SelectFirstStatement selectOrderByNullsLastStat = selectFirst( field("field1")).from(tableRef("OrderByNullsLastTable")).orderBy(field("field1").nullsLast(),field("field2").nullsLast());

    String sql = convertStatementToSQL(selectOrderByNullsLastStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        String expectedResultField1 = "1";
        assertTrue(resultSet.next());
        assertEquals(expectedResultField1, resultSet.getString(1));
        assertFalse(resultSet.next());
        return null;
      };
    });
  }


  /**
   * Tests the select order by statement (with nulls last) against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testSelectFirstFromJoin() throws SQLException {

    TableReference selectFirstTable = tableRef("SelectFirstTable");
    TableReference orderbyNulls = tableRef("OrderByNullsLastTable");
    SelectFirstStatement selectOrderByNullsLastStat = selectFirst(orderbyNulls.field("field1"))
        .from(orderbyNulls)
        .innerJoin(selectFirstTable,eq(selectFirstTable.field("field2"),orderbyNulls.field("field2")))
        .orderBy(selectFirstTable.field("field1").desc().nullsLast());

    String sql = convertStatementToSQL(selectOrderByNullsLastStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {

        assertTrue(resultSet.next());
        assertEquals("3", resultSet.getString(1));
        assertFalse(resultSet.next());

        return null;
      };
    });
  }


  /**
   * Tests the select order by statement (with nulls last) against all {@linkplain SqlDialect}s
   *
   * @throws SQLException in case of error.
   */
  @Test
  public void testSelectFirstOrderByNullsLastGetUndocumentedResult() throws SQLException {
    SelectFirstStatement selectOrderByNullsLastStat = selectFirst( field("field2")).from(tableRef("OrderByNullsLastTable")).orderBy(field("field1").desc().nullsLast());

    String sql = convertStatementToSQL(selectOrderByNullsLastStat);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {

      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedResultField2 = Lists.newArrayList("3","4");
        assertTrue(resultSet.next());
        assertTrue(expectedResultField2.contains(resultSet.getString(1)));
        assertFalse(resultSet.next());
        return null;
      };
    });
  }


  /**
   * Test the behaviour of the addDays SQL functions against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void MtestAddDays() throws SQLException {
    SelectStatement testStatement1 = select(
                                       addDays(field("actualDate"), literal(-1)),
                                       addDays(field("actualDate"), literal(1)),
                                       addDays(field("actualDate"), literal(0)),
                                       addDays(field("actualDate"), literal(365)),
                                       addDays(field("actualDate"), field("column3")),
                                       addDays(field("actualDate"), literal(0).minus(field("column3")))
                                     )
                                     .from(tableRef("ActualDates"))
                                     .innerJoin(tableRef("CoalesceTable"), eq(field("column3"), literal(5)))
                                     .orderBy(field("actualDate"));

    String sql = convertStatementToSQL(testStatement1);

    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<List<java.sql.Date>> expected = ImmutableList.of(
          (List<java.sql.Date>)ImmutableList.of(
            java.sql.Date.valueOf("1898-12-31"),
            java.sql.Date.valueOf("1899-01-02"),
            java.sql.Date.valueOf("1899-01-01"),
            java.sql.Date.valueOf("1900-01-01"),
            java.sql.Date.valueOf("1899-01-06"),
            java.sql.Date.valueOf("1898-12-27")
          ),
          ImmutableList.of(
            java.sql.Date.valueOf("1995-10-22"),
            java.sql.Date.valueOf("1995-10-24"),
            java.sql.Date.valueOf("1995-10-23"),
            java.sql.Date.valueOf("1996-10-22"),
            java.sql.Date.valueOf("1995-10-28"),
            java.sql.Date.valueOf("1995-10-18")
          ),
          ImmutableList.of(
            java.sql.Date.valueOf("2020-01-30"),
            java.sql.Date.valueOf("2020-02-01"),
            java.sql.Date.valueOf("2020-01-31"),
            java.sql.Date.valueOf("2021-01-30"),
            java.sql.Date.valueOf("2020-02-05"),
            java.sql.Date.valueOf("2020-01-26")
          ),
          ImmutableList.of(
            java.sql.Date.valueOf("2100-12-30"),
            java.sql.Date.valueOf("2101-01-01"),
            java.sql.Date.valueOf("2100-12-31"),
            java.sql.Date.valueOf("2101-12-31"),
            java.sql.Date.valueOf("2101-01-05"),
            java.sql.Date.valueOf("2100-12-26")
          )
        );
        for (int counter = 0; resultSet.next(); counter++) {
          assertEquals(expected.get(counter).size(), resultSet.getMetaData().getColumnCount());
          for (int i = 0; i < expected.get(counter).size(); i++) {
            assertEquals(expected.get(counter).get(i), resultSet.getDate(i + 1));
          }
        }
        return null;
      }
    });
  }


  /**
   * Ensure that we can merge into a table with all primary key fields. This will only insert and will not do any updates.
   */
  @Test
  public void NtestMergeWithAllPrimaryKeys()  {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement testSelectForInsert = select(field("key1"), field("key2"))
                                            .from(tableRef("MergeAllKeys"))
                                            .where(
                                              and(
                                                eq(field("key1"), literal(100)),
                                                eq(field("key2"), literal(200))));


    TableReference mergeSourceMultipleKeys = tableRef("MergeSourceMultipleKeys");

    SelectStatement sourceStmt = select(mergeSourceMultipleKeys.field("columnA").as("key1"),
                                        mergeSourceMultipleKeys.field("columnB").as("key2"))
                                 .from(mergeSourceMultipleKeys)
                                 .alias("xxx");

    TableReference mergeTableMultipleKeys = tableRef("MergeAllKeys");

    MergeStatement mergeStmt = merge()
                                .into(mergeTableMultipleKeys)
                                .tableUniqueKey(mergeTableMultipleKeys.field("key1"),
                                                mergeTableMultipleKeys.field("key2"))
                                .from(sourceStmt);

    executor.execute(ImmutableList.of(convertStatementToSQL(mergeStmt)), connection);

    // Check result for inserted
    String sqlForInsertedRecord = convertStatementToSQL(testSelectForInsert);

    Integer numberOfInsertedRecords = executor.executeQuery(sqlForInsertedRecord, connection, new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        int result = 0;
        while (resultSet.next()) {
          result++;
          assertEquals("column1 value not correctly set/returned after merge", 100, resultSet.getInt(1));
          assertEquals("column2 value value not correctly set/returned after merge", 200, resultSet.getInt(2));
        }
        return result;
      }
    });
    assertEquals("Should be exactly one records", 1, numberOfInsertedRecords.intValue());
  }


  /**
   * Test execution of the random function
   *
   * @throws SQLException
   */
  @Test
  public void testRandom() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("stringCol"), random().as("rnd"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("rnd")));
    executor.execute(ImmutableList.of(sql), connection);
  }


  /**
   * Test behaviour of the power function
   *
   * @throws SQLException
   */
  @Test
  public void testPower() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(power(literal(10),literal(3)).as("powerResult"))
                                                                        .from(tableRef("SimpleTypes")));
    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals(1000, resultSet.getInt(1));
        }
        return null;
      }
    });
  }


  /**
   * Test execution of the random string function
   *
   * @throws SQLException
   */
  @Test
  public void testRandomString() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(randomString(literal(10)).as("rnd"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("rnd")));
    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertNotNull(resultSet.getString(1));
          assertEquals(10, resultSet.getString(1).length());
        }
        return null;
      }
    });
  }


  /**
   * Test execution of the LIKE operator
   *
   * @throws SQLException
   */
  @Test
  public void testLike() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("column1"))
                                                                        .from(tableRef("LikeTest"))
                                                                        .where(
                                                                          or(
                                                                            like(field("column1"), "1%"),
                                                                            like(field("column1"), "%2"),
                                                                            like(field("column1"), "%3%"),
                                                                            like(field("column1"), "4%5"),
                                                                            like(field("column1"), "%*%"),
                                                                            like(field("column1"), "xxx\\%xxx"),
                                                                            like(field("column1"), "1%%"),
                                                                            like(field("column1"), "%%2"),
                                                                            like(field("column1"), "%%3%%"),
                                                                            like(field("column1"), "4%%5"),
                                                                            like(field("column1"), "%%*%%"),
                                                                            like(field("column1"), "xxx\\%xxx")
                                                                          )

                                                                        ));

    final Set<String> results = executor.executeQuery(sql, connection, new ResultSetProcessor<Set<String>>(){
      @Override
      public Set<String> process(ResultSet resultSet) throws SQLException {
        Set<String> res = Sets.newHashSet();
        while (resultSet.next()) {
          res.add(resultSet.getString(1));
        }
        return res;
      }
    });

    assertEquals(
      ImmutableSet.of("1xxxxxx", "xxxxxx2", "xxx3xxx", "xxx*xxx", "xxx%xxx", "4xxxxx5"),
      results
    );
  }


  /**
   * Test behaviour of the floor function
   *
   * @throws SQLException
   */
  @Test
  public void testFloor() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("decimalNineFiveCol"), floor(field("decimalNineFiveCol")).as("floorResult"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("floorResult")));

    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals(278.231, resultSet.getDouble(1), 0D);
          assertEquals(278, resultSet.getInt(2));
        }
        return null;
      }

    });
  }


  /**
   * Test behaviour of the length function
   *
   * @throws SQLException
   */
  @Test
  public void testLength() throws SQLException {
    // Key value
    final String value = "hello world AA";

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("stringCol"), length(field("stringCol")).as("lengthResult"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("lengthResult")));

    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals(value, resultSet.getString(1));
          assertEquals(value.length(), resultSet.getInt(2));
        }
        return null;
      }

    });
  }


  /**
   * Test behaviour of the length-of-blob function
   *
   * @throws SQLException
   */
  @Test
  public void testBlobLength() throws SQLException {
    // Key value
    final byte[] value = "hello world BLOB".getBytes();

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("blobCol"), blobLength(field("blobCol")).as("lengthResult"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("lengthResult")));

    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertArrayEquals(value, resultSet.getBytes(1));
          assertEquals(value.length, resultSet.getInt(2));
        }
        return null;
      }

    });
  }


  /**
   * Test behaviour of the MOD function
   *
   * @throws SQLException
   */
  @Test
  public void testMod() throws SQLException {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());
    String sql = convertStatementToSQL(select(field("nullableBigIntegerCol"), mod(field("nullableBigIntegerCol"), literal(12)).as("modResult"))
                                                                        .from(tableRef("SimpleTypes"))
                                                                        .orderBy(field("modResult")));

    executor.executeQuery(sql, connection, new ResultSetProcessor<Void>(){
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals(56732, resultSet.getDouble(1), 0D);
          assertEquals(8, resultSet.getInt(2));
        }
        return null;
      }

    });
  }


  /**
   * Test the behaviour of the <code>LOWER</code> and <code>UPPER</code> SQL
   * functions against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testLowerAndUpper() throws SQLException {
    SelectStatement statement = select(lowerCase(field("firstName")), upperCase(field("lastName"))).from(
      tableRef("LowerAndUpperTable")).orderBy(field("id"));
    String sql = convertStatementToSQL(statement);
    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<String> expectedFirstName = ImmutableList.of("ludwig van", "wolfgang amadeus", "johann sebastian");
        List<String> expectedLastName = ImmutableList.of("BEETHOVEN", "MOZART", "BACH");
        int counter = 0;
        while (resultSet.next()) {
          assertEquals(expectedFirstName.get(counter), resultSet.getString(1));
          assertEquals(expectedLastName.get(counter), resultSet.getString(2));
          counter++;
        }
        return null;
      }
    });
  }


  /**
   * Test the behaviour of the DateToYyyymmdd function. The function should
   * provide SQL to convert an SQL date to an 8 digit ALFA date
   */
  @Test
  public void testDateToYyyymmdd() {
    SelectStatement statement = select(dateToYyyymmdd(field("actualDate"))).from(tableRef("ActualDates")).orderBy(field("actualDate"));

    String sql = convertStatementToSQL(statement);
    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        List<Integer> expectedAlfaDates = ImmutableList.of(18990101, 19951023, 20200131, 21001231);
        int counter = 0;
        while (resultSet.next()) {
          assertThat(resultSet.getObject(1), instanceOf(Number.class));
          assertEquals(expectedAlfaDates.get(counter).intValue(), resultSet.getInt(1));
          assertEquals(expectedAlfaDates.get(counter).longValue(), resultSet.getLong(1));
          assertEquals(BigDecimal.valueOf(expectedAlfaDates.get(counter).longValue()), resultSet.getBigDecimal(1));
          counter++;
        }
        assertEquals(expectedAlfaDates.size(), counter);
        return null;
      }
    });
  }


  /**
   * Test the behaviour of the DateToYyyymmddHHmmss function. The function should
   * provide SQL to convert an SQL date to an 14 digit ALFA date
   */
  @Test
  public void testDateToYyyymmddHHmmss() {
    SelectStatement statement = select(dateToYyyyMMddHHmmss(now()), dateToYyyymmdd(now()), dateToYyyymmdd(addDays(now(), literal(1))));

    String sql = convertStatementToSQL(statement);
    sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        assertThat(resultSet.getObject(1), instanceOf(Number.class));
        assertThat(resultSet.getLong(1), allOf(greaterThanOrEqualTo(resultSet.getLong(2) * 1_000_000), lessThanOrEqualTo(resultSet.getLong(3) * 1_000_000)));
        assertThat(resultSet.getBigDecimal(1), allOf(greaterThanOrEqualTo(resultSet.getBigDecimal(2).multiply(BigDecimal.valueOf(1_000_000))), lessThanOrEqualTo(resultSet.getBigDecimal(3).multiply(BigDecimal.valueOf(1_000_000)))));

        try {
          SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMddhhmmss");
          Date dateTimeInstance = dateTimeFormatter.parse(String.valueOf(resultSet.getLong(1)));
          assertNotNull("Invalid numeric date time", dateTimeInstance);
        } catch (ParseException e) {
          throw new RuntimeException("Invalid numeric date time", e);
        }
        return null;
      }
    });
  }


  /**
   * Test the behaviour of the <code>concat</code> SQL
   * functions against all {@linkplain SqlDialect}s
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testConcatWithNulls() {
    InsertStatement insertStatement = insert().into(tableRef("AutoNumbered")).fields(field("column2")).values(concat(literal("A"), nullLiteral(), literal("B")).as("column2"));
    sqlScriptExecutorProvider.get().execute(
        convertStatementToSQL(insertStatement));
    SelectStatement select = select(field("surrogateKey"), field("column2"))
        .from(tableRef("AutoNumbered"))
        .orderBy(field("surrogateKey"));
    sqlScriptExecutorProvider.get().executeQuery(convertStatementToSQL(select), new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        int counter = 0;
        while (resultSet.next()) {
          switch (counter) {
            case 2:
              assertEquals("Inserted record 1 - string", "AB", resultSet.getString(2));
              break;
            default:
              if (counter > 2)
                fail("More records returned than expected");
          }
          counter++;
        }
        return null;
      }
    });
  }


  /**
   * Tests that we can use merges with prepared statements.
   *
   * @throws SQLException
   */
  @Test
  public void testParameterisedMerge() throws SQLException {
    SqlDialect sqlDialect = connectionResources.sqlDialect();
    MergeStatement merge = merge()
        .into(tableRef("MergeTableMultipleKeys"))
        .tableUniqueKey(field("column1"), field("column2"))
        .from(
          select(
            parameter("column1").type(DataType.INTEGER),
            parameter("column2").type(DataType.DECIMAL),
            parameter("column3").type(DataType.STRING).width(0),
            parameter("parameterValue").type(DataType.STRING).as("column4")
          )
        );
    NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parseSql(sqlDialect.convertStatementToSQL(merge), sqlDialect).createFor(connection);
    try {

      // Put in two records.  The first should merge with the initial data set.
      preparedStatementRecord(sqlDialect, preparedStatement, 500, 800, "Correct", "Updated");
      preparedStatementRecord(sqlDialect, preparedStatement, 101, 201, "301", "401");
      if (sqlDialect.useInsertBatching()) {
        preparedStatement.executeBatch();
      }

      // Check we have what we expect
      SelectStatement statement = select(field("column1"), field("column2"), field("column3"), field("column4")).from(tableRef("MergeTableMultipleKeys")).orderBy(field("autoNum"));
      String sql = convertStatementToSQL(statement);
      sqlScriptExecutorProvider.get().executeQuery(sql, connection, new ResultSetProcessor<Void>() {
        @Override
        public Void process(ResultSet resultSet) throws SQLException {
          assertTrue("No record 1", resultSet.next());
          assertEquals("Row 1 column 1", 500, resultSet.getInt(1));
          assertEquals("Row 1 column 2", 800, resultSet.getInt(2));
          assertEquals("Row 1 column 3", "Correct", resultSet.getString(3));
          assertEquals("Row 1 column 4", "Updated", resultSet.getString(4));
          assertTrue("No record 2", resultSet.next());
          assertEquals("Row 2 column 1", 101, resultSet.getInt(1));
          assertEquals("Row 2 column 2", 201, resultSet.getInt(2));
          assertEquals("Row 2 column 3", "301", resultSet.getString(3));
          assertEquals("Row 2 column 4", "401", resultSet.getString(4));
          assertFalse("Noo many records", resultSet.next());
          return null;
        }
      });

    } finally {
      preparedStatement.close();
    }
  }


  /**
   * Tests that we can use updates with prepared statements.
   *
   * @throws SQLException
   */
  @Test
  public void testParameterisedUpdate() throws SQLException {
    SqlParameter column1 = parameter("column1").type(DataType.INTEGER);
    SqlParameter column2 = parameter("column2").type(DataType.DECIMAL);
    SqlParameter column3 = parameter("column3").type(DataType.STRING).width(0);
    AliasedField column4 = parameter("parameterValue").type(DataType.STRING).as("column4");

    SqlDialect sqlDialect = connectionResources.sqlDialect();
    UpdateStatement update = update(tableRef("MergeTableMultipleKeys"))
        .set(column2, column3, column4)
        .where(field("column1").eq(column1));
    ParseResult parsed = NamedParameterPreparedStatement.parseSql(sqlDialect.convertStatementToSQL(update), sqlDialect);

    NamedParameterPreparedStatement preparedStatement = parsed.createFor(connection);
    try {
      // Use method chaining syntax
      preparedStatement.setInt(column1, 500)
                       .setInt(column2, 801)
                       .setString(column3, "Correct")
                       .setString(parameter("parameterValue").type(DataType.STRING), "Updated")
                       .executeUpdate();

      // Check we have what we expect
      SelectStatement statement = select(field("column1"), field("column2"), field("column3"), field("column4")).from(tableRef("MergeTableMultipleKeys")).orderBy(field("autoNum"));
      String sql = convertStatementToSQL(statement);
      sqlScriptExecutorProvider.get().executeQuery(sql, connection, new ResultSetProcessor<Void>() {
        @Override
        public Void process(ResultSet resultSet) throws SQLException {
          assertTrue("No record 1", resultSet.next());
          assertEquals("Row 1 column 1", 500, resultSet.getInt(1));
          assertEquals("Row 1 column 2", 801, resultSet.getInt(2));
          assertEquals("Row 1 column 3", "Correct", resultSet.getString(3));
          assertEquals("Row 1 column 4", "Updated", resultSet.getString(4));
          assertFalse("Noo many records", resultSet.next());
          return null;
        }
      });
    } finally {
      preparedStatement.close();
    }
  }



  /**
   * Tests parameterised SELECT.
   */
  @Test
  public void testParameterisedSelect() throws SQLException {
    SqlDialect sqlDialect = connectionResources.sqlDialect();
    SelectStatement select = select(
          field("field1"),
          field("field2"),
          literal(":justtomesswithyou"), // just to confuse it - should be treated as a string
          parameter("param3").type(DataType.INTEGER).as("field3"),
          coalesce(literal("value"), parameter("param4").type(DataType.STRING)),
          isnull(literal("value"), parameter("param4").type(DataType.STRING))
        )
        .from("SelectFirstTable")
        .where(field("field1").in(
          parameter("param1").type(DataType.INTEGER).plus(parameter("param1").type(DataType.INTEGER)),
          parameter("param2").type(DataType.DECIMAL),
          parameter("param2").type(DataType.INTEGER)
        ))
        .orderBy(field("field1"), field("field2"));

    NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parseSql(sqlDialect.convertStatementToSQL(select), sqlDialect).createForQueryOn(connection);
    try {
      preparedStatement.setFetchSize(sqlDialect.fetchSizeForBulkSelects());
      sqlDialect.prepareStatementParameters(
        preparedStatement,
        ImmutableList.of(
          parameter("param1").type(DataType.INTEGER),
          parameter("param2").type(DataType.DECIMAL),
          parameter("param3").type(DataType.INTEGER),
          parameter("param4").type(DataType.STRING)
        ),
        DataSetUtils.statementParameters()
          .setInteger("param1", 1) // 1 + 1 = 2
          .setInteger("param2", 5)
          .setInteger("param3", 7)
          .setString("param4", "value4")
      );
      ResultSet resultSet = preparedStatement.executeQuery();
      assertTrue("No record 1", resultSet.next());
      assertEquals("Row 1 column 1", 2, resultSet.getInt(1));
      assertEquals("Row 1 column 2", 2, resultSet.getInt(2));
      assertEquals("Row 1 column 3", ":justtomesswithyou", resultSet.getString(3));
      assertEquals("Row 1 column 4", 7, resultSet.getInt(4));
      assertEquals("Row 1 column 5", "value", resultSet.getString(5));
      assertEquals("Row 1 column 6", "value", resultSet.getString(6));
      assertTrue("No record 2", resultSet.next());
      assertEquals("Row 2 column 1", 2, resultSet.getInt(1));
      assertEquals("Row 2 column 2", 3, resultSet.getInt(2));
      assertEquals("Row 2 column 3", ":justtomesswithyou", resultSet.getString(3));
      assertEquals("Row 2 column 4", 7, resultSet.getInt(4));
      assertEquals("Row 2 column 5", "value", resultSet.getString(5));
      assertEquals("Row 2 column 6", "value", resultSet.getString(6));
      assertTrue("No record 3", resultSet.next());
      assertEquals("Row 3 column 1", 5, resultSet.getInt(1));
      assertEquals("Row 3 column 2", 4, resultSet.getInt(2));
      assertEquals("Row 3 column 3", ":justtomesswithyou", resultSet.getString(3));
      assertEquals("Row 3 column 4", 7, resultSet.getInt(4));
      assertEquals("Row 3 column 5", "value", resultSet.getString(5));
      assertEquals("Row 3 column 6", "value", resultSet.getString(6));
      assertFalse("Noo many records", resultSet.next());
    } finally {
      preparedStatement.close();
    }
  }


  private void preparedStatementRecord(SqlDialect sqlDialect, NamedParameterPreparedStatement preparedStatement, Integer col1Value, Integer col2Value, String col3Value, String col4Value) throws SQLException {
    sqlDialect.prepareStatementParameters(
      preparedStatement,
      ImmutableList.of(
        parameter("column1").type(DataType.INTEGER),
        parameter("column2").type(DataType.DECIMAL),
        parameter("column3").type(DataType.STRING),
        parameter("parameterValue").type(DataType.STRING)
      ),
      DataSetUtils.statementParameters()
        .setInteger("column1", col1Value)
        .setInteger("column2", col2Value)
        .setString("column3", col3Value)
        .setString("parameterValue", col4Value)
    );
    if (sqlDialect.useInsertBatching()) {
      preparedStatement.addBatch();
    } else {
      preparedStatement.executeUpdate();
    }
  }


  /**
   * Checks if parametrised query execution is working correctly.
   */
  @Test
  public void shouldExecuteParametrisedQuery()  {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    SelectStatement testSelect = select(field("alfaDate1"), field("alfaDate2"), literal(123))
                                 .from(tableRef("DateTable")).where(eq(field("alfaDate1"), parameter("firstDateParam").type(DataType.BIG_INTEGER)));
    Iterable<SqlParameter> parameterMetadata = ImmutableList.of(parameter(column("firstDateParam", DataType.DECIMAL)));
    RecordBuilder parameterData = DataSetUtils.record().setLong("firstDateParam", 20040609L);
    ResultSetProcessor<List<List<String>>> resultSetProcessor = new ResultSetProcessor<>() {
      /**
       * Takes all rows and puts into two-dimension String array.
       */
      @Override
      public List<List<String>> process(ResultSet resultSet) throws SQLException {
        Builder<List<String>> builder = ImmutableList.<List<String>>builder();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
          List<String> rowBuilder = new LinkedList<>();
          for (int columnNumber = 1; columnNumber < columnCount + 1; columnNumber++) {
            String stringifiezedCell = resultSet.getString(columnNumber);
            rowBuilder.add(stringifiezedCell);
          }
          builder.add(rowBuilder);
        }
        return builder.build();
      }
    };
    List<List<String>> result = executor.executeQuery(testSelect, parameterMetadata, parameterData, connection, resultSetProcessor);

    assertEquals(ImmutableList.of(ImmutableList.of("20040609","20040813", "123"), ImmutableList.of("20040609","20040609", "123") , ImmutableList.of("20040609","20040610", "123")), result);
  }


  @Test
  public void testSingleLineComment() {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    // This should work without failing
    int rowsAffected = executor.execute(connectionResources.sqlDialect().convertCommentToSQL("hello world!"));
    assertEquals(0, rowsAffected);
  }


  @Test
  public void testSingleLineCommentFollowedByStatement() {
    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    InsertStatement insertStatement = insert().into(tableRef("ParameterTable")).values(literal("foo").as("parameterCode"), literal(1).as("parameterValue"));

    String sql = connectionResources.sqlDialect().convertCommentToSQL("hello world!") + "\n" +
                  convertStatementToSQL(insertStatement).get(0);

    int rowsAffected = executor.execute(sql);
    assertEquals(1, rowsAffected);

    int rows = executor.executeQuery(convertStatementToSQL(select(count()).from(tableRef("ParameterTable"))), new ResultSetProcessor<Integer>() {
      @Override
      public Integer process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        return resultSet.getInt(1);
      }
    });

    assertEquals("expect 1 from fixture and one from test", 2, rows);
  }


  /**
   * Tests execute now function.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testNow() throws SQLException {
    SelectStatement select = select(now());

    SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    executor.executeQuery(convertStatementToSQL(select), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        resultSet.next();

        final long maximumDifferenceMillis = 2000;
        final Instant currentSystemTime = Clock.systemUTC().instant();
        final Instant databaseTime = resultSet.getTimestamp(1).toInstant();
        final long differenceMillis = Duration.between(currentSystemTime, databaseTime).abs().toMillis();

        log.info("Current system time: " + currentSystemTime + ". Current database time: " + databaseTime);

        //Assert that the time of the database and system are accurate within 2 s.
        assertTrue("Database and system times don't match to within 2 s, the difference is " + differenceMillis + " ms. "
            + "This could be because of different timezones.", differenceMillis <= maximumDifferenceMillis);
        assertFalse("More than one record", resultSet.next());
        return null;
      }
    });
  }


  @Test
  public void testExecuteSqlStatementWithParams() {
    InsertStatement insert = insert().into(tableRef("ParamStatementsTest")).values(
      parameter("one").type(DataType.INTEGER),
      parameter("two").type(DataType.STRING).width(10));

    String insertStatement = convertStatementToSQL(insert).get(0);
    List<SqlParameter> insertStatementParams = connectionResources.sqlDialect().extractParameters(insert);

    assertEquals(1, sqlScriptExecutorProvider.get().execute(
      insertStatement, connection,
      insertStatementParams,
      DataSetUtils.record().setInteger("one", 1)
          .setString("two", "two")));

    SelectStatement select = select(
      count().as("a"),
      count().as("b").negated()
    ).from("ParamStatementsTest");

    sqlScriptExecutorProvider.get().executeQuery(select).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        assertEquals(2, resultSet.getInt(1));
        assertEquals(-2, resultSet.getInt(2));
        assertEquals("A", resultSet.getMetaData().getColumnLabel(1).toUpperCase());
        assertEquals("B", resultSet.getMetaData().getColumnLabel(2).toUpperCase());
        return null;
      }
    });
  }



  /**
   * Tests that we enforce the maximum number of rows on a query.
   *
   * @throws SQLException
   */
  @Test
  public void testMaxRows() throws SQLException {
    SelectStatement select =
        select(field("parameterCode"), field("parameterValue"))
        .from("ParameterTable")
        .where(like(field("parameterCode"), "KEY%"))
        .orderBy(field("parameterValue"));
    InsertStatement insert = insert().into(tableRef("ParameterTable")).values(
      parameter("parameterCode").type(DataType.STRING).width(10),
      parameter("parameterValue").type(DataType.INTEGER)
    );
    String sql = convertStatementToSQL(insert).get(0);
    List<SqlParameter> params = connectionResources.sqlDialect().extractParameters(insert);

    for (int i = 0 ; i < 20 ; i++) {
      sqlScriptExecutorProvider.get().execute(sql, connection, params,
        record().setString("parameterCode", "KEY" + i)
                .setInteger("parameterValue", i)
      );
    }
    checkMaxRows(select, 10);
    checkMaxRows(select, 15);
  }


  /**
   *  Tests that the correct behaviour occurs when using the count function
   */
  @Test
  public void testCount() {
    SelectStatement selectCount =
        select(
          count().as("rowCount"),
          count(field("decimalColumn")).as("valueCount"),
          countDistinct(field("integerColumn")).as("distinctCount"))
        .from("NumericTable");

    sqlScriptExecutorProvider.get().executeQuery(selectCount).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("Row count returned should be", 7, resultSet.getInt(1));
          assertEquals("Value count returned should be", 6, resultSet.getInt(2));
          assertEquals("Distinct count returned should be", 5, resultSet.getInt(3));
        }
        return null;
      }

    });
  }


  /**
   *  Tests that the correct behaviour occurs when using the average function
   */
  @Test
  public void testAverage() {
    SelectStatement selectAverage =
        select(
          average(field("decimalColumn")).as("decimalAverage"),
          average(field("integerColumn")).as("integerAverage"),
          averageDistinct(field("decimalColumn")).as("decimalDistinctAverage"),
          averageDistinct(field("integerColumn")).as("integerDistinctAverage"))
        .from("NumericTable");

    sqlScriptExecutorProvider.get().executeQuery(selectAverage).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("Decimal average returned should be", 189949710.968, resultSet.getDouble(1), 0.005);
          assertEquals("Integer average returned should be", 503846, resultSet.getInt(2));
          assertEquals("Decimal distinct average returned should be", 227938805.676, resultSet.getDouble(3), 0.005);
          assertEquals("Integer distinct average returned should be", 562189, resultSet.getInt(4));
        }
        return null;
      }

    });
  }


  /**
   *  Tests that the correct behaviour occurs when using the sum function
   */
  @Test
  public void testSum() {
    SelectStatement selectSum =
        select(
          sum(field("decimalColumn")).as("decimalSum"),
          sum(field("integerColumn")).as("integerSum"),
          sumDistinct(field("decimalColumn")).as("decimalDistinctSum"),
          sumDistinct(field("integerColumn")).as("integerDistinctSum"))
        .from("NumericTable");

    sqlScriptExecutorProvider.get().executeQuery(selectSum).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("Decimal sum returned should be", 1139698265.81, resultSet.getDouble(1), 0.005);
          assertEquals("Integer sum returned should be", 3023078, resultSet.getInt(2));
          assertEquals("Decimal distinct sum returned should be", 1139694028.38, resultSet.getDouble(3), 0.005);
          assertEquals("Integer distinct sum returned should be", 2810947, resultSet.getInt(4));
        }
        return null;
      }

    });
  }

  protected void checkMaxRows(SelectStatement select, final int maxRows) {
    sqlScriptExecutorProvider.get().executeQuery(select).withMaxRows(maxRows).processWith(new ResultSetProcessor<Void>() {
      private int index;
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          int value = resultSet.getInt(2);
          assertEquals("Invalid value or value out of sequence", index, value);
          assertTrue("Too many records returned", index < maxRows);
          index++;
        }
        return null;
      }
    });
  }


  /**
   * Testing that select (without distinct) returns a set of duplicate rows.
   */
  @Test
  public void testSelectWithoutDistinct() {
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference joinTable = tableRef("SelectDistinctJoinTable");

    SelectStatement selectStatement = select(selectTable
      .field("column1"), selectTable.field("column2"))
      .from(selectTable)
      .innerJoin(joinTable, eq(joinTable.field("foreignKeyId"), selectTable.field("id")))
      .where(eq(selectTable.field("column1"), literal("TEST1")));

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should have 3 duplicate records selected", 3, numberOfRecords.intValue());
  }


  /**
   * Testing that select for update returns the specified row (we assume the lock worked!)
   */
  @Test
  public void testSelectForUpdate() {
    TableReference selectTable = tableRef("SelectDistinctTable");

    SelectStatement selectStatement = select(selectTable.field("column1"))
      .from(selectTable)
      .where(eq(selectTable.field("column1"), literal("TEST1")))
      .forUpdate();

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should have 1 record", 1, numberOfRecords.intValue());
  }


  /**
   * Testing that select distinct returns a reduced result set of duplicate rows.
   */
  @Test
  public void testSelectDistinct() {
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference joinTable = tableRef("SelectDistinctJoinTable");

    SelectStatement selectStatement = selectDistinct(selectTable
      .field("column1"), selectTable.field("column2"))
      .from(selectTable)
      .innerJoin(joinTable, eq(joinTable.field("foreignKeyId"), selectTable.field("id")))
      .where(eq(selectTable.field("column1"), literal("TEST1")));

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should only have 1 record selected", 1, numberOfRecords.intValue());
  }


  /**
   * Testing merging into a table on select distinct does not violate the composite primary key constraint.
   *
   * <p>
   *   <em>Note:</em> Cannot test merge on select without distinct due to some database dialects (e.g. MySQL) handling duplicates without the need for distinct
   * </p>
   */
  @Test
  public void testMergeOnSelectDistinct() {
    TableReference mergeTable = tableRef("MergeSelectDistinctTable");
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference joinTable = tableRef("SelectDistinctJoinTable");

    MergeStatement mergeStatement = merge().into(mergeTable)
        .tableUniqueKey(mergeTable.field("column1"), mergeTable.field("column2"))
        .from(
          selectDistinct(selectTable.field("column1"), selectTable.field("column2"))
          .from(selectTable)
          .innerJoin(joinTable, eq(joinTable.field("foreignKeyId"), selectTable.field("id"))));

    Integer initialNumberOfRecords = getNumberOfRecordsInMergedTable(mergeTable);

    sqlScriptExecutorProvider.get().execute(connectionResources
      .sqlDialect().convertStatementToSQL(mergeStatement));

    Integer finalNumberOfRecords = getNumberOfRecordsInMergedTable(mergeTable);

    assertEquals("Merged table should have 2 additional records now", 2, finalNumberOfRecords - initialNumberOfRecords);
  }


  /**
   * Testing insert into a table on select distinct does not violate the composite primary key constraint.
   */
  @Test
  public void testInsertOnSelectDistinct() {
    TableReference insertTable = tableRef("InsertSelectDistinctTable");
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference joinTable = tableRef("SelectDistinctJoinTable");

    InsertStatement insertStatement = insert().into(insertTable)
        .from(
          selectDistinct(ImmutableList.of(selectTable.field("column1"), selectTable.field("column2")))
          .from(selectTable)
          .innerJoin(joinTable, eq(joinTable.field("foreignKeyId"), selectTable.field("id"))));

    Integer initialNumberOfRecords = getNumberOfRecordsInMergedTable(insertTable);

    sqlScriptExecutorProvider.get().execute(connectionResources
      .sqlDialect().convertStatementToSQL(insertStatement));

    Integer finalNumberOfRecords = getNumberOfRecordsInMergedTable(insertTable);

    assertEquals("Insert table should have 2 additional records now", 2, finalNumberOfRecords - initialNumberOfRecords);
  }


  /**
   * Testing insert into a table on a select with a subquery select distinct in a where clause
   * (i.e.
   *       INSERT INTO InsertSelectDistinctTable
   *          SELECT column1, column2 FROM SelectDistinctTable
   *          WHERE SelectDistinctTable.column2 IN
   *           (SELECT DISTINCT foreignKeyId FROM SelectDistinctJoinTable)
   *  )
   * does not violate the composite primary key constraint.
   */
  @Test
  public void testInsertOnSelectWithASubQuerySelectDistinct() {
    TableReference insertTable = tableRef("InsertSelectDistinctTable");
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference whereTable = tableRef("SelectDistinctJoinTable");

    InsertStatement insertStatement = insert().into(insertTable)
        .from(
          select(selectTable.field("column1"), selectTable.field("column2"))
          .from(selectTable)
          .where(in(selectTable.field("column2"), selectDistinct(whereTable.field("foreignKeyId"))
            .from(whereTable))));

    Integer initialNumberOfRecords = getNumberOfRecordsInMergedTable(insertTable);

    sqlScriptExecutorProvider.get().execute(connectionResources
      .sqlDialect().convertStatementToSQL(insertStatement));

    Integer finalNumberOfRecords = getNumberOfRecordsInMergedTable(insertTable);

    assertEquals("Insert table should have 2 additional records now", 2, finalNumberOfRecords - initialNumberOfRecords);
  }


  /**
   * Testing insert into a table on select without using distinct violates the composite primary key constraint.
   */
  @Test
  public void testInsertOnSelectWithoutDistinct() {
    TableReference insertTable = tableRef("InsertSelectDistinctTable");
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference joinTable = tableRef("SelectDistinctJoinTable");

    InsertStatement insertStatement = insert().into(insertTable)
        .from(
          select(selectTable.field("column1"), selectTable.field("column2"))
          .from(selectTable)
          .innerJoin(joinTable, eq(joinTable.field("foreignKeyId"), selectTable.field("id"))));

    try {
      sqlScriptExecutorProvider.get().execute(connectionResources
        .sqlDialect().convertStatementToSQL(insertStatement));
      fail("Expected an Exception to be thrown");
    } catch (Exception e) {

    }
  }


  /**
   * Runs a select statement with all our SQL hint directives to make sure the query doesn't blow up.
   */
  @Test
  public void testSelectHints1() {
    TableReference selectTable = tableRef("SelectDistinctTable");

    SelectStatement selectStatement = select(selectTable.field("column1"), selectTable.field("column2"))
      .from(selectTable)
      .where(field("column1").eq("TEST1"))
      .useImplicitJoinOrder()
      .optimiseForRowCount(5)
      .useIndex(selectTable, "SelectDistinctTable_1")
      .forUpdate(); // To make sure SQL server in particular is happy with the relative positions
                    // of FOR UPDATE and OPTION in the query.

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should have 1 records selected", 1, numberOfRecords.intValue());
  }


  /**
   * We can't use joins with FOR UPDATE, so this tries join order hinting
   */
  @Test
  public void testSelectHints2() {
    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference selectTable2 = tableRef("SelectDistinctTable").as("abc");
    TableReference selectTable3 = tableRef("SelectDistinctTable").as("def");

    SelectStatement selectStatement = select(selectTable.field("column1"), selectTable.field("column2"))
      .from(selectTable)
      .innerJoin(selectTable2, selectTable.field("column1").eq(selectTable2.field("column1")))
      .leftOuterJoin(selectTable3, selectTable.field("column1").eq(selectTable3.field("column1")))
      .where(selectTable.field("column1").eq("TEST1"))
      .useImplicitJoinOrder()
      .optimiseForRowCount(5)
      .useIndex(selectTable, "SelectDistinctTable_1");

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should have 1 records selected", 1, numberOfRecords.intValue());
  }


  /**
   * We can't use joins with FOR UPDATE, so this tries join order hinting
   */
  @Test
  public void testSelectLeftJoinFullJoin() {
    Assume.assumeFalse("Not yet supported on H2", "H2".equals(connectionResources.getDatabaseType())); // https://github.com/h2database/h2database/issues/457
    Assume.assumeFalse("Not yet supported on MySQL", "MY_SQL".equals(connectionResources.getDatabaseType()));

    TableReference selectTable = tableRef("SelectDistinctTable");
    TableReference selectTable2 = tableRef("SelectDistinctTable").as("abc");
    TableReference selectTable3 = tableRef("SelectDistinctTable").as("def");
    TableReference selectTable4 = tableRef("SelectDistinctTable").as("ghi");

    SelectStatement selectStatement = select(selectTable.field("column1"), selectTable.field("column2"))
      .from(selectTable)
      .innerJoin(selectTable2, selectTable.field("column1").eq(selectTable2.field("column1")))
      .leftOuterJoin(selectTable3, selectTable.field("column1").eq(selectTable3.field("column1")))
      .fullOuterJoin(selectTable4, selectTable.field("column1").eq(selectTable4.field("column1")))
      .where(selectTable.field("column1").eq("TEST1"));

    Integer numberOfRecords = getNumberOfRecordsFromSelect(selectStatement);

    assertEquals("Should have 1 records selected", 1, numberOfRecords.intValue());
  }


  /**
   * Runs a insert... select statement with all our SQL hint directives to make sure the query doesn't blow up.
   */
  @Test
  public void testInsertFromSelectWithHints() {
    TableReference selectTable = tableRef("SelectDistinctTable").as("abc");
    TableReference insertTable = tableRef("InsertTargetTable");

    SqlScriptExecutor sqlScriptExecutor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    sqlScriptExecutor.execute(convertStatementToSQL(
      insert()
      .into(insertTable)
      .from(
        select()
        .from(selectTable)
        .where(field("column1").eq("TEST1"))
        .optimiseForRowCount(2)
        .useImplicitJoinOrder()
        .useIndex(selectTable, "SelectDistinctTable_1")
      )
    ));

    Integer numberOfRecords = getNumberOfRecordsFromSelect(select().from(insertTable));

    assertEquals("Should have 1 records selected", 1, numberOfRecords.intValue());
  }


  /**
   * Tests a basic window functions with running total.
   */
  @Test
  public void testWindowFunction() {
    assertResultsMatch(
      select(
       field("id"),
       field("partitionValue1"),

        windowFunction(
          sum(field("aggregationValue")))
          .partitionBy(field("partitionValue1"))
          .orderBy(field("id"))
          .build().as("runningTotal"))

        .from(tableRef("WindowFunctionTable"))
        .orderBy(field("partitionValue1")),

        "1-A-2.1",
        "2-A-5.3",
        "4-A-9.1",
        "5-A-11",
        "3-B-5.7",
        "6-B-9.1",
        "7-B-19.3");
  }


  /**
   * Tests a basic window functions which partitions by multiple columns
   */
  @Test
  public void testWindowFunctionMultiPartitionBy() {

    assertResultsMatch(
      select(
       field("id"),
       field("partitionValue1"),
       field("partitionValue2"),
        windowFunction(
          average(field("aggregationValue")))
          .partitionBy(field("partitionValue1"),field("partitionValue2"))
          .orderBy(field("id"))
          .build().as("movingAverage"),

          windowFunction(
           count())
           .partitionBy(field("partitionValue1"),field("partitionValue2"))
           .build().as("countPerPartition"))

        .from(tableRef("WindowFunctionTable"))
        .orderBy(field("id")),

        "1-A-Z-2.1-2",
        "2-A-Y-3.2-2",
        "3-B-Z-5.7-2",
        "4-A-Y-3.5-2",
        "5-A-Z-2-2",
        "6-B-Z-4.55-2",
        "7-B-Y-10.2-1");
  }


  /**
   * Tests a window function with an order by but no partition by.
   */
  @Test
  public void testWindowFunctionWithOrderByNoPartitionBy() {

    assertResultsMatch(
      select(
       windowFunction(
         count())
         .orderBy(field("partitionValue1"))
         .build().as("theCount"))
       .from(tableRef("WindowFunctionTable"))
       .orderBy(field("partitionValue1")),

       "4","4","4","4","7","7","7");
  }


  /**
   * Tests a window function with a partition by but no order by.
   * The entire partition is used as the window frame, demonstrated here as the sum is over each partition,
   * not a running total.
   */
  @Test
  public void testWindowFunctionWithPartitionByNoOrderBy() {

    assertResultsMatch(
      select(
        windowFunction(
          sum(field("aggregationValue")))
          .partitionBy(field("partitionValue1"))
          .build().as("unorderedWindowSum"))
        .from(tableRef("WindowFunctionTable"))
        .orderBy(field("partitionValue1")),

       "11","11","11","11","19.3","19.3","19.3");
  }


  /**
   * Tests a a window function with no order nor partition by.
   */
  @Test
  public void testWindowFunctionWithoutOrderByOrPartitionBy() {

    assertResultsMatch(
      select(
        windowFunction(
          count())
         .build().as("totalCount"),

      windowFunction(
        sum(field("aggregationValue")))
       .build().as("totalSum"))

        .from(tableRef("WindowFunctionTable")),

      "7-30.3","7-30.3","7-30.3","7-30.3","7-30.3","7-30.3","7-30.3");
  }


  /**
   * Tests behaviour of the Every function
   */
  @Test
  public void testEveryFunction() {
    SelectStatement selectEvery =
        select(
          every(field("column1")).as("column1Every"),
          every(field("column2")).as("column2Every"))
        .from("AccumulateBooleanTable");
    sqlScriptExecutorProvider.get().executeQuery(selectEvery).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("Aggregated Every value of column1 should be", false, resultSet.getBoolean(1));
          assertEquals("Aggregated Every value of column2 should be", true, resultSet.getBoolean(2));
        }
        return null;
      }
    });
  }


  /**
   * Tests behaviour of the Some function
   */
  @Test
  public void testSomeFunction() {
    SelectStatement selectSome =
        select(
          some(field("column1")).as("column1Every"),
          some(field("column2")).as("column2Every"))
        .from("AccumulateBooleanTable");
    sqlScriptExecutorProvider.get().executeQuery(selectSome).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          assertEquals("Aggregated Some value of column1 should be", true, resultSet.getBoolean(1));
          assertEquals("Aggregated Some value of column2 should be", true, resultSet.getBoolean(2));
        }
        return null;
      }
    });
  }


  /**
   * Tests behaviour of Some function with a case statement as the argument.
   */
  @Test
  public void testSomeFunctionWithACaseStatement() {
    CaseStatement caseStmt = caseStatement(
      when(cast(field("id")).asType(DataType.INTEGER).lessThanOrEqualTo(literal(1))).then(true))
        .otherwise(false);
    SelectStatement selectComplexSome = select(some(caseStmt), every(caseStmt)).from(tableRef("WithDefaultValue"));
    sqlScriptExecutorProvider.get().executeQuery(selectComplexSome).processWith(new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
        assertEquals("Aggregated value of id should be", true, resultSet.getBoolean(1));
        assertEquals("Aggregated value of id should be", false, resultSet.getBoolean(2));
      }
      return null;
      }
    });
  }


  /**
   * Test the greatest SQL function against all {@linkplain SqlDialect}s
   */
  @Test
  public void testGreatest() throws SQLException {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    executor.executeQuery(convertStatementToSQL(select(greatest(literal(1), literal(7), literal(-1)))), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        assertTrue (resultSet.next());
        assertEquals(7, resultSet.getInt(1));
        return null;
      }
    });

    executor.executeQuery(convertStatementToSQL(select(greatest(ImmutableList.of(literal(1), literal(7), literal(-1))))), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        assertTrue (resultSet.next());
        assertEquals(7, resultSet.getInt(1));
        return null;
      }
    });
  }


  /**
   * Test the greatest SQL function against all {@linkplain SqlDialect}s
   */
  @Test
  public void testLeast() throws SQLException {
    final SqlScriptExecutor executor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    executor.executeQuery(convertStatementToSQL(select(least(literal(1), literal(7), literal(-1)))), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        assertTrue (resultSet.next());
        assertEquals(-1, resultSet.getInt(1));
        return null;
      }
    });

    executor.executeQuery(convertStatementToSQL(select(least(ImmutableList.of(literal(1), literal(7), literal(-1))))), connection, new ResultSetProcessor<Void>() {
      @Override
      public Void process(ResultSet resultSet) throws SQLException {
        assertTrue (resultSet.next());
        assertEquals(-1, resultSet.getInt(1));
        return null;
      }
    });
  }


  private void assertResultsMatch(SelectStatement statement, String... expectedJoinedStringRows) {
    List<String> result = sqlScriptExecutorProvider
        .get(new LoggingSqlScriptVisitor())
        .executeQuery(statement)
        .processWith(processResultsAsJoinedString());

    assertEquals(Lists.<String>newArrayList(expectedJoinedStringRows),result);
  }


  private ResultSetProcessor<List<String>> processResultsAsJoinedString(){
    return new ResultSetProcessor<>() {
      @Override
      public List<String> process(ResultSet resultSet) throws SQLException {
        int numberOfColumns = resultSet.getMetaData().getColumnCount();
        List<String> result = Lists.newArrayList();
        String[] row = new String[numberOfColumns];

        while (resultSet.next()) {
          for(int i = 0; i < numberOfColumns; i++) {
            String valueAsString = resultSet.getString(i+1);
            row[i] = valueAsString.contains(".") ?  valueAsString.replaceAll("0*$", "").replaceAll("\\.$", "") : valueAsString;
          }
          result.add(Joiner.on("-").join(row));
        }

        return result;
      }
    };
  }


  private Integer getNumberOfRecordsFromSelect(SelectStatement selectStatement) {
    return sqlScriptExecutorProvider
          .get(new LoggingSqlScriptVisitor())
          .executeQuery(selectStatement)
          .processWith(new ResultSetProcessor<Integer>() {
            @Override
            public Integer process(ResultSet resultSet) throws SQLException {
              int count = 0;
              while (resultSet.next()) {
                count++;
              }
              return count;
            }
          });
  }


  private Integer getNumberOfRecordsInMergedTable(TableReference table) {
    return getNumberOfRecordsFromSelect(select(table.field("column1"), table.field("column2")).from(table));
  }
}
