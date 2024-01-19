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

import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISMATCH;
import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISSING_LEFT;
import static org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType.MISSING_RIGHT;
import static org.alfasoftware.morf.metadata.DataSetUtils.dataSetProducer;
import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.DataType.INTEGER;
import static org.alfasoftware.morf.metadata.DataType.STRING;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Function.count;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.ResultSetComparer;
import org.alfasoftware.morf.jdbc.ResultSetComparer.CompareCallback;
import org.alfasoftware.morf.jdbc.ResultSetComparer.ResultSetValidation;
import org.alfasoftware.morf.jdbc.ResultSetMismatch;
import org.alfasoftware.morf.jdbc.ResultSetMismatch.MismatchType;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.StatementParameters;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.stringcomparator.TestingDatabaseEquivalentStringComparator;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;

import net.jcip.annotations.NotThreadSafe;

/**
 * Tests that the result set comparer is valid for all supported database platforms.
 * <p>
 * This test will setup a basic {@link Schema} which can then be used to run
 * tests against.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
@NotThreadSafe
public class TestResultSetComparer {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule(), new TestingDatabaseEquivalentStringComparator.Module());
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Inject private Provider<DatabaseDataSetConsumer> databaseDataSetConsumer;
  @Inject private Provider<DatabaseSchemaManager> schemaManager;
  @Inject private ResultSetComparer.Factory resultSetComparerFactory;

  private ResultSetComparer resultSetComparer;

  @Inject private DataSource dataSource;
  private Connection connection;
  private boolean autoCommitInitalState;


  @Before
  public void onSetup() throws SQLException {
    resultSetComparer = resultSetComparerFactory.create();
    connection = dataSource.getConnection();

    //Disable autocommit
    autoCommitInitalState = connection.getAutoCommit();
    connection.setAutoCommit(false);

    // We don't want to inherit some old sequence numbers on existing tables - drop
    // so we reset any autonumbering
    schemaManager.get().invalidateCache();
    schemaManager.get().dropTablesIfPresent(ImmutableSet.of("Autonumbered"));
    schemaManager.get().mutateToSupportSchema(schema, TruncationBehavior.ALWAYS);
    new DataSetConnector(dataSet, databaseDataSetConsumer.get()).connect();
  }


  @After
  public void tearDown() throws SQLException {
    connection.setAutoCommit(autoCommitInitalState);
    connection.close();
  }


  /**
   * The test schema.
   */
  private final Schema schema = schema(
    table("SingleKeyLeft")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("nullableDateCol", DataType.INTEGER).nullable(),
        column("nullableDecimalCol", DataType.DECIMAL, 10, 2).nullable(),
        column("nullableCLOBCol", DataType.BLOB).nullable(),
        column("booleanCol", DataType.BOOLEAN)
      ),
    table("SingleKeyMatchRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("nullableDateCol", DataType.INTEGER).nullable(),
        column("nullableDecimalCol", DataType.DECIMAL, 10, 2).nullable()
      ),
    table("SingleKeyMismatchRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("nullableDateCol", DataType.INTEGER).nullable(),
        column("nullableDecimalCol", DataType.DECIMAL, 10, 2).nullable()
      ),
    table("SingleKeyNullMismatchRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("nullableDateCol", DataType.INTEGER).nullable(),
        column("nullableDecimalCol", DataType.DECIMAL, 10, 2).nullable()
      ),
    table("SingleKeyMissingRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("nullableDateCol", DataType.INTEGER).nullable(),
        column("nullableDecimalCol", DataType.DECIMAL, 10, 2).nullable()
      ),
    table("MultiKeyLeft")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intKey", DataType.INTEGER).primaryKey(),
        column("nullableStringCol", DataType.STRING, 20).nullable(),
        column("nullableIntCol", DataType.INTEGER).nullable()
      ),
    table("MultiKeyMatchRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intKey", DataType.INTEGER).primaryKey(),
        column("nullableStringCol", DataType.STRING, 20).nullable(),
        column("nullableIntCol", DataType.INTEGER).nullable()
      ),
    table("MultiKeyMismatchRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intKey", DataType.INTEGER).primaryKey(),
        column("nullableStringCol", DataType.STRING, 20).nullable(),
        column("nullableIntCol", DataType.INTEGER).nullable()
      ),
    table("MultiKeyMissingRight")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intKey", DataType.INTEGER).primaryKey(),
        column("nullableStringCol", DataType.STRING, 20).nullable(),
        column("nullableIntCol", DataType.INTEGER).nullable()
      ),
      table("ComparableNumericalColumns")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intCol", DataType.INTEGER),
        column("zeroScaleDecimalCol", DataType.DECIMAL, 10, 0)
      ),
      table("UnComparableNumericalColumns")
      .columns(
        column("stringKey", DataType.STRING, 10).primaryKey(),
        column("intCol", DataType.INTEGER),
        column("decimalCol", DataType.DECIMAL, 10, 2)
      )
  );


  /**
   * The test dataset.
   */
  private final DataSetProducer dataSet = dataSetProducer(schema)
    .table("SingleKeyLeft",
      record()
        .setString("stringKey", "keyA")
        .setInteger("nullableDateCol", 20140101)
        .setString("nullableDecimalCol", "10.11")
        .setBoolean("booleanCol", false),
      record()
        .setString("stringKey", "keyB")
        .setInteger("nullableDateCol", 20140201)
        .setString("nullableDecimalCol", "10.2")
        .setBoolean("booleanCol", true),
      record()
        .setString("stringKey", "keyC")
        .setInteger("nullableDateCol", 20140301)
        .setString("nullableDecimalCol", "10.3")
        .setBoolean("booleanCol", false)
    )
    .table("SingleKeyMatchRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("nullableDateCol", 20140101)
        .setString("nullableDecimalCol", "10.11"),
      record()
        .setString("stringKey", "keyB")
        .setInteger("nullableDateCol", 20140201)
        .setString("nullableDecimalCol", "10.2"),
      record()
        .setString("stringKey", "keyC")
        .setInteger("nullableDateCol", 20140301)
        .setString("nullableDecimalCol", "10.3")
    )
    .table("SingleKeyMismatchRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("nullableDateCol", 20141101)
        .setString("nullableDecimalCol", "11.1"),
      record()
        .setString("stringKey", "keyB")
        .setInteger("nullableDateCol", 20140201)
        .setString("nullableDecimalCol", "10.2")
    )
    .table("SingleKeyNullMismatchRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("nullableDateCol", 20141101)
        .setString("nullableDecimalCol", null),
      record()
        .setString("stringKey", "keyB")
        .setInteger("nullableDateCol", 20140201)
        .setString("nullableDecimalCol", "10.2")
    )
    .table("SingleKeyMissingRight",
      record()
        .setString("stringKey", "keyB")
        .setInteger("nullableDateCol", 20140201)
        .setString("nullableDecimalCol", "10.2"),
      record()
        .setString("stringKey", "keyC")
        .setInteger("nullableDateCol", 20140301)
        .setString("nullableDecimalCol", "10.3"),
      record()
        .setString("stringKey", "keyD")
        .setInteger("nullableDateCol", 20140401)
        .setString("nullableDecimalCol", "10.4")
    )
    .table("MultiKeyLeft",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intKey", 1)
        .setString("nullableStringCol", "valueA")
        .setInteger("nullableIntCol", 1),
      record()
        .setString("stringKey", "keyB")
        .setInteger("intKey", 2)
        .setString("nullableStringCol", null)
        .setInteger("nullableIntCol", 2),
      record()
        .setString("stringKey", "keyC")
        .setInteger("intKey", 3)
        .setString("nullableStringCol", "valueC")
        .setInteger("nullableIntCol", 3)
    )
    .table("MultiKeyMatchRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intKey", 1)
        .setString("nullableStringCol", "valueA")
        .setInteger("nullableIntCol", 1),
      record()
        .setString("stringKey", "keyB")
        .setInteger("intKey", 2)
        .setString("nullableStringCol", null)
        .setInteger("nullableIntCol", 2),
      record()
        .setString("stringKey", "keyC")
        .setInteger("intKey", 3)
        .setString("nullableStringCol", "valueC")
        .setInteger("nullableIntCol", 3)
    )
    .table("MultiKeyMismatchRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intKey", 1)
        .setString("nullableStringCol", "valueA")
        .setInteger("nullableIntCol", 1),
      record()
        .setString("stringKey", "keyB")
        .setInteger("intKey", 2)
        .setString("nullableStringCol", "valueFlopB")
        .setInteger("nullableIntCol", 2),
      record()
        .setString("stringKey", "keyC")
        .setInteger("intKey", 3)
        .setString("nullableStringCol", null)
        .setInteger("nullableIntCol", 3)
    )
    .table("MultiKeyMissingRight",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intKey", 1)
        .setString("nullableStringCol", "valueA")
        .setInteger("nullableIntCol", 1),
      record()
        .setString("stringKey", "keyB")
        .setInteger("intKey", 3)
        .setString("nullableStringCol", "valueB3")
        .setInteger("nullableIntCol", 3),
      record()
        .setString("stringKey", "keyC")
        .setInteger("intKey", 4)
        .setString("nullableStringCol", "valueC4")
        .setInteger("nullableIntCol", 4)
    )
    .table("ComparableNumericalColumns",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intCol", 5)
        .setString("zeroScaleDecimalCol", "1")
    )
    .table("UnComparableNumericalColumns",
      record()
        .setString("stringKey", "keyA")
        .setInteger("intCol", 5)
        .setString("decimalCol", "1.00")
    );


  /**
   * Test comparing where the metadata of the selects is ambiguous.  This affects
   * Oracle only and results in the metadata scale getting misreported.
   * We should be able to handle it.
   *
   * @
   */
  @Test
  public void testAmbiguousMetadata()  {

    SelectStatement left = select(sum(field("a"))).from(
      select(
        cast(literal("5.56")).asType(DataType.DECIMAL, 3, 2).as("a")
      ).union(select(
        cast(literal("5")).asType(DataType.DECIMAL, 3, 2).as("a")
      )).alias("s")
    );

    SelectStatement right = select(sum(field("a"))).from(
      select(
        cast(literal("5.56")).asType(DataType.DECIMAL, 3, 2).as("a")
      ).union(select(
        cast(literal("5")).asType(DataType.DECIMAL, 3, 2).as("a")
      )).alias("s")
    );

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, never()).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("Row result should match", 0, result);
  }


  /**
   * Test compare count(*) on data sets
   *
   * @
   */
  @Test
  public void testSingleRowResult()  {
    SelectStatement left = select(count()).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(count()).from(tableRef("SingleKeyMatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, never()).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("Row result should match", 0, result);
  }


  /**
   * Test compare value on data sets.
   *
   */
  @Test
  public void testSingleKeyValue()  {
    SelectStatement left = select(field("stringKey"), field("nullableDateCol")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("nullableDateCol")).from(tableRef("SingleKeyMatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, never()).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("All rows should match", 0, result);
  }


  /**
   * Test compare value on data sets with composite key.
   *
   */
  @Test
  public void testMultiKeyValue()  {
    SelectStatement left = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyMatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, never()).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("All rows should match", 0, result);
  }


  /**
   * Test compare count(*) on data sets where there are mismatch.
   *
   */
  @Test
  public void testSingleRowResultMismatch()  {
    SelectStatement left = select(count()).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(count()).from(tableRef("SingleKeyMismatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int mismatchCount = resultSetComparer.compare(new int[]{}, left, right, connection, callBackMock);

    verify(callBackMock).mismatch(rsMismatchCaptor.capture());
    assertEquals("Row count should have 1 mismatch", 1, mismatchCount);
    checkMismatch(rsMismatchCaptor.getValue(), MISMATCH, "3", "2", 1);
  }


  /**
   * Test compare value on data sets where there are mismatch.
   *
   */
  @Test
  public void testSingleKeyValueMismatch()  {
    SelectStatement left = select(field("stringKey"), field("nullableDateCol"), field("nullableDecimalCol")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("nullableDateCol"), field("nullableDecimalCol")).from(tableRef("SingleKeyMismatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, times(4)).mismatch(rsMismatchCaptor.capture());
    List<ResultSetMismatch> rsMismatches = rsMismatchCaptor.getAllValues();
    assertEquals("Row count should match", rsMismatches.size(), result);

    checkMismatch(rsMismatches.get(0), MISMATCH, "20140101", "20141101", 2, "keyA");
    checkMismatch(rsMismatches.get(1), MISMATCH, "10.11", "11.1", 3, "keyA");
    checkMismatch(rsMismatches.get(2), MISSING_RIGHT, "20140301", ResultSetComparer.RECORD_NOT_PRESENT, 2, "keyC");
    checkMismatch(rsMismatches.get(3), MISSING_RIGHT, "10.3", ResultSetComparer.RECORD_NOT_PRESENT, 3, "keyC");
  }


  /**
   * Test compare value on data sets where there are mismatch.
   *
   */
  @Test
  public void testSingleKeyValueMismatchToNull() {
    SelectStatement left = select(field("stringKey"), field("nullableDateCol"), field("nullableDecimalCol")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("nullableDateCol"), field("nullableDecimalCol")).from(tableRef("SingleKeyNullMismatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, times(4)).mismatch(rsMismatchCaptor.capture());
    List<ResultSetMismatch> rsMismatches = rsMismatchCaptor.getAllValues();
    assertEquals("Row count should match", rsMismatches.size(), result);

    checkMismatch(rsMismatches.get(0), MISMATCH, "20140101", "20141101", 2, "keyA");
    checkMismatch(rsMismatches.get(1), MISMATCH, "10.11", null, 3, "keyA");
    checkMismatch(rsMismatches.get(2), MISSING_RIGHT, "20140301", ResultSetComparer.RECORD_NOT_PRESENT, 2, "keyC");
    checkMismatch(rsMismatches.get(3), MISSING_RIGHT, "10.3", ResultSetComparer.RECORD_NOT_PRESENT, 3, "keyC");
  }


  private void checkMismatch(ResultSetMismatch mismatch, MismatchType type, String left, String right, int colIndex, String... key) {
    assertEquals("Mismatch type", type, mismatch.getMismatchType());
    assertEquals("Mismatch left", left, mismatch.getLeftValue());
    assertEquals("Mismatch right", right, mismatch.getRightValue());
    assertEquals("Mismatch col index", colIndex, mismatch.getMismatchColumnIndex());
    assertArrayEquals("Key of the data not match", key, mismatch.getKey());
  }


  /**
   * Test compare value on data sets with composite key where there are mismatch.
   *
   */
  @Test
  public void testMultiKeyValueMismatch()  {
    SelectStatement left = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyMismatchRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1, 2}, left, right, connection, callBackMock);

    verify(callBackMock, times(2)).mismatch(rsMismatchCaptor.capture());
    List<ResultSetMismatch> rsMismatches = rsMismatchCaptor.getAllValues();
    assertEquals("Mismatch count", rsMismatches.size(), result);

    checkMismatch(rsMismatches.get(0), MISMATCH, null,     "valueFlopB", 3, "keyB", "2");
    checkMismatch(rsMismatches.get(1), MISMATCH, "valueC", null,         3, "keyC", "3");
  }


  /**
   * Test compare single row result set where the left hand side is null.
   *
   */
  @Test
  public void testSingleRowResultMissing()  {
    SelectStatement left = select(field("stringKey")).from(tableRef("SingleKeyLeft")).where(field("stringKey").eq("keyA"));
    SelectStatement right = select(field("stringKey")).from(tableRef("SingleKeyLeft")).where(literal("A").eq("B"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int mismatchCount = resultSetComparer.compare(new int[]{}, left, right, connection, callBackMock);
    assertEquals("Mismatch count", 1, mismatchCount);
    verify(callBackMock).mismatch(rsMismatchCaptor.capture());

    checkMismatch(rsMismatchCaptor.getValue(), MISSING_RIGHT, "keyA", ResultSetComparer.RECORD_NOT_PRESENT, 1);
  }


  /**
   * Test compare value on data sets where there are missing data.
   *
   */
  @Test
  public void testSingleKeyValueMissing()  {
    SelectStatement left = select(field("stringKey"), field("nullableDateCol")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("nullableDateCol")).from(tableRef("SingleKeyMissingRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock, times(2)).mismatch(rsMismatchCaptor.capture());
    List<ResultSetMismatch> rsMismatches = rsMismatchCaptor.getAllValues();
    assertEquals("Row count should match", rsMismatches.size(), result);

    checkMismatch(rsMismatches.get(0), MISSING_RIGHT, "20140101", ResultSetComparer.RECORD_NOT_PRESENT, 2, "keyA");
    checkMismatch(rsMismatches.get(1), MISSING_LEFT, ResultSetComparer.RECORD_NOT_PRESENT, "20140401", 2, "keyD");
  }


  /**
   * Test compare value on data sets with composite key where there are missing data.
   *
   */
  @Test
  public void testMultiKeyValueMissing()  {
    SelectStatement left = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("intKey"), field("nullableStringCol"), field("nullableIntCol")).from(tableRef("MultiKeyMissingRight"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1, 2}, left, right, connection, callBackMock);

    verify(callBackMock, times(8)).mismatch(rsMismatchCaptor.capture());
    List<ResultSetMismatch> rsMismatches = rsMismatchCaptor.getAllValues();
    assertEquals("Row count should match", rsMismatches.size(), result);

    checkMismatch(rsMismatches.get(0), MISSING_RIGHT, null, ResultSetComparer.RECORD_NOT_PRESENT, 3, "keyB", "2");
    checkMismatch(rsMismatches.get(1), MISSING_RIGHT, "2", ResultSetComparer.RECORD_NOT_PRESENT, 4, "keyB", "2");
    checkMismatch(rsMismatches.get(2), MISSING_LEFT, ResultSetComparer.RECORD_NOT_PRESENT, "valueB3", 3, "keyB", "3");
    checkMismatch(rsMismatches.get(3), MISSING_LEFT, ResultSetComparer.RECORD_NOT_PRESENT, "3", 4, "keyB", "3");
    checkMismatch(rsMismatches.get(4), MISSING_RIGHT, "valueC", ResultSetComparer.RECORD_NOT_PRESENT, 3, "keyC", "3");
    checkMismatch(rsMismatches.get(5), MISSING_RIGHT, "3", ResultSetComparer.RECORD_NOT_PRESENT, 4, "keyC", "3");
    checkMismatch(rsMismatches.get(6), MISSING_LEFT, ResultSetComparer.RECORD_NOT_PRESENT, "valueC4", 3, "keyC", "4");
    checkMismatch(rsMismatches.get(7), MISSING_LEFT, ResultSetComparer.RECORD_NOT_PRESENT, "4", 4, "keyC", "4");
  }


  /**
   * Test data sets where their column type do not match.
   *
   */
  @Test
  public void testColumnTypeMismatch()  {
    SelectStatement left = select(field("stringKey")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("nullableDecimalCol")).from(tableRef("SingleKeyMismatchRight"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Column metadata does not match");
    resultSetComparer.compare(new int[]{1}, left, right, connection, mock(CompareCallback.class));
  }


  /**
   *  Checks that two different compatible numerical types can be compared, but have different values
   */
  @Test
  public void testNumericalColumnTypeMismatchIsComparable()  {
    SelectStatement left = select(field("stringKey"), field("intCol")).from(tableRef("ComparableNumericalColumns"));
    SelectStatement right = select(field("stringKey"), field("zeroScaleDecimalCol")).from(tableRef("ComparableNumericalColumns"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock,times(1)).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("Different values expected", 1, result);
  }


  /**
   *  Checks that two different numerical types can be compared, e.g. an INTEGER and a DECIMAL
   */
  @Test
  public void testNumericalColumnTypeMismatchIsComparable2()  {
    SelectStatement left = select(field("stringKey"), field("intCol")).from(tableRef("UnComparableNumericalColumns"));
    SelectStatement right = select(field("stringKey"), field("decimalCol")).from(tableRef("UnComparableNumericalColumns"));

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> resultSetMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int result = resultSetComparer.compare(new int[]{1}, left, right, connection, callBackMock);

    verify(callBackMock,times(1)).mismatch(resultSetMismatchCaptor.capture());
    assertEquals("Different values expected", 1, result);
  }


  /**
   * Checks that we can handle decimal types with different widths and scales
   */
  @Test
  public void testCanHandleCompatibleMismatchedTypes1()  {
    SelectStatement left = select(literal(21.3));
    SelectStatement right = select(sum(field("nullableDecimalCol"))).from(tableRef("SingleKeyMismatchRight"));
    resultSetComparer.compare(new int[]{1}, left, right, connection, mock(CompareCallback.class));
  }


  /**
   * Checks that we can handle decimal types with different widths and scales
   */
  @Test
  public void testCanHandleCompatibleMismatchedTypes2()  {
    SelectStatement left = select(count()).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(literal(3));
    resultSetComparer.compare(new int[]{1}, left, right, connection, mock(CompareCallback.class));
  }


  /**
   * Test data sets where they do not have the same number of columns.
   *
   */
  @Test
  public void testColumnCountMismatch()  {
    SelectStatement left = select(field("stringKey")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey"), field("nullableDateCol")).from(tableRef("SingleKeyMismatchRight"));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Column counts mismatch");
    resultSetComparer.compare(new int[]{1}, left, right, connection, mock(CompareCallback.class));
  }


  /**
   * Test data sets with no key column defined but contain multiple rows.
   *
   */
  @Test
  public void testKeylessMultiRowResult()  {
    SelectStatement left = select(field("stringKey")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("stringKey")).from(tableRef("SingleKeyMismatchRight"));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Comparison can only handle one row for keyless result sets");
    resultSetComparer.compare(new int[]{}, left, right, connection, mock(CompareCallback.class));
  }


  /**
   * Negative test with booleans
   */
  @Test
  public void testBooleanMatch()  {
    SelectStatement left1 = select(field("booleanCol")).from(tableRef("SingleKeyLeft")).where(field("stringKey").eq("keyA"));
    SelectStatement right = select(field("booleanCol")).from(tableRef("SingleKeyLeft")).where(field("stringKey").eq("keyC"));

    CompareCallback callBackMock = mock(CompareCallback.class);

    resultSetComparer.compare(new int[]{}, left1, right, connection, callBackMock);

    verifyNoMoreInteractions(callBackMock);
  }


  /**
   * Positive test with booleans
   */
  @Test
  public void testBooleanNoMatch()  {
    SelectStatement left1 = select(field("booleanCol")).from(tableRef("SingleKeyLeft")).where(field("stringKey").eq("keyA"));
    SelectStatement right = select(field("booleanCol")).from(tableRef("SingleKeyLeft")).where(field("stringKey").eq("keyB"));

    CompareCallback callBackMock = mock(CompareCallback.class);

    resultSetComparer.compare(new int[]{}, left1, right, connection, callBackMock);

    verify(callBackMock).mismatch(any(ResultSetMismatch.class));
  }


  /**
   * Test non-supported data type.
   *
   */
  @Test
  public void testUnsupportedDataType()  {
    SelectStatement left1 = select(field("nullableCLOBCol")).from(tableRef("SingleKeyLeft"));
    SelectStatement right = select(field("nullableCLOBCol")).from(tableRef("SingleKeyLeft"));

    thrown.expect(IllegalArgumentException.class);
    resultSetComparer.compare(new int[]{}, left1, right, connection, mock(CompareCallback.class));
  }


  /**
   * Tests using statement parameters
   */
  @Test
  public void testWithStatmentParameters()  {
    SelectStatement left = select(count()).from(tableRef("MultiKeyLeft")).where(field("intKey").eq(parameter("param1").type(INTEGER)));
    SelectStatement right = select(count()).from(tableRef("MultiKeyMatchRight")).where(field("stringKey").eq(parameter("param2").type(STRING)));

    StatementParameters leftParams = DataSetUtils.statementParameters().setInteger("param1", 2); // <-- Exists
    StatementParameters rightParams = DataSetUtils.statementParameters().setString("param2", "NonExistent"); // <-- Does not exist

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int mismatchCount = resultSetComparer.compare(new int[]{}, left, right, connection, connection, callBackMock, leftParams, rightParams);

    verify(callBackMock).mismatch(rsMismatchCaptor.capture());
    assertEquals("Row count should have 1 mismatch", 1, mismatchCount);
    checkMismatch(rsMismatchCaptor.getValue(), MISMATCH, "1", "0", 1);
  }


  /**
   * Tests using statement parameters on the left side only
   */
  @Test
  public void testWithStatmentParametersLeftOnly()  {
    SelectStatement left = select(count()).from(tableRef("MultiKeyLeft")).where(field("intKey").eq(parameter("param1").type(INTEGER)));
    SelectStatement right = select(count()).from(tableRef("MultiKeyMatchRight"));

    StatementParameters leftParams = DataSetUtils.statementParameters().setInteger("param1", 2); // <-- Exists
    StatementParameters rightParams = DataSetUtils.statementParameters();

    CompareCallback callBackMock = mock(CompareCallback.class);
    ArgumentCaptor<ResultSetMismatch> rsMismatchCaptor = ArgumentCaptor.forClass(ResultSetMismatch.class);

    int mismatchCount = resultSetComparer.compare(new int[]{}, left, right, connection, connection, callBackMock, leftParams, rightParams);

    verify(callBackMock).mismatch(rsMismatchCaptor.capture());
    assertEquals("Row count should have 1 mismatch", 1, mismatchCount);
    checkMismatch(rsMismatchCaptor.getValue(), MISMATCH, "1", "3", 1);
  }


  /**
   * Tests the validation of a non empty result sets.
   */
  @Test
  public void testNonEmptyResultSetValidation()  {
    SelectStatement left = select(field("intKey")).from(tableRef("MultiKeyLeft")).where(field("intKey").eq(parameter("param1").type(INTEGER)));
    SelectStatement right = select(field("intKey")).from(tableRef("MultiKeyMatchRight")).where(field("intKey").eq(parameter("param1").type(INTEGER)));

    StatementParameters leftParams = DataSetUtils.statementParameters().setInteger("param1", 99); // <-- Does not exist
    StatementParameters rightParams = DataSetUtils.statementParameters().setInteger("param1", 99); // <-- Does not exist

    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> resultSetComparer.compare(new int[]{}, left, right, connection, connection, mock(CompareCallback.class), leftParams, rightParams, ResultSetValidation.NON_EMPTY_RESULT));
    assertTrue(exception.getMessage().contains("The following queries should return at least one record"));
  }


  /**
   * Tests the validation of a non zero result count.
   */
  @Test
  public void testNonZeroCountResultSetValidation()  {
    SelectStatement left = select(count()).from(tableRef("MultiKeyLeft")).where(field("intKey").eq(parameter("param1").type(INTEGER)));
    SelectStatement right = select(count()).from(tableRef("MultiKeyMatchRight")).where(field("intKey").eq(parameter("param1").type(INTEGER)));

    StatementParameters leftParams = DataSetUtils.statementParameters().setInteger("param1", 99); // <-- Does not exist
    StatementParameters rightParams = DataSetUtils.statementParameters().setInteger("param1", 99); // <-- Does not exist

    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> resultSetComparer.compare(new int[]{}, left, right, connection, connection, mock(CompareCallback.class), leftParams, rightParams, ResultSetValidation.NON_ZERO_RESULT));
    assertTrue(exception.getMessage().contains("Left and right record queries returned zero"));
  }

}
