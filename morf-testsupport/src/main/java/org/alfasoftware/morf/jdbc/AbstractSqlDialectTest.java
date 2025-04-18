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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.alfasoftware.morf.metadata.DataSetUtils.statementParameters;
import static org.alfasoftware.morf.metadata.SchemaUtils.autonumber;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.sequence;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.blobLiteral;
import static org.alfasoftware.morf.sql.SqlUtils.bracket;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.merge;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.selectDistinct;
import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.SqlUtils.windowFunction;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.eq;
import static org.alfasoftware.morf.sql.element.Criterion.exists;
import static org.alfasoftware.morf.sql.element.Criterion.greaterThan;
import static org.alfasoftware.morf.sql.element.Criterion.greaterThanOrEqualTo;
import static org.alfasoftware.morf.sql.element.Criterion.in;
import static org.alfasoftware.morf.sql.element.Criterion.isNotNull;
import static org.alfasoftware.morf.sql.element.Criterion.isNull;
import static org.alfasoftware.morf.sql.element.Criterion.lessThan;
import static org.alfasoftware.morf.sql.element.Criterion.lessThanOrEqualTo;
import static org.alfasoftware.morf.sql.element.Criterion.like;
import static org.alfasoftware.morf.sql.element.Criterion.neq;
import static org.alfasoftware.morf.sql.element.Criterion.not;
import static org.alfasoftware.morf.sql.element.Criterion.or;
import static org.alfasoftware.morf.sql.element.Function.addDays;
import static org.alfasoftware.morf.sql.element.Function.addMonths;
import static org.alfasoftware.morf.sql.element.Function.average;
import static org.alfasoftware.morf.sql.element.Function.averageDistinct;
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
import static org.alfasoftware.morf.sql.element.Function.least;
import static org.alfasoftware.morf.sql.element.Function.leftPad;
import static org.alfasoftware.morf.sql.element.Function.leftTrim;
import static org.alfasoftware.morf.sql.element.Function.lowerCase;
import static org.alfasoftware.morf.sql.element.Function.max;
import static org.alfasoftware.morf.sql.element.Function.min;
import static org.alfasoftware.morf.sql.element.Function.mod;
import static org.alfasoftware.morf.sql.element.Function.now;
import static org.alfasoftware.morf.sql.element.Function.power;
import static org.alfasoftware.morf.sql.element.Function.random;
import static org.alfasoftware.morf.sql.element.Function.randomString;
import static org.alfasoftware.morf.sql.element.Function.rightPad;
import static org.alfasoftware.morf.sql.element.Function.rightTrim;
import static org.alfasoftware.morf.sql.element.Function.round;
import static org.alfasoftware.morf.sql.element.Function.rowNumber;
import static org.alfasoftware.morf.sql.element.Function.some;
import static org.alfasoftware.morf.sql.element.Function.substring;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.alfasoftware.morf.sql.element.Function.sumDistinct;
import static org.alfasoftware.morf.sql.element.Function.trim;
import static org.alfasoftware.morf.sql.element.Function.upperCase;
import static org.alfasoftware.morf.sql.element.Function.yyyymmddToDate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.CustomHint;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ClobFieldLiteral;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.PortableSqlFunction;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.alfasoftware.morf.upgrade.AddColumn;
import org.alfasoftware.morf.upgrade.ChangeColumn;
import org.alfasoftware.morf.upgrade.ChangeIndex;
import org.alfasoftware.morf.upgrade.RemoveColumn;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Abstract test case that defines behaviour for an implementation of {@link SqlDialect}.
 *
 * <p>Tests are based on meta data from the following tables:</p>
 * <ul>
 * <li>Test - variety of fields and indexes.</li>
 * <li>Alternate - simple alternate table when a second table is required.</li>
 * <li>Other - yet another alternate table for testing really complex stuff.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public abstract class AbstractSqlDialectTest {

  protected static final String TEST_TABLE = "Test";
  private static final String TEST_NK = "Test_NK";
  private static final String TEST_1 = "Test_1";
  private static final String TEST_2 = "Test_2";

  private static final String TEMP_TEST_TABLE = "TempTest";
  private static final String TEMP_TEST_NK = "TempTest_NK";
  private static final String TEMP_TEST_1 = "TempTest_1";
  private static final String TEMP_TEST_2 = "TempTest_2";

  private static final String ALTERNATE_TABLE = "Alternate";
  protected static final String OTHER_TABLE = "Other";
  private static final String UPPER_TABLE = "UPPER";
  private static final String MIXED_TABLE = "Mixed";
  private static final String NON_NULL_TABLE = "NonNull";
  private static final String COMPOSITE_PRIMARY_KEY_TABLE = "CompositePrimaryKey";
  private static final String AUTO_NUMBER_TABLE = "AutoNumber";

  private static final String INNER_FIELD_B = "innerFieldB";
  private static final String INNER_FIELD_A = "innerFieldA";
  private static final String SECOND_PRIMARY_KEY = "secondPrimaryKey";
  private static final String FIELDA = "FIELDA";
  private static final String CLOB_FIELD = "clobField";
  private static final String BIG_INTEGER_FIELD = "bigIntegerField";
  private static final String BLOB_FIELD = "blobField";
  private static final String CHAR_FIELD = "charField";
  private static final String BOOLEAN_FIELD = "booleanField";
  private static final String DATE_FIELD = "dateField";
  private static final String FLOAT_FIELD = "floatField";
  private static final String INT_FIELD = "intField";
  private static final String STRING_FIELD = "stringField";
  private static final String DBLINK_NAME = "MYDBLINKREF";

  private static final String SEQUENCE_NAME = "TestSequence";

  protected static final String ID_VALUES_TABLE = "idvalues";
  protected static final String ID_INCREMENTOR_TABLE_COLUMN_VALUE = "nextvalue";

  private static final byte[] BYTE_ARRAY = new byte[] { 2, 1, (byte) 164, 3, 14, 4, 9, 0, 0, 0, 48, 111, 114, 103, 46, 105, 110, 102, 105,
    110, 105, 115, 112, 97, 110, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 67, 111, 110,
    99, 117, 114, 114, 101, 110, 116, 72, 97, 115, 104, 83, 101, 116, 73, (byte) 186, 42, 14, (byte) 206, 6, (byte) 195,
    (byte) 157, 0, 0, 0, 1, 0, 0, 0, 3, 109, 97, 112, 114, 0, 110, 4, 114, 0, 0, 0, 15, 0, 0, 0, 28, 66, 16, 9, 0, 0, 0, 46,
    106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 67, 111, 110, 99, 117,
    114, 114, 101, 110, 116, 72, 97, 115, 104, 77, 97, 112, 36, 83, 101, 103, 109, 101, 110, 116, 31, 54, 76, (byte) 144, 88,
    (byte) 147, 41, 61, 0, 0, 0, 1, 0, 0, 0, 10, 108, 111, 97, 100, 70, 97, 99, 116, 111, 114, 38, 0, 9, 0, 0, 0, 40, 106, 97,
    118, 97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 108, 111, 99, 107, 115, 46, 82,
    101, 101, 110, 116, 114, 97, 110, 116, 76, 111, 99, 107, 102, 85, (byte) 168, 44, 44, (byte) 200, 106, (byte) 235, 0, 0, 0,
    1, 0, 0, 0, 4, 115, 121, 110, 99, 9, 0, 0, 0, 45, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114,
    114, 101, 110, 116, 46, 108, 111, 99, 107, 115, 46, 82, 101, 101, 110, 116, 114, 97, 110, 116, 76, 111, 99, 107, 36, 83,
    121, 110, 99, (byte) 184, 30, (byte) 162, (byte) 148, (byte) 170, 68, 90, 124, 0, 0, 0, 0, 9, 0, 0, 0, 53, 106, 97, 118,
    97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 108, 111, 99, 107, 115, 46, 65, 98,
    115, 116, 114, 97, 99, 116, 81, 117, 101, 117, 101, 100, 83, 121, 110, 99, 104, 114, 111, 110, 105, 122, 101, 114, 102, 85,
    (byte) 168, 67, 117, 63, 82, (byte) 227, 0, 0, 0, 1, 0, 0, 0, 5, 115, 116, 97, 116, 101, 35, 0, 9, 0, 0, 0, 54, 106, 97,
    118, 97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 108, 111, 99, 107, 115, 46, 65,
    98, 115, 116, 114, 97, 99, 116, 79, 119, 110, 97, 98, 108, 101, 83, 121, 110, 99, 104, 114, 111, 110, 105, 122, 101, 114,
    51, (byte) 223, (byte) 175, (byte) 185, (byte) 173, 109, 111, (byte) 169, 0, 0, 0, 0, 22, 0, 22, 4, 59, (byte) 251, 4, 9,
    0, 0, 0, 52, 106, 97, 118, 97, 46, 117, 116, 105, 108, 46, 99, 111, 110, 99, 117, 114, 114, 101, 110, 116, 46, 108, 111,
    99, 107, 115, 46, 82, 101, 101, 110, 116, 114, 97, 110, 116, 76, 111, 99, 107, 36, 78, 111, 110, 102, 97, 105, 114, 83,
    121, 110, 99, 101, (byte) 136, 50, (byte) 231, 83, 123, (byte) 191, 11, 0, 0, 0, 0, 59, (byte) 252, 0, 0, 0, 0, 63, 64, 0,
    0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63,
    64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0,
    63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0,
    0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255,
    0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59,
    (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4,
    59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59,
    (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0, 4, 59, (byte) 250, 4, 59, (byte) 255, 0, 0, 0, 0, 63, 64, 0, 0,
    62, 6, 95, 48, 46, 116, 105, 105, 75, 0, 0, 0, 0, 62, 6, 95, 48, 46, 116, 105, 115, 75, 0, 0, 0, 0, 62, 6, 95, 48, 46, 110,
    114, 109, 75, 0, 0, 0, 0, 62, 12, 115, 101, 103, 109, 101, 110, 116, 115, 46, 103, 101, 110, 75, 0, 0, 0, 0, 62, 6, 95, 48,
    46, 112, 114, 120, 75, 0, 0, 0, 0, 62, 6, 95, 48, 46, 102, 100, 116, 75, 0, 0, 0, 0, 62, 6, 95, 48, 46, 102, 114, 113, 75,
    0, 0, 0, 0, 62, 6, 95, 48, 46, 102, 110, 109, 75, 0, 0, 0, 0, 62, 10, 115, 101, 103, 109, 101, 110, 116, 115, 95, 50, 75,
    0, 0, 0, 0, 62, 6, 95, 48, 46, 102, 100, 120, 75, 0, 0, 0, 0, 1, 1, 53 };

  private static final String BASE64_ENCODED = "AgGkAw4ECQAAADBvcmcuaW5maW5pc3Bhbi51dGlsLmNvbmN1cnJlbnQuQ29uY3VycmVudEhhc2hTZXRJuioOzgbDnQAAAAEAAAADbWFwcgBuBHIAAAAPAAAAH"
      + "EIQCQAAAC5qYXZhLnV0aWwuY29uY3VycmVudC5Db25jdXJyZW50SGFzaE1hcCRTZWdtZW50HzZMkFiTKT0AAAABAAAACmxvYWRGYWN0b3ImAAkAAAAoa"
      + "mF2YS51dGlsLmNvbmN1cnJlbnQubG9ja3MuUmVlbnRyYW50TG9ja2ZVqCwsyGrrAAAAAQAAAARzeW5jCQAAAC1qYXZhLnV0aWwuY29uY3VycmVudC5sb"
      + "2Nrcy5SZWVudHJhbnRMb2NrJFN5bmO4HqKUqkRafAAAAAAJAAAANWphdmEudXRpbC5jb25jdXJyZW50LmxvY2tzLkFic3RyYWN0UXVldWVkU3luY2hyb"
      + "25pemVyZlWoQ3U/UuMAAAABAAAABXN0YXRlIwAJAAAANmphdmEudXRpbC5jb25jdXJyZW50LmxvY2tzLkFic3RyYWN0T3duYWJsZVN5bmNocm9uaXplcj"
      + "Pfr7mtbW+pAAAAABYAFgQ7+wQJAAAANGphdmEudXRpbC5jb25jdXJyZW50LmxvY2tzLlJlZW50cmFudExvY2skTm9uZmFpclN5bmNliDLnU3u/CwAAAAA"
      + "7/AAAAAA/QAAABDv6BDv/AAAAAD9AAAAEO/oEO/8AAAAAP0AAAAQ7+gQ7/wAAAAA/QAAABDv6BDv/AAAAAD9AAAAEO/oEO/8AAAAAP0AAAAQ7+gQ7/wAA"
      + "AAA/QAAABDv6BDv/AAAAAD9AAAAEO/oEO/8AAAAAP0AAAAQ7+gQ7/wAAAAA/QAAABDv6BDv/AAAAAD9AAAAEO/oEO/8AAAAAP0AAAAQ7+gQ7/wAAAAA/"
      + "QAAABDv6BDv/AAAAAD9AAAAEO/oEO/8AAAAAP0AAAAQ7+gQ7/wAAAAA/QAAAPgZfMC50aWlLAAAAAD4GXzAudGlzSwAAAAA+Bl8wLm5ybUsAAAAAPgxz"
      + "ZWdtZW50cy5nZW5LAAAAAD4GXzAucHJ4SwAAAAA+Bl8wLmZkdEsAAAAAPgZfMC5mcnFLAAAAAD4GXzAuZm5tSwAAAAA+CnNlZ21lbnRzXzJLAAAAAD4GX"
      + "zAuZmR4SwAAAAABATU=";
  private static final String NEW_BLOB_VALUE = "New Blob Value";
  private static final String OLD_BLOB_VALUE = "Old Blob Value";
  protected static final String NEW_BLOB_VALUE_HEX = "4E657720426C6F622056616C7565";
  protected static final String OLD_BLOB_VALUE_HEX = "4F6C6420426C6F622056616C7565";

  private static final String LONG_FIELD_STRING = "CREATE VIEW viewName AS (SELECT tableField1, tableField2, tableField3, tableField4, tableField5, tableField6, tableField7, tableField8, tableField9, tableField10, tableField11, tableField12, tableField13, tableField14, tableField15, tableField16, tableField17, tableField18, tableField19, tableField20, tableField21, tableField22, tableField23, tableField24, tableField25, tableField26, tableField27, tableField28, tableField29, tableField30 FROM table INNER JOIN table2 ON (table1.tableField1 = table2 = tableField1));";


  /**
   * Exception verifier.
   */
  @Rule
  public ExpectedException exception = ExpectedException.none();


  /**
   * Metadata to use for tests
   */
  protected Schema metadata;
  private View testView;

  private View testViewWithUnion;

  private Sequence testSequence;

  /**
   * Very long table name to test name truncation.
   */
  public static final String TABLE_WITH_VERY_LONG_NAME = "tableWithANameThatExceeds30Char";
  /**
   * Dialect being tested.
   */
  protected SqlDialect testDialect;

  private Table testTable;

  private Table testTempTable;
  private Table nonNullTempTable;
  private Table alternateTestTempTable;

  protected final Connection connection = mock(Connection.class);
  protected final SqlScriptExecutor sqlScriptExecutor = mock(SqlScriptExecutor.class,Mockito.RETURNS_DEEP_STUBS);

  private static final long MAX_ID_UNDER_REPAIR_LIMIT = 999L;
  private static final long MAX_ID_OVER_REPAIR_LIMIT = 1000L;

  /**
   * Initialise the fixture state.
   */
  @Before
  public void setUp() {
    // Get the candidate dialect to test
    testDialect = createTestDialect();

    // Main test table
    testTable = table(TEST_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable(),
          column(INT_FIELD, DataType.INTEGER).nullable(),
          column(FLOAT_FIELD, DataType.DECIMAL, 13, 2),
          column(DATE_FIELD, DataType.DATE).nullable(),
          column(BOOLEAN_FIELD, DataType.BOOLEAN).nullable(),
          column(CHAR_FIELD, DataType.STRING, 1).nullable(),
          column(BLOB_FIELD, DataType.BLOB).nullable(),
          column(BIG_INTEGER_FIELD, DataType.BIG_INTEGER).nullable().defaultValue("12345"),
          column(CLOB_FIELD, DataType.CLOB).nullable()
            ).indexes(
              index(TEST_NK).unique().columns(STRING_FIELD),
              index(TEST_1).columns(INT_FIELD, FLOAT_FIELD).unique()
                );

    // Temporary version of the main test table
    testTempTable = table(testDialect.decorateTemporaryTableName(TEMP_TEST_TABLE)).temporary()
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable(),
          column(INT_FIELD, DataType.INTEGER).nullable(),
          column(FLOAT_FIELD, DataType.DECIMAL, 13, 2),
          column(DATE_FIELD, DataType.DATE).nullable(),
          column(BOOLEAN_FIELD, DataType.BOOLEAN).nullable(),
          column(CHAR_FIELD, DataType.STRING, 1).nullable(),
          column(BLOB_FIELD, DataType.BLOB).nullable(),
          column(BIG_INTEGER_FIELD, DataType.BIG_INTEGER).nullable().defaultValue("12345"),
          column(CLOB_FIELD, DataType.CLOB).nullable()
            ).indexes(
              index(TEMP_TEST_NK).unique().columns(STRING_FIELD),
              index(TEMP_TEST_1).columns(INT_FIELD, FLOAT_FIELD)
                );

    // Simple alternate test table
    Table alternateTestTable = table(ALTERNATE_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable()
            ).indexes(
              index("Alternate_1").columns(STRING_FIELD)
                );

    // Temporary version of the alternate test table
    alternateTestTempTable = table(testDialect.decorateTemporaryTableName("TempAlternate")).temporary()
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable()
            ).indexes(
              index("TempAlternate_1").columns(STRING_FIELD)
                );

    // Third test table
    Table otherTable = table(OTHER_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable(),
          column(INT_FIELD, DataType.DECIMAL, 8).nullable(),
          column(FLOAT_FIELD, DataType.DECIMAL, 13, 2)
            );

    // Test table with a very long name
    Table testTableLongName = table(TABLE_WITH_VERY_LONG_NAME)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3).nullable(),
          column(INT_FIELD, DataType.DECIMAL, 8).nullable(),
          column(FLOAT_FIELD, DataType.DECIMAL, 13, 2),
          column(DATE_FIELD, DataType.DATE).nullable(),
          column(BOOLEAN_FIELD, DataType.BOOLEAN).nullable(),
          column(CHAR_FIELD, DataType.STRING, 1).nullable()
            ).indexes(
              index(TEST_NK).unique().columns(STRING_FIELD),
              index(TEST_1).columns(INT_FIELD, FLOAT_FIELD)
                );

    Table testTableAllUpperCase = table(UPPER_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(FIELDA, DataType.STRING, 4)
            );

    Table testTableMixedCase = table(MIXED_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(FIELDA, DataType.STRING, 4)
            );

    // Test table with non null columns
    Table nonNullTable = table(NON_NULL_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3),
          column(INT_FIELD, DataType.DECIMAL, 8),
          column(BOOLEAN_FIELD, DataType.BOOLEAN),
          column(DATE_FIELD, DataType.DATE),
          column(BLOB_FIELD, DataType.BLOB)
            );

    // Temporary version of the test table with non null columns
    nonNullTempTable = table(testDialect.decorateTemporaryTableName("TempNonNull")).temporary()
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3),
          column(INT_FIELD, DataType.DECIMAL, 8),
          column(BOOLEAN_FIELD, DataType.BOOLEAN),
          column(DATE_FIELD, DataType.DATE),
          column(BLOB_FIELD, DataType.BLOB)
            );

    // Test table with composite primary key
    Table compositePrimaryKey = table(COMPOSITE_PRIMARY_KEY_TABLE)
        .columns(
          idColumn(),
          versionColumn(),
          column(STRING_FIELD, DataType.STRING, 3),
          column(SECOND_PRIMARY_KEY, DataType.STRING, 3).primaryKey()
            );

    // Test table with a database-supplied unique id
    Table autoNumber = table(AUTO_NUMBER_TABLE)
        .columns(
          autonumber(INT_FIELD, 5)
            );

    // Test view
    TableReference tr = new TableReference(TEST_TABLE);
    FieldReference f = new FieldReference(STRING_FIELD);
    testView = view("TestView", select(f).from(tr).where(eq(f, new FieldLiteral("blah"))));

    //Test sequence
    testSequence = sequence(SEQUENCE_NAME);

    TableReference tr1 = new TableReference(OTHER_TABLE);
    testViewWithUnion = view("TestView", select(f).from(tr).where(eq(f, new FieldLiteral("blah")))
      .unionAll(select(f).from(tr1).where(eq(f, new FieldLiteral("blah")))));

    Table inner = table("Inner")
        .columns(
          column(INNER_FIELD_A, DataType.STRING, 3),
          column(INNER_FIELD_B, DataType.STRING, 3)
            );

    Table insertAB = table("InsertAB")
        .columns(
          column(INNER_FIELD_A, DataType.STRING, 3),
          column(INNER_FIELD_B, DataType.STRING, 3)
            );

    Table insertA = table("InsertA")
        .columns(
          column(INNER_FIELD_A, DataType.STRING, 3)
            );

    // Builds a test schema
    metadata = schema(testTable, testTempTable, testTableLongName, alternateTestTable, alternateTestTempTable, otherTable,
      testTableAllUpperCase, testTableMixedCase, nonNullTable, nonNullTempTable, compositePrimaryKey, autoNumber,
      inner, insertAB, insertA);
  }

  /**
   * Test that ensures that no dialect specific tests exist. I.e. If there is an SQL dialect test
   * then it must be defined here so that all descendent dialects are obliged to support it.
   *
   * <p>This test exists to make sure we test all required SQL constructs on all supported platforms.
   * Previously there were lots of features only tested on MySql or Oracle.</p>
   *
   * <p>The desired format for new tests that have different results for different platforms is
   * to define a (possibly abstract) "expectedOutcome" method in this class such as {@link #expectedCreateTableStatements()}.</p>
   */
  @Test
  public void testDialectHasNoBespokeTests() {
    for (Method method : getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test") || method.getAnnotation(Test.class) != null) {
        fail("Descendents of " + AbstractSqlDialectTest.class.getSimpleName() + " must not define tests directly");
      }
    }
  }


  /**
   * Tests the SQL for creating tables.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateTableStatements() {
    Table table = metadata.getTable(TEST_TABLE);
    Table alternate = metadata.getTable(ALTERNATE_TABLE);
    Table nonNull = metadata.getTable(NON_NULL_TABLE);
    Table compositePrimaryKey = metadata.getTable(COMPOSITE_PRIMARY_KEY_TABLE);
    Table autoNumber = metadata.getTable(AUTO_NUMBER_TABLE);

    compareStatements(
      expectedCreateTableStatements(),
      testDialect.tableDeploymentStatements(table),
      testDialect.tableDeploymentStatements(alternate),
      testDialect.tableDeploymentStatements(nonNull),
      testDialect.tableDeploymentStatements(compositePrimaryKey),
      testDialect.tableDeploymentStatements(autoNumber)
    );
  }


  /**
   * Tests the SQL for creating views.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateViewStatements() {
    compareStatements(
      expectedCreateViewStatements(),
      testDialect.viewDeploymentStatements(testView));
  }


  /**
   * Tests the SQL for creating sequences.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateSequencesStatements() {
    compareStatements(
      expectedCreateSequenceStatements(),
      testDialect.sequenceDeploymentStatements(testSequence));
  }


  /**
   * Tests the SQL for creating temporary sequences.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateTemporarySequencesStatements() {
    testSequence = sequence(SEQUENCE_NAME).temporary();

    compareStatements(
      expectedCreateTemporarySequenceStatements(),
      testDialect.sequenceDeploymentStatements(testSequence));
  }


  /**
   * Tests the SQL for creating sequences when no explicit 'START WITH' value is specified.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateSequencesStatementWhenNoStartWithSpecified() {
    testSequence = sequence(SEQUENCE_NAME).startsWith(null);

    compareStatements(
      expectedCreateSequenceStatementsWithNoStartWith(),
      testDialect.sequenceDeploymentStatements(testSequence));
  }


  /**
   * Tests the SQL for creating temporary sequences when no explicit 'START WITH' value is specified.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateTemporarySequencesStatementWhenNoStartWithSpecified() {
    testSequence = sequence(SEQUENCE_NAME).startsWith(null).temporary();

    compareStatements(
      expectedCreateTemporarySequenceStatementsWithNoStartWith(),
      testDialect.sequenceDeploymentStatements(testSequence));
  }


  /**
   * Tests the SQL for returning the next value from a sequence.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetSqlFromSequenceReferenceWithNextValue() {
    // Given
    SequenceReference sequenceReference = new SequenceReference(testSequence.getName()).nextValue();
    SelectStatement stmt = new SelectStatement(sequenceReference);

    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals(expectedNextValForSequence(), result);
  }


  /**
   * Tests the SQL for returning the current value of a sequence.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetSqlFromSequenceReferenceWithCurrentValue() {
    // Given
    SequenceReference sequenceReference = new SequenceReference(testSequence.getName()).currentValue();
    SelectStatement stmt = new SelectStatement(sequenceReference);

    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals(expectedCurrValForSequence(), result);
  }


  /**
   * Tests the SQL for creating a view over a union select.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateViewStatementOverUnionSelect() {
    compareStatements(
      expectedCreateViewOverUnionSelectStatements(),
      testDialect.viewDeploymentStatements(testViewWithUnion));
  }


  /**
   * Tests the SQL for creating tables.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testTemporaryCreateTableStatements() {
    compareStatements(
      expectedCreateTemporaryTableStatements(),
      testDialect.tableDeploymentStatements(testTempTable),
      testDialect.tableDeploymentStatements(alternateTestTempTable),
      testDialect.tableDeploymentStatements(nonNullTempTable));
  }


  /**
   * Test the SQL for dropping temporary tables.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDropTemporaryTableStatements() {
    compareStatements(
      expectedDropTempTableStatements(),
      testDialect.dropStatements(testTempTable)
        );
  }


  /**
   * Tests the SQL for creating tables with long names
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testCreateTableStatementsLongTableName() {
    Table table = metadata.getTable(TABLE_WITH_VERY_LONG_NAME);

    compareStatements(
      expectedCreateTableStatementsWithLongTableName(),
      testDialect.tableDeploymentStatements(table)
    );
  }


  /**
   * Tests SQL for dropping a table.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDropTableStatements() {
    Table table = metadata.getTable(TEST_TABLE);

    compareStatements(
      expectedDropTableStatements(),
      testDialect.dropStatements(table));
  }


  /**
   * Tests SQL for dropping a tables with optional parameters.
   */
  @Test
  public void testDropTables() {
    Table table1 = metadata.getTable(TEST_TABLE);
    Table table2 = metadata.getTable(OTHER_TABLE);

    compareStatements(
        expectedDropSingleTable(),
        testDialect.dropTables(ImmutableList.of(table1), false, false)
    );

    compareStatements(
        expectedDropTables(),
        testDialect.dropTables(ImmutableList.of(table1, table2), false, false));

    compareStatements(
        expectedDropTablesWithParameters(),
        testDialect.dropTables(ImmutableList.of(table1, table2), true, true));
  }


  /**
   * Tests SQL for selecting literal fields where is a WHERE clause
   */
  @Test
  public void testSelectLiteralWithWhereClause() {
    assertEquals(
      expectedSelectLiteralWithWhereClauseString(),
      testDialect.convertStatementToSQL(
        new SelectStatement(new FieldLiteral("LITERAL")).where(eq(new FieldLiteral("ONE"), "ONE"))
      )
    );
  }


  /**
   * Tests SQL for dropping a view.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDropViewStatements() {
    compareStatements(
      expectedDropViewStatements(),
      testDialect.dropStatements(testView));
  }


  /**
   * Tests SQL for dropping a view.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDropSequenceStatements() {
    compareStatements(
      expectedDropSequenceStatements(),
      testDialect.dropStatements(testSequence));
  }


  /**
   * Tests SQL for clearing tables.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateTableStatements() {
    Table table = metadata.getTable(TEST_TABLE);

    compareStatements(
      expectedTruncateTableStatements(),
      testDialect.truncateTableStatements(table));
  }


  /**
   * Tests SQL for clearing tables.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testTruncateTemporaryTableStatements() {
    compareStatements(
      expectedTruncateTempTableStatements(),
      testDialect.truncateTableStatements(testTempTable)
        );
  }


  /**
   * Tests SQL for clearing table with a delete.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDeleteAllFromTableStatements() {
    Table table = metadata.getTable(TEST_TABLE);

    compareStatements(
      expectedDeleteAllFromTableStatements(),
      testDialect.deleteAllFromTableStatements(table));
  }


  /**
   * Tests the SQL for a simple select statement.
   */
  @Test
  public void testSelectAllRecords() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE));
    assertEquals("SQL to select all records", "SELECT * FROM " + tableName(TEST_TABLE), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests the SQL for select for update
   */
  @Test
  public void testSelectForUpdate() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE)).forUpdate();
    assertEquals("SQL to select for update", "SELECT * FROM " + tableName(TEST_TABLE) + expectedForUpdate(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * @return How the dialect should represent a FOR UPDATE.
   */
  protected String expectedForUpdate() {
    return " FOR UPDATE";
  }


  /**
   * Tests that we can't combine DISTINCT with FOR UPDATE
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSelectDistinctForUpdate() {
    SelectStatement stmt = selectDistinct().from(new TableReference(TEST_TABLE)).forUpdate();
    testDialect.convertStatementToSQL(stmt);
  }


  /**
   * Tests that we can't combine GROUP BY with FOR UPDATE
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSelectGroupForUpdate() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE)).groupBy(field("x")).forUpdate();
    testDialect.convertStatementToSQL(stmt);
  }


  /**
   * Tests that we can't combine JOIN with FOR UPDATE
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSelectWithJoinForUpdate() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE)).crossJoin(new TableReference("Test2")).forUpdate();
    testDialect.convertStatementToSQL(stmt);
  }


  /**
   * Test that {@link SqlDialect#convertStatementToHash(SelectStatement)} works.
   */
  @Test
  public void testSelectHash() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE));
    String hash = testDialect.convertStatementToHash(stmt);
    assertFalse("Valid", StringUtils.isBlank(hash));
  }


  /**
   * Tests a simple select with fields specified
   */
  @Test
  public void testSelectSpecificFields() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(DATE_FIELD).as("aliasDate"))
    .from(new TableReference(TEST_TABLE));

    String expectedSql = "SELECT stringField, intField, dateField AS aliasDate FROM " + tableName(TEST_TABLE);
    assertEquals("Select specific fields", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with fields qualified with table specifiers.
   */
  @Test
  public void testSelectWithQualifiedFieldNames() {
    SelectStatement stmt = new SelectStatement(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD),
      new FieldReference(new TableReference(TEST_TABLE), INT_FIELD),
      new FieldReference(new TableReference(TEST_TABLE), DATE_FIELD).as("aliasDate"))
    .from(new TableReference(TEST_TABLE));

    String expectedSql = "SELECT Test.stringField, Test.intField, Test.dateField AS aliasDate FROM " + tableName(TEST_TABLE);
    assertEquals("Select statement with qualified field names", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with table aliases.
   */
  @Test
  public void testSelectWithTableAlias() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(DATE_FIELD).as("aliasDate"))
    .from(new TableReference(TEST_TABLE).as("aliasTest"));


    String expectedSql = "SELECT stringField, intField, dateField AS aliasDate FROM " + tableName(TEST_TABLE) + " aliasTest";
    assertEquals("Select statement with qualified field names", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with table aliases.
   */
  @Test
  public void testSelectWithMultipleTableAlias() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(DATE_FIELD).as("aliasDate"))
    .from(new TableReference(TEST_TABLE).as("T"))
    .innerJoin(new TableReference(ALTERNATE_TABLE).as("A"), eq(new FieldReference(new TableReference("T"), STRING_FIELD), new FieldReference(new TableReference("A"), STRING_FIELD)));

    String expectedSql = "SELECT stringField, intField, dateField AS aliasDate FROM " + tableName(TEST_TABLE) + " T INNER JOIN " + tableName(ALTERNATE_TABLE) + " A ON (T.stringField = A.stringField)";
    assertEquals("Select scripts are not the same", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a where clause.
   */
  @Test
  public void testSelectWhereScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(eq(new FieldReference(STRING_FIELD), "A0001"));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() +value+")";
    assertEquals("Select scripts are not the same", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a nested "or where" clause.
   */
  @Test
  public void testSelectOr() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(or(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          greaterThan(new FieldReference(INT_FIELD), 20080101)
        ));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() + value+") OR (intField > 20080101))";
    assertEquals("Select with nested or", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a nested "or where" clause.
   */
  @Test
  public void testSelectOrList() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(or(ImmutableList.of(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          greaterThan(new FieldReference(INT_FIELD), 20080101)
        )));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() + value+") OR (intField > 20080101))";
    assertEquals("Select with nested or", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a nested "and where" clause.
   */
  @Test
  public void testSelectAnd() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(and(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          greaterThan(new FieldReference(INT_FIELD), 20080101)
        ));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() +value+") AND (intField > 20080101))";
    assertEquals("Select with multiple where clauses", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a nested "and where" clause.
   */
  @Test
  public void testSelectAndList() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(and(ImmutableList.of(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          greaterThan(new FieldReference(INT_FIELD), 20080101)
        )));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() +value+") AND (intField > 20080101))";
    assertEquals("Select with multiple where clauses", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a "not" where clause.
   */
  @Test
  public void testSelectNotWhereScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(not(
          eq(new FieldReference(STRING_FIELD), "A0001")
            ));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (NOT (stringField = " + stringLiteralPrefix() + value+"))";
    assertEquals("Select using a where not clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with multiple where clauses
   */
  @Test
  public void testSelectMultipleWhereScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(and(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          greaterThan(new FieldReference(INT_FIELD), 20080101),
          lessThan(new FieldReference(DATE_FIELD), 20090101)
            ));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() + value+") AND (intField > 20080101) AND (dateField < 20090101))";
    assertEquals("Select with multiple where clauses", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with multiple nested where clauses.
   */
  @Test
  public void testSelectMultipleNestedWhereScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(and(
          eq(new FieldReference(STRING_FIELD), "A0001"),
          or(
            greaterThan(new FieldReference(INT_FIELD), 20080101),
            lessThan(new FieldReference(DATE_FIELD), 20090101)
              )
            ));

    String value = varCharCast("'A0001'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE ((stringField = " + stringLiteralPrefix() + value+") AND ((intField > 20080101) OR (dateField < 20090101)))";
    assertEquals("Select with nested where clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a simple join.
   */
  @Test
  public void testSelectSimpleJoinScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .innerJoin(new TableReference(ALTERNATE_TABLE),
          eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD),
            new FieldReference(new TableReference(ALTERNATE_TABLE), STRING_FIELD))
            );

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " INNER JOIN " + tableName(ALTERNATE_TABLE) + " ON (Test.stringField = Alternate.stringField)";

    assertEquals("Select with simple join", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with multiple joins.
   */
  @Test
  public void testSelectMultipleJoinScript() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(ALTERNATE_TABLE))
        .innerJoin(new TableReference(TEST_TABLE),
          eq(new FieldReference(new TableReference(ALTERNATE_TABLE), STRING_FIELD),
            new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD))
            )
        .leftOuterJoin(new TableReference(OTHER_TABLE),
              and(
                eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD),
                  new FieldReference(new TableReference(OTHER_TABLE), STRING_FIELD)),
                  eq(new FieldReference(new TableReference(TEST_TABLE), INT_FIELD),
                    new FieldReference(new TableReference(OTHER_TABLE), INT_FIELD))
                  )
                )
        .fullOuterJoin(new TableReference(MIXED_TABLE),
          and(
            eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD),
              new FieldReference(new TableReference(MIXED_TABLE), STRING_FIELD)),
              eq(new FieldReference(new TableReference(TEST_TABLE), INT_FIELD),
                new FieldReference(new TableReference(MIXED_TABLE), INT_FIELD))
              )
            );

    String expectedSql = "SELECT * FROM " + tableName(ALTERNATE_TABLE)
        + " INNER JOIN " + tableName(TEST_TABLE) + " ON (Alternate.stringField = Test.stringField)"
        + " LEFT OUTER JOIN " + tableName(OTHER_TABLE) + " ON ((Test.stringField = Other.stringField) AND (Test.intField = Other.intField))"
        + " FULL OUTER JOIN " + tableName(MIXED_TABLE) + " ON ((Test.stringField = Mixed.stringField) AND (Test.intField = Mixed.intField))"
        ;
    assertEquals("Select with multiple joins", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a "having" clause.
   */
  @Test
  public void testSelectHavingScript() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(ALTERNATE_TABLE))
    .groupBy(new FieldReference(STRING_FIELD))
    .having(eq(new FieldReference("blah"), "X"));

    String value = varCharCast("'X'");
    String expectedSql = "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " GROUP BY stringField HAVING (blah = " + stringLiteralPrefix() + value+")";
    assertEquals("Select with having clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an "order by" clause.
   */
  @Test
  public void testSelectOrderByScript() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(new FieldReference(STRING_FIELD));

    String expectedSql = "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField";
    if (!nullOrder().equals(StringUtils.EMPTY)) {
      expectedSql = expectedSql + " " + nullOrder();
    }
    assertEquals("Select with order by", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a descending "order by" clause.
   */
  @Test
  public void testSelectOrderByDescendingScript() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(new FieldReference(STRING_FIELD, Direction.DESCENDING));

    String expectedSql = "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField DESC";
    if (!nullOrder().equals(StringUtils.EMPTY)) {
      expectedSql = expectedSql + " " + nullOrderForDirection(Direction.DESCENDING);
    }
    assertEquals("Select with descending order by", expectedSql, testDialect.convertStatementToSQL(stmt));
  }

  /**
   * Tests a select with an "order by" clause with nulls last and default direction.
   */
  @Test
  public void testSelectOrderByNullsLastScript() {
    FieldReference fieldReference = new FieldReference(STRING_FIELD);
    SelectStatement stmt = new SelectStatement(fieldReference)
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(fieldReference.nullsLast());

    assertEquals("Select with order by", expectedSelectOrderByNullsLast(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an "order by" clause with nulls first and descending direction.
   */
  @Test
  public void testSelectOrderByNullsFirstDescendingScript() {
    FieldReference fieldReference = new FieldReference(STRING_FIELD);
    SelectStatement stmt = new SelectStatement(fieldReference)
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(fieldReference.desc().nullsFirst());

    assertEquals("Select with descending order by", expectedSelectOrderByNullsFirstDesc(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an "order by" clause with nulls last and default direction.
   */
  @Test
  public void testSelectOrderByWithNoExplicitNullHandling() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(new FieldReference(STRING_FIELD).noNullHandling());

    String expectedSql = "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField";

    assertEquals("Select with order by", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an "order by" clause with two fields.
   */
  @Test
  public void testSelectOrderByTwoFields() {
    FieldReference fieldReference1 = new FieldReference("stringField1");
    FieldReference fieldReference2 = new FieldReference("stringField2");
    SelectStatement stmt = new SelectStatement(fieldReference1,fieldReference2)
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(fieldReference1.desc().nullsFirst(),fieldReference2.asc().nullsLast());

    assertEquals("Select with descending order by", expectedSelectOrderByTwoFields(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an "order by" clause with nulls first and descending direction.
   */
  @Test
  public void testSelectFirstOrderByNullsLastDescendingScript() {
    FieldReference fieldReference = new FieldReference(STRING_FIELD);
    SelectFirstStatement stmt = selectFirst(fieldReference)
    .from(new TableReference(ALTERNATE_TABLE))
    .orderBy(fieldReference.desc().nullsLast());

    assertEquals("Select with descending order by", expectedSelectFirstOrderByNullsLastDesc(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests case clauses in a select statement.
   */
  @Test
  public void testCaseSelect() {
    WhenCondition whenCondition  = new WhenCondition(
      eq(new FieldReference(CHAR_FIELD),  new FieldLiteral('Y')),
      new FieldReference(INT_FIELD));

    SelectStatement stmt = new SelectStatement(
      new FieldReference(STRING_FIELD),
      new FieldReference(BOOLEAN_FIELD),
      new FieldReference(CHAR_FIELD) ,
      new CaseStatement(new FieldReference(FLOAT_FIELD), whenCondition))
    .from(new TableReference(TEST_TABLE));

    String value = varCharCast("'Y'");
    String expectedSql = "SELECT stringField, booleanField, charField, CASE WHEN (charField = " + stringLiteralPrefix() + value +") THEN intField ELSE floatField END FROM " + tableName(TEST_TABLE);
    assertEquals("Select with case statement", expectedSql, testDialect.convertStatementToSQL(stmt));
  }



  /**
   * Test an update statement with case in it.
   */
  @Test
  public void testCaseWithStrings() {

    CaseStatement enabledWhenAutoRunIsT =
        new CaseStatement(new FieldLiteral("DISABLED"),
          new WhenCondition(eq(new FieldReference("autorunBackgroundProcess"),
            new FieldLiteral("Y")),
            new FieldLiteral("ENABLED")));

    UpdateStatement stmt =
        new UpdateStatement(new TableReference("BackgroundProcess"))
    .set(enabledWhenAutoRunIsT.as("targetState"));

    String value1 = varCharCast("'Y'");
    String value2 = varCharCast("'ENABLED'");
    String value3 = varCharCast("'DISABLED'");
    assertEquals("Update with case statement",
      "UPDATE " + tableName("BackgroundProcess") + " SET targetState = CASE WHEN (autorunBackgroundProcess = " + stringLiteralPrefix() + value1 +") THEN " + stringLiteralPrefix() + value2 + " ELSE " + stringLiteralPrefix() + value3 + " END",
      testDialect.convertStatementToSQL(stmt));
  }



  /**
   * Tests a select with a simple where clause.
   */
  @Test
  public void testSelectWithLessThanWhereClauses() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(lessThan(new FieldReference(INT_FIELD), 20090101));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField < 20090101)";
    assertEquals("Select with less than where clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a where like clause.
   */
  @Test
  public void testSelectWithLikeClause() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(like(new FieldReference(STRING_FIELD), "A%"));

    String value = varCharCast("'A%'");
    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (stringField LIKE " + stringLiteralPrefix() + value + likeEscapeSuffix() +")";
    assertEquals("Select with a like clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a simple where clause.
   */
  @Test
  public void testSelectWithWhereLessThanOrEqualTo() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(lessThanOrEqualTo(new FieldReference(INT_FIELD), 20090101));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField <= 20090101)";
    assertEquals("Select with less or equal clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a greater than clause.
   */
  @Test
  public void testSelectWhereGreaterThan() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(greaterThan(new FieldReference(INT_FIELD), 20090101));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField > 20090101)";
    assertEquals("Select with greater than clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a greater than or equals to clause.
   */
  @Test
  public void testSelectWithGreaterThanOrEqualToClause() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(greaterThanOrEqualTo(new FieldReference(INT_FIELD), 20090101));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField >= 20090101)";
    assertEquals("Select with greater than or equal to clause", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with a null check clause.
   */
  @Test
  public void testSelectWhereIsNull() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(isNull(new FieldReference(INT_FIELD)));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField IS NULL)";
    assertEquals("Select with null check clause", expectedSql, testDialect.convertStatementToSQL(stmt));

  }


  /**
   * Tests a select with a not null check clause.
   */
  @Test
  public void testSelectWhereIsNotNull() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(isNotNull(new FieldReference(INT_FIELD)));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField IS NOT NULL)";
    assertEquals("Select with not null clause", expectedSql, testDialect.convertStatementToSQL(stmt));

  }


  /**
   * Tests a select an exists check.
   */
  @Test
  public void testSelectWhereExists() {
    SelectStatement existsStatement = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(isNotNull(new FieldReference(INT_FIELD)));

    SelectStatement stmt = new SelectStatement().from(new TableReference(ALTERNATE_TABLE))
        .where(exists(existsStatement));

    String expectedSql = "SELECT * FROM " + tableName(ALTERNATE_TABLE) + " WHERE (EXISTS (SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField IS NOT NULL)))";
    assertEquals("Select with exists check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a select with an IN operator against a sub-query.
   */
  @Test
  public void testSelectWhereInSubquery() {
    SelectStatement inStatement = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(TEST_TABLE))
    .where(isNotNull(new FieldReference(INT_FIELD)));

    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(ALTERNATE_TABLE))
    .where(in(new FieldReference(STRING_FIELD), inStatement));

    String expectedSql = "SELECT * FROM " + tableName(ALTERNATE_TABLE) + " WHERE (stringField IN (SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (intField IS NOT NULL)))";
    assertEquals("Select with exists check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  @Test
  public void testSelectWhereInIntegerList() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(ALTERNATE_TABLE))
    .where(in(new FieldReference(STRING_FIELD), 1, 2, 3));

    String expectedSql = "SELECT * FROM " + tableName(ALTERNATE_TABLE) + " WHERE (stringField IN (1, 2, 3))";
    assertEquals("Select with exists check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  @Test
  public void testSelectWhereInFunctionList() {
    SelectStatement stmt = new SelectStatement()
    .from(new TableReference(ALTERNATE_TABLE))
    .where(in(new FieldReference(STRING_FIELD), sum(field("one")), sum(field("two"))));

    String expectedSql = "SELECT * FROM " + tableName(ALTERNATE_TABLE) + " WHERE (stringField IN (SUM(one), SUM(two)))";
    assertEquals("Select with exists check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests if a select with an IN operator and a sub-select with more than one
   * selected field throws an exception.
   */
  @Test
  public void testSelectWhereInSubqueryWithMoreThanOneField() {
    SelectStatement inStatement = new SelectStatement(new FieldReference(STRING_FIELD), new FieldReference(BOOLEAN_FIELD))
    .from(new TableReference(TEST_TABLE))
    .where(isNotNull(new FieldReference(INT_FIELD)));

    exception.expect(IllegalArgumentException.class);

    new SelectStatement()
    .from(new TableReference(ALTERNATE_TABLE))
    .where(in(new FieldReference(STRING_FIELD), inStatement));
  }


  /**
   * Tests if a select with an IN operator and a sub-select containing all (*)
   * fields from sub-table throws an exception.
   */
  @Test
  public void testSelectWhereInSubqueryWithAllFields() {
    SelectStatement inStatement = new SelectStatement()
    .from(new TableReference(TEST_TABLE))
    .where(isNotNull(new FieldReference(INT_FIELD)));

    exception.expect(IllegalArgumentException.class);

    new SelectStatement()
    .from(new TableReference(ALTERNATE_TABLE))
    .where(in(new FieldReference(STRING_FIELD), inStatement));
  }


  /**
   * Tests a select with not equals check.
   */
  @Test
  public void testSelectWhereNotEqualTo() {
    SelectStatement stmt = new SelectStatement().from(new TableReference(TEST_TABLE))
        .where(neq(new FieldReference(INT_FIELD), 20090101));

    String expectedSql = "SELECT * FROM " + tableName(TEST_TABLE) + " WHERE (intField <> 20090101)";
    assertEquals("Select with not equals check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests the count function in a select.
   */
  @Test
  public void testSelectWithCountFunction() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD), count())
    .from(new TableReference(ALTERNATE_TABLE))
    .groupBy(new FieldReference(STRING_FIELD));

    String expectedSql = "SELECT stringField, COUNT(*) FROM " + tableName(ALTERNATE_TABLE) + " GROUP BY stringField";
    assertEquals("Select with count function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
  * Test the row number function in a select.
  */
  @Test
  public void testSelectWithRowNumberFunction() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD), rowNumber())
            .from(new TableReference(ALTERNATE_TABLE))
            .groupBy(new FieldReference(STRING_FIELD));

    String expectedSql = "SELECT stringField, " + expectedRowNumber() + " FROM " + tableName(ALTERNATE_TABLE) + " GROUP BY stringField";
    assertEquals("Select with row number function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests the group by in a select.
   */
  @Test
  public void testSelectWithGroupBy() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD), count(literal(1)), countDistinct(literal(1)))
    .from(new TableReference(ALTERNATE_TABLE))
    .groupBy(field(STRING_FIELD), field(INT_FIELD), field(FLOAT_FIELD));

    String expectedSql = "SELECT stringField, COUNT(1), COUNT(DISTINCT 1) FROM " + tableName(ALTERNATE_TABLE) + " GROUP BY stringField, intField, floatField";
    assertEquals("Select with count function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests the group by in a select.
   */
  @Test
  public void testSelectWithGroupByList() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD), count(field(STRING_FIELD)), countDistinct(field(STRING_FIELD)))
    .from(new TableReference(ALTERNATE_TABLE))
    .groupBy(ImmutableList.of(field(STRING_FIELD), field(INT_FIELD), field(FLOAT_FIELD)));

    String expectedSql = "SELECT stringField, COUNT(stringField), COUNT(DISTINCT stringField) FROM " + tableName(ALTERNATE_TABLE) + " GROUP BY stringField, intField, floatField";
    assertEquals("Select with count function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Test the use of the sum function in a select
   */
  @Test
  public void testSelectWithSum() {
    SelectStatement stmt = new SelectStatement(sum(new FieldReference(INT_FIELD)), sumDistinct(new FieldReference(INT_FIELD))).from(new TableReference(TEST_TABLE));
    String expectedSql = "SELECT SUM(intField), SUM(DISTINCT intField) FROM " + tableName(TEST_TABLE);
    assertEquals("Select with sum function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests use of a minimum function in a select.
   */
  @Test
  public void testSelectMinimum() {
    SelectStatement stmt = new SelectStatement(min(new FieldReference(INT_FIELD))).from(new TableReference(TEST_TABLE));
    String expectedSql = "SELECT MIN(intField) FROM " + tableName(TEST_TABLE);
    assertEquals("Select with minimum function", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests select statement with maximum function.
   */
  @Test
  public void testSelectMaximum() {
    SelectStatement stmt = new SelectStatement(max(new FieldReference(INT_FIELD))).from(new TableReference(TEST_TABLE));
    String expectedSql = "SELECT MAX(intField) FROM " + tableName(TEST_TABLE);
    assertEquals("Select scripts are not the same", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests select statement with maximum function using more than a simple field.
   */
  @Test
  public void testSelectMaximumWithExpression() {
    SelectStatement stmt = select(max(field(INT_FIELD).plus(literal(1)))).from(tableRef(TEST_TABLE));
    assertEquals("Select scripts are not the same", expectedSelectMaximumWithExpression(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * @return The decimal representation of a literal for testing
   */
  protected String expectedSelectMaximumWithExpression() {
    return "SELECT MAX(intField + 1) FROM " + tableName(TEST_TABLE);
  }


  /**
   * Tests select statement with minimum function using more than a simple field.
   */
  @Test
  public void testSelectMinimumWithExpression() {
    SelectStatement stmt = select(min(field(INT_FIELD).minus(literal(1)))).from(tableRef(TEST_TABLE));
    assertEquals("Select scripts are not the same", expectedSelectMinimumWithExpression(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * @return the decimal representation of a literal for testing
   */
  protected String expectedSelectMinimumWithExpression() {
    return "SELECT MIN(intField - 1) FROM " + tableName(TEST_TABLE);
  }


  /**
   * Tests select statement with Some function.
   */
  @Test
  public void testSelectSome() {
    SelectStatement statement = select(some(field(BOOLEAN_FIELD))).from(tableRef(TEST_TABLE));
    assertEquals("Select scripts are not the same", expectedSelectSome(), testDialect.convertStatementToSQL(statement));
  }


  /**
  * Tests select statement with Every function.
  */
 @Test
 public void testSelectEvery() {
   SelectStatement statement = select(every(field(BOOLEAN_FIELD))).from(tableRef(TEST_TABLE));
   assertEquals("Select scripts are not the same", expectedSelectEvery(), testDialect.convertStatementToSQL(statement));
 }


  /**
   * Tests select statement with SUM function using more than a simple field.
   */
  @Test
  public void testSelectSumWithExpression() {
    SelectStatement stmt = select(sum(field(INT_FIELD).multiplyBy(literal(2)).divideBy(literal(3)))).from(tableRef(TEST_TABLE));
    assertEquals("Select scripts are not the same", expectedSelectSumWithExpression(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * @return the decimal representation of a literal for testing
   */
  protected String expectedSelectSumWithExpression() {
    return "SELECT SUM(intField * 2 / 3) FROM " + tableName(TEST_TABLE);
  }


  /**
   * Tests select statement with mod function.
   */
  @Test
  public void testSelectMod() {
    SelectStatement stmt = new SelectStatement(mod(new FieldReference(INT_FIELD), new FieldLiteral(5))).from(new TableReference(TEST_TABLE));
    String expectedSql = expectedSelectModSQL();
    assertEquals("Select scripts are not the same", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that strange equality behaviour is maintained.
   */
  @Test
  public void testSelectWithNestedEqualityCheck() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(TEST_TABLE))
    .where(eq(new FieldReference(BOOLEAN_FIELD), eq(new FieldReference(CHAR_FIELD), "Y")));

    String value = varCharCast("'Y'");
    String expectedSql = "SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (booleanField = (charField = " + stringLiteralPrefix() + value + "))";
    assertEquals("Select with nested equality check", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that selects which include field literals.
   */
  @Test
  public void testSelectWithLiterals() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(DATE_FIELD).as("aliasDate"),
      new FieldLiteral("SOME_STRING"),
      new FieldLiteral(1.23d),
      new FieldLiteral(1),
      new FieldLiteral('c'),
      new FieldLiteral("ANOTHER_STRING").as("aliasedString"))
    .from(new TableReference(TEST_TABLE));

    String value1 = varCharCast("'SOME_STRING'");
    String value2 = varCharCast("'c'");
    String value3 = varCharCast("'ANOTHER_STRING'");
    String expectedSql = "SELECT stringField, intField, dateField AS aliasDate, " + stringLiteralPrefix() + value1 + ", " + expectedDecimalRepresentationOfLiteral("1.23") + ", 1, " + stringLiteralPrefix() + value2 + ", " + stringLiteralPrefix() + value3 + " AS aliasedString FROM " + tableName(TEST_TABLE);
    assertEquals("Select with literal values for some fields", expectedSql, testDialect.convertStatementToSQL(stmt));
  }

  /**
   * @param literal The literal whose decimal representation will be returned
   * @return the decimal representation of a literal for testing
   */
  protected String expectedDecimalRepresentationOfLiteral(String literal) {
    return literal;
  }


  /**
   * Tests that an unparameterised insert where field values have been supplied
   * via a list of {@link FieldLiteral}s results in the field literals' values
   * being used as the inserted values.
   *
   * <p>By way of a regression test, this test omits some {@linkplain FieldLiteral}s
   * from its 'fields' array (namely 'charField', 'decimalField' and the internal 'version').
   * It checks that the value for these in the resulting sql statement's 'VALUE' part are
   * '?' as it won't know what to substitute for these.</p>
   */
  @Test
  public void testParameterisedInsert() {
    AliasedField[] fields = new AliasedField[] {
      new FieldLiteral(5).as("id"),
      new FieldLiteral("Escap'd").as(STRING_FIELD),
      new FieldLiteral(20100405).as(DATE_FIELD),
      new FieldLiteral(7).as(INT_FIELD),
      new FieldLiteral(true).as(BOOLEAN_FIELD),
    };

    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE)).fields(fields);
    String sql = testDialect.convertStatementToSQL(stmt, metadata);
    assertEquals("Generated SQL not as expected", expectedParameterisedInsertStatement(), sql);
  }


  /**
   * Same as {@link #testParameterisedInsert()}, but this also checks when the table is in a separate schema.
   */
  @Test
  public void testParameterisedInsertWithTableInDifferentSchema() {
    AliasedField[] fields = new AliasedField[] {
      new FieldLiteral(5).as("id"),
      new FieldLiteral("Escap'd").as(STRING_FIELD),
      new FieldLiteral(20100405).as(DATE_FIELD),
      new FieldLiteral(7).as(INT_FIELD),
      new FieldLiteral(true).as(BOOLEAN_FIELD),
    };

    InsertStatement stmt = new InsertStatement().into(new TableReference("MYSCHEMA", TEST_TABLE)).fields(fields);
    String sql = testDialect.convertStatementToSQL(stmt, metadata);
    assertEquals("Generated SQL not as expected", expectedParameterisedInsertStatementWithTableInDifferentSchema(), sql);
  }


  /**
   * Tests a parameterised insert with no specified field values specified.
   */
  @Test
  public void testParameterisedInsertWithNoFieldsSpecified() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE));
    String sql = testDialect.convertStatementToSQL(stmt, metadata);
    assertEquals("Generated SQL not as expected", expectedParameterisedInsertStatementWithNoColumnValues(), sql);
  }


  /**
   * Tests an insert statement where the value for each column (except the id) has been explicitly specified,
   */
  @Test
  public void testSpecifiedValueInsert() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE)).values(
      new FieldLiteral("Escap'd").as(STRING_FIELD),
      new FieldLiteral(7).as(INT_FIELD),
      new FieldLiteral(11.25).as(FLOAT_FIELD),
      new FieldLiteral(20100405).as(DATE_FIELD),
      new FieldLiteral(true).as(BOOLEAN_FIELD),
      new FieldLiteral('X').as(CHAR_FIELD)
        );
    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertSQLEquals("Generated SQL not as expected", expectedSpecifiedValueInsert(), sql);
  }


  /**
   * Tests an insert statement where the value for each column (except the id) has been explicitly specified,
   */
  @Test
  public void testSpecifiedValueInsertWithTableInDifferentSchema() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("MYSCHEMA", TEST_TABLE)).values(
      new FieldLiteral("Escap'd").as(STRING_FIELD),
      new FieldLiteral(7).as(INT_FIELD),
      new FieldLiteral(11.25).as(FLOAT_FIELD),
      new FieldLiteral(20100405).as(DATE_FIELD),
      new FieldLiteral(true).as(BOOLEAN_FIELD),
      new FieldLiteral('X').as(CHAR_FIELD)
    );

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertSQLEquals("Generated SQL not as expected", expectedSpecifiedValueInsertWithTableInDifferentSchema(), sql);
  }


  /**
   * Tests that an insert from a select works when some of the defaults are supplied
   */
  @Test
  public void testInsertFromSelectWithSomeDefaults() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE)).from(new TableReference(OTHER_TABLE)).withDefaults(new FieldLiteral(20010101).as(DATE_FIELD), new FieldLiteral(0).as(BOOLEAN_FIELD), new NullFieldLiteral().as(BLOB_FIELD));
    // FIXME The default of '' for a charField is WRONG. This should probably be one of NULL or ' '. Not an empty string, which is an invalid character!
    String expectedSql = "INSERT INTO " + tableName(TEST_TABLE) + " (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) SELECT id, version, stringField, intField, floatField, 20010101 AS dateField, 0 AS booleanField, NULL AS charField, null AS blobField, 12345 AS bigIntegerField, null AS clobField FROM " + tableName(OTHER_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert from select statement with some defaults", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Tests that an insert from a select works when no defaults are supplied.
   */
  @Test
  public void testInsertWithAutoGeneratedId() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("version"),
      new FieldReference(STRING_FIELD))
    .from(new TableReference(OTHER_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE))
        .fields(new FieldReference("version"),
          new FieldReference(STRING_FIELD))
          .from(sourceStmt);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertSQLEquals("Insert from a select with no default for id", expectedAutoGenerateIdStatement(), sql);
  }


  /**
   * Tests that an insert from a select works when no defaults are supplied for the id or version columns.
   */
  @Test
  public void testInsertWithIdAndVersion() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(OTHER_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE))
        .fields(new FieldReference(STRING_FIELD))
        .from(sourceStmt);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertSQLEquals("Insert from a select with no default for id", expectedInsertWithIdAndVersion(), sql);
  }


  /**
   * Test that an Insert statement is generated with a null value
   */
  @Test
  public void testInsertWithNullDefaults() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE))
        .from(new TableReference(OTHER_TABLE)).withDefaults(
          new NullFieldLiteral().as(DATE_FIELD),
          new NullFieldLiteral().as(BOOLEAN_FIELD),
          new NullFieldLiteral().as(CHAR_FIELD),
          new NullFieldLiteral().as(BLOB_FIELD)
            );

    String expectedSql = "INSERT INTO " + tableName(TEST_TABLE) + " (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) SELECT id, version, stringField, intField, floatField, null AS dateField, null AS booleanField, null AS charField, null AS blobField, 12345 AS bigIntegerField, null AS clobField FROM " + tableName(OTHER_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with null defaults", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Test that an insert is generated with a single space as the default
   * value for a character field.
   */
  @Test
  public void testInsertWithNonNullDefault() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE))
        .from(new TableReference(OTHER_TABLE)).withDefaults(
          new NullFieldLiteral().as(DATE_FIELD),
          new NullFieldLiteral().as(BOOLEAN_FIELD),
          new FieldLiteral(' ').as(CHAR_FIELD),
          new NullFieldLiteral().as(BLOB_FIELD));

    String value = varCharCast("' '");
    String expectedSql = "INSERT INTO " + tableName(TEST_TABLE) + " (id, version, stringField, intField, floatField, dateField, booleanField, charField, blobField, bigIntegerField, clobField) SELECT id, version, stringField, intField, floatField, null AS dateField, null AS booleanField, " + stringLiteralPrefix() + value +" AS charField, null AS blobField, 12345 AS bigIntegerField, null AS clobField FROM " + tableName(OTHER_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with null defaults", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Test that an Insert statement is generated with a null value
   */
  @Test
  public void testInsertWithNullLiterals() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(ALTERNATE_TABLE))
        .fields(
          literal(1).as("id"),
          literal(0).as("version"),
          new NullFieldLiteral().as(STRING_FIELD)
            );

    String expectedSql = "INSERT INTO " + tableName(ALTERNATE_TABLE) + " (id, version, stringField) VALUES (1, 0, NULL)";

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with null literals", ImmutableList.of(expectedSql).toString().toLowerCase(), sql.toString().replaceAll("/\\*.*?\\*/ ", "").toLowerCase());
  }


  /**
   * Tests that an insert from a select works when no defaults are supplied.
   */
  @Test
  public void testInsertFromSelectFullyExpressed() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("id"),
      new FieldReference("version"),
      new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(FLOAT_FIELD))
    .from(new TableReference(TEST_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference(OTHER_TABLE))
        .fields(new FieldReference("id"),
          new FieldReference("version"),
          new FieldReference(STRING_FIELD),
          new FieldReference(INT_FIELD),
          new FieldReference(FLOAT_FIELD))
          .from(sourceStmt);

    String expectedSql = "INSERT INTO " + tableName(OTHER_TABLE) + " (id, version, stringField, intField, floatField) SELECT id, version, stringField, intField, floatField FROM " + tableName(TEST_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with explicit field lists", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Tests that an insert from a select works when the source table is in a different schema.
   */
  @Test
  public void testInsertFromSelectWithSourceInDifferentSchema() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("id"),
      new FieldReference("version"),
      new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(FLOAT_FIELD))
    .from(new TableReference("MYSCHEMA", TEST_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference(OTHER_TABLE))
        .fields(new FieldReference("id"),
          new FieldReference("version"),
          new FieldReference(STRING_FIELD),
          new FieldReference(INT_FIELD),
          new FieldReference(FLOAT_FIELD))
          .from(sourceStmt);

    String expectedSql = "INSERT INTO " + tableName(OTHER_TABLE) + " (id, version, stringField, intField, floatField) SELECT id, version, stringField, intField, floatField FROM " + differentSchemaTableName(TEST_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with explicit field lists", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Tests that an insert from a select works when the source table and those in the join statement are in a different schema.
   */
  @Test
  public void testInsertFromSelectWithSourceAndJoinedInDifferentSchema() {
    TableReference source = new TableReference("MYSCHEMA", TEST_TABLE);
    TableReference sourceJoin = new TableReference("MYSCHEMA", ALTERNATE_TABLE);
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("id"),
      new FieldReference("version"),
      new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(FLOAT_FIELD))
    .from(source)
    .innerJoin(sourceJoin, source.field(STRING_FIELD).eq(sourceJoin.field(STRING_FIELD)));

    InsertStatement stmt = new InsertStatement().into(new TableReference(OTHER_TABLE))
        .fields(new FieldReference("id"),
          new FieldReference("version"),
          new FieldReference(STRING_FIELD),
          new FieldReference(INT_FIELD),
          new FieldReference(FLOAT_FIELD))
          .from(sourceStmt);

    String expectedSql = "INSERT INTO " + tableName(OTHER_TABLE) + " (id, version, stringField, intField, floatField) SELECT id, version, stringField, intField, floatField FROM " + differentSchemaTableName(TEST_TABLE) + " INNER JOIN " + differentSchemaTableName(ALTERNATE_TABLE) + " ON (Test.stringField = Alternate.stringField)";

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with explicit field lists", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Tests that an insert from a select works when the target table is in a different schema.
   */
  @Test
  public void testInsertFromSelectWithTargetInDifferentSchema() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("id"),
      new FieldReference("version"),
      new FieldReference(STRING_FIELD),
      new FieldReference(INT_FIELD),
      new FieldReference(FLOAT_FIELD))
    .from(new TableReference(TEST_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference("MYSCHEMA", OTHER_TABLE))
        .fields(new FieldReference("id"),
          new FieldReference("version"),
          new FieldReference(STRING_FIELD),
          new FieldReference(INT_FIELD),
          new FieldReference(FLOAT_FIELD))
          .from(sourceStmt);

    String expectedSql = "INSERT INTO " + differentSchemaTableName(OTHER_TABLE) + " (id, version, stringField, intField, floatField) SELECT id, version, stringField, intField, floatField FROM " + tableName(TEST_TABLE);

    List<String> sql = testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with explicit field lists", ImmutableList.of(expectedSql), sql);
  }


  /**
   * Tests an insert from a select which joins inner selects using a where clause. The fields for selection are not specified.
   */
  @Test
  public void testInsertFromSelectStatementWhereJoinOnInnerSelect() {
    SelectStatement inner1 = select(field(INNER_FIELD_A).as(INNER_FIELD_A), field(INNER_FIELD_B).as(INNER_FIELD_B)).from(tableRef("Inner")).alias("InnerAlias");

    SelectStatement outer = select().
        from(inner1);

    InsertStatement insert = insert().
        into(tableRef("InsertAB")).
        fields(field(INNER_FIELD_A), field(INNER_FIELD_B)).
        from(outer);

    String expectedSql =
        "INSERT INTO " + tableName("InsertAB") + " (innerFieldA, innerFieldB) " +
            "SELECT InnerAlias.innerFieldA, InnerAlias.innerFieldB " +
            "FROM (SELECT innerFieldA AS innerFieldA, innerFieldB AS innerFieldB FROM " + tableName("Inner") + ") InnerAlias";

    assertEquals("Select with join on where clause", ImmutableList.of(expectedSql), testDialect.convertStatementToSQL(insert, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE)));
  }


  /**
   * Tests an insert from a select which joins inner selects using a where clause. The fields for selection are specified.
   *
   * <p>The use case for this is to select a subset of the fields from an inner select, where the inner select has joined across several tables</p>.
   */
  @Test
  public void testInsertFromSelectStatementWithExplicitFieldsWhereJoinOnInnerSelect() {
    SelectStatement inner1 = select(field(INNER_FIELD_A).as(INNER_FIELD_A), field(INNER_FIELD_B).as(INNER_FIELD_B)).from(tableRef("Inner")).alias("InnerAlias");

    SelectStatement outer = select(field(INNER_FIELD_A)).
        from(inner1);

    InsertStatement insert = insert().
        into(tableRef("InsertA")).
        fields(field(INNER_FIELD_A)).
        from(outer);

    String expectedSql =
        "INSERT INTO " + tableName("InsertA") + " (innerFieldA) " +
            "SELECT innerFieldA " +
            "FROM (SELECT innerFieldA AS innerFieldA, innerFieldB AS innerFieldB FROM " + tableName("Inner") + ") InnerAlias";

    assertEquals("Select with join on where clause", ImmutableList.of(expectedSql), testDialect.convertStatementToSQL(insert, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE)));
  }


  /**
   * Tests that when forming an insert/select from two table references, the {@link SqlDialect} acts in a case insensitive manner
   * when determining if source fields are present in the destination table.
   */
  @Test
  public void testInsertFromSelectIgnoresCase() {
    InsertStatement insertStatement = new InsertStatement().into(new TableReference(UPPER_TABLE)).from(new TableReference(MIXED_TABLE));
    String expectedSql = "INSERT INTO " + tableName(UPPER_TABLE) + " (id, version, FIELDA) SELECT id, version, FIELDA FROM " + tableName(MIXED_TABLE);
    List<String> sql = testDialect.convertStatementToSQL(insertStatement, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Expected INSERT to be case insensitive", expectedSql, sql.get(sql.size() - 1));
  }


  /**
   * Tests that an insert from a select with mis matched fields generates an error.
   */
  @Test
  public void testInsertFromSelectWithMismatchedFieldsError() {
    SelectStatement sourceStmt = new SelectStatement(new FieldReference("id"),
      new FieldReference("version"),
      new FieldReference(STRING_FIELD))
    .from(new TableReference(OTHER_TABLE));

    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE))
        .fields(new FieldReference("id"),
          new FieldReference("version"),
          new FieldReference(STRING_FIELD),
          new FieldReference(INT_FIELD))
          .from(sourceStmt);

    try {
      testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
      fail("Should error due to mismatched field counts");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }
  }


  /**
   * Tests the SQL statement that are run after a data insert.
   */
  @Test
  public void testPostInsertWithPresetAutonumStatementsInsertingUnderAutonumLimit() {

    testDialect.postInsertWithPresetAutonumStatements(metadata.getTable(TEST_TABLE), sqlScriptExecutor,connection,true);
    testDialect.postInsertWithPresetAutonumStatements(metadata.getTable(AUTO_NUMBER_TABLE), sqlScriptExecutor,connection, true);

    verifyPostInsertStatementsInsertingUnderAutonumLimit(sqlScriptExecutor,connection);
  }


  /**
   * Tests the SQL statement that are run after a data insert.
   */
  @Test
  public void testPostInsertWithPresetAutonumStatementsNotInsertingUnderAutonumLimit() {
    testDialect.postInsertWithPresetAutonumStatements(metadata.getTable(TEST_TABLE), sqlScriptExecutor,connection,false);
    testDialect.postInsertWithPresetAutonumStatements(metadata.getTable(AUTO_NUMBER_TABLE), sqlScriptExecutor,connection, false);

    verifyPostInsertStatementsNotInsertingUnderAutonumLimit(sqlScriptExecutor,connection);
  }


  /**
   * Tests the SQL statement that are run before a data insert.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testPreInsertWithPresetAutonumStatementsInsertingUnderAutonumLimit() {
    compareStatements(
      expectedPreInsertStatementsInsertingUnderAutonumLimit(),
      testDialect.preInsertWithPresetAutonumStatements(metadata.getTable(TEST_TABLE), true),
      testDialect.preInsertWithPresetAutonumStatements(metadata.getTable(AUTO_NUMBER_TABLE), false)
    );
  }


  /**
   * Tests for {@link SqlDialect#repairAutoNumberStartPosition(Table, SqlScriptExecutor, Connection)}
   */
  @Test
  public void testRepairAutoNumberStartPositionOverRepairLimit() {

    setMaxIdOnAutonumberTable(MAX_ID_OVER_REPAIR_LIMIT);

    testDialect.repairAutoNumberStartPosition(metadata.getTable(TEST_TABLE), sqlScriptExecutor,connection);
    testDialect.repairAutoNumberStartPosition(metadata.getTable(AUTO_NUMBER_TABLE), sqlScriptExecutor,connection);
    verifyRepairAutoNumberStartPosition(sqlScriptExecutor,connection);
  }


  /**
   * Tests for {@link SqlDialect#repairAutoNumberStartPosition(Table, SqlScriptExecutor, Connection)}
   */
  @Test
  public void testRepairAutoNumberStartPositionUnderRepairLimit() {

    setMaxIdOnAutonumberTable(MAX_ID_UNDER_REPAIR_LIMIT);

    testDialect.repairAutoNumberStartPosition(metadata.getTable(TEST_TABLE), sqlScriptExecutor,connection);
    testDialect.repairAutoNumberStartPosition(metadata.getTable(AUTO_NUMBER_TABLE), sqlScriptExecutor,connection);
    verifyRepairAutoNumberStartPosition(sqlScriptExecutor,connection);
  }


  /**
   * Method to override in dialect specific tests to set the max id value on the autonumber table for use during
   * testRepairAutoNumberStartPosition
   *
   * @param id  The max id
   */
  protected void setMaxIdOnAutonumberTable(@SuppressWarnings("unused") long id) {

  }


  /**
   * Verify on the expected SQL statements to be run on repairing the autonumber start position.
   * @param sqlScriptExecutor The script executor
   * @param connection The connection to use
   */
  @SuppressWarnings("unused")
  protected void verifyRepairAutoNumberStartPosition(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    verifyNoMoreInteractions(sqlScriptExecutor);
  }

  /**
   * Tests the SQL statement that are run before a data insert.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testPreInsertWithPresetAutonumStatementsNotInsertingUnderAutonumLimit() {
    compareStatements(
      expectedPreInsertStatementsNotInsertingUnderAutonumLimit(),
      testDialect.preInsertWithPresetAutonumStatements(metadata.getTable(TEST_TABLE), false),
      testDialect.preInsertWithPresetAutonumStatements(metadata.getTable(AUTO_NUMBER_TABLE), false)
    );
  }


  /**
   * Tests that a simple update with field literal works.
   */
  @Test
  public void testSimpleUpdate() {
    UpdateStatement stmt = new UpdateStatement(new TableReference(TEST_TABLE)).set(new FieldLiteral("A1001001").as(STRING_FIELD));
    String value = varCharCast("'A1001001'");
    String expectedSql = "UPDATE " + tableName(TEST_TABLE) + " SET stringField = " + stringLiteralPrefix() + value;
    assertEquals("Simple update", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a simple delete string is created correctly.
   */
  @Test
  public void testSimpleDelete() {
    DeleteStatement stmt = new DeleteStatement(new TableReference(TEST_TABLE));
    String expectedSql = "DELETE FROM " + tableName(TEST_TABLE);
    assertEquals("Simple delete", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a delete string with a where criterion is created correctly.
   */
  @Test
  public void testDeleteWithWhereCriterion() {
    DeleteStatement stmt = new DeleteStatement(new TableReference(TEST_TABLE)).where(eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD), "A001003657"));
    String value = varCharCast("'A001003657'");
    String expectedSql = "DELETE FROM " + tableName(TEST_TABLE) + " WHERE (Test.stringField = " + stringLiteralPrefix() + value + ")";
    assertEquals("Simple delete", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a delete string with a limit and a simple where criterion is created correctly.
   */
  @Test
  public void testDeleteWithLimitAndSimpleWhereCriterion() {
    DeleteStatement stmt = DeleteStatement
      .delete(new TableReference(TEST_TABLE))
      .where(eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD), "A001003657"))
      .limit(1000)
      .build();

    String value = varCharCast("'A001003657'");
    assertEquals("Delete with simple where clause and limit", expectedDeleteWithLimitAndWhere(value), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a delete string with a limit and a complex where criterion (involving an 'OR') is created correctly (i.e. brackets around the 'OR' are preserved).
   */
  @Test
  public void testDeleteWithLimitAndComplexWhereCriterion() {
    DeleteStatement stmt = DeleteStatement
      .delete(new TableReference(TEST_TABLE))
      .where(or(eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD), "A001003657"),
        eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD), "A001003658")))
      .limit(1000)
      .build();

    String value1 = varCharCast("'A001003657'");
    String value2 = varCharCast("'A001003658'");
    assertEquals("Delete with 'OR' where clause and limit - NB do not alter brackets incautiously", expectedDeleteWithLimitAndComplexWhere(value1, value2), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a delete string with a limit and no where criterion is created correctly.
   */
  @Test
  public void testDeleteWithLimitWithoutWhereCriterion() {
    DeleteStatement stmt = DeleteStatement
      .delete(new TableReference(TEST_TABLE))
      .limit(1000)
      .build();

    assertEquals("Delete with limit", expectedDeleteWithLimitWithoutWhere(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that a delete statement is prefixed with the schema name if the schema is specified.
   */
  @Test
  public void testDeleteWithTableInDifferentSchema() {
    DeleteStatement stmt = new DeleteStatement(new TableReference("MYSCHEMA", TEST_TABLE));
    String expectedSql = "DELETE FROM " + differentSchemaTableName(TEST_TABLE);
    assertEquals("Simple delete", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests that an update from a select works when defaults are supplied.
   */
  @Test
  public void testUpdateUsingFieldFromSelect() {
    SelectStatement fieldOneSelect = new SelectStatement(new FieldReference(FLOAT_FIELD)).from(new TableReference(TEST_TABLE))
        .where(eq(new FieldReference(new TableReference(TEST_TABLE), STRING_FIELD), "A001003657"));

    UpdateStatement updateStmt = new UpdateStatement(new TableReference(OTHER_TABLE))
    .set(new FieldFromSelect(fieldOneSelect).as(INT_FIELD), new FieldLiteral("blank").as(STRING_FIELD));

    String value1 = varCharCast("'A001003657'");
    String value2 = varCharCast("'blank'");
    String expectedSql = "UPDATE " + tableName(OTHER_TABLE) + " SET intField = (SELECT floatField FROM " + tableName(TEST_TABLE) + " WHERE (Test.stringField = " + stringLiteralPrefix() + value1 + ")), stringField = " + stringLiteralPrefix() + value2;

    assertEquals("Update from a select", expectedSql, testDialect.convertStatementToSQL(updateStmt));
  }


  /**
   * Test whether the right update SQL statement was generated
   */
  @Test
  public void testUpdateWithLiteralValues() {
    UpdateStatement stmt = update(tableRef(TEST_TABLE))
      .set(literal("Value").as(STRING_FIELD))
      .set(blobLiteral(NEW_BLOB_VALUE).as("blobFieldOne"))
      .set(blobLiteral(NEW_BLOB_VALUE.getBytes(StandardCharsets.UTF_8)).as("blobFieldTwo"))
      .where(and(
        field("field1").eq(true),
        field("field2").eq(false),
        field("field3").eq(literal(true)),
        field("field4").eq(literal(false)),
        field("field5").eq(new LocalDate(2010, 1, 2)),
        field("field6").eq(literal(new LocalDate(2010, 1, 2))),
        field("field7").eq("Value"),
        field("field8").eq(literal("Value")),
        field("field9").eq(blobLiteral(OLD_BLOB_VALUE)),
        field("field10").eq(blobLiteral(OLD_BLOB_VALUE.getBytes(StandardCharsets.UTF_8)))
      ));
    assertEquals(
      "Update with literal values",
      expectedUpdateWithLiteralValues(),
      testDialect.convertStatementToSQL(stmt)
    );
  }


  /**
   * Test that an update statement is generated with a null value.
   */
  @Test
  public void testUpdateWithNull() {
    UpdateStatement stmt = new UpdateStatement(new TableReference(TEST_TABLE)).set(new NullFieldLiteral().as(STRING_FIELD));
    String expectedSql = "UPDATE " + tableName(TEST_TABLE) + " SET stringField = null";
    assertEquals("Update with null value", expectedSql, testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Test that an empty string literal is converted to {@code NULL}
   * on all database platforms, following the WEB-9161 harmonisation
   * of empty-string/null handling across vendors.
   *
   * @see #testInsertWithNullDefaults()
   * @see #testUpdateWithNull()
   */
  @Test
  public void testEmptyStringLiteralIsNull() {
    UpdateStatement updateStmt = new UpdateStatement(new TableReference(TEST_TABLE)).set(new FieldLiteral("").as(STRING_FIELD));
    assertEquals("Update with literal value", "UPDATE " + tableName(TEST_TABLE) + " SET stringField = NULL", testDialect.convertStatementToSQL(updateStmt));

    InsertStatement insertStmt = new InsertStatement().into(new TableReference(TEST_TABLE)).values(new FieldLiteral("").as(STRING_FIELD));

    List<String> sql = testDialect.convertStatementToSQL(insertStmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
    assertEquals("Insert with literal null", expectedEmptyStringInsertStatement(), sql.get(sql.size() - 1));
  }


  /**
   * Tests update SQL using a select minimum.
   */
  @Test
  public void testUpdateWithSelectMinimum() {
    SelectStatement stmt = new SelectStatement(min(new FieldReference(INT_FIELD)))
    .from(new TableReference(TEST_TABLE).as("T"))
    .where(and(
      eq(new FieldReference(new TableReference("T"), CHAR_FIELD), new FieldLiteral("S")),
      eq(new FieldReference(new TableReference("T"), STRING_FIELD), new FieldReference(new TableReference("O"), STRING_FIELD)),
      eq(new FieldReference(new TableReference("T"), INT_FIELD), new FieldReference(new TableReference("O"), INT_FIELD))
        )
        );

    UpdateStatement updateStmt = new UpdateStatement(new TableReference(OTHER_TABLE).as("O"))
    .set(new FieldFromSelect(stmt).as(INT_FIELD))
    .where(eq(new FieldReference(STRING_FIELD), new FieldLiteral("Y")));

    assertEquals("Update scripts are not the same",
      expectedUpdateWithSelectMinimum(),
      testDialect.convertStatementToSQL(updateStmt));
  }


  /**
   * Tests an update with arguments referring to an aliased table.
   */
  @Test
  public void testUpdateUsingAliasedTable() {

    SelectStatement fieldOneSelect = new SelectStatement(new FieldReference(FLOAT_FIELD)).from(new TableReference(TEST_TABLE).as("stageName"))
        .where(eq(new FieldReference(new TableReference(TEST_TABLE).as("stageName"), STRING_FIELD), "A001003657"));

    UpdateStatement updateStmt = new UpdateStatement(new TableReference("myUpdateTable"))
    .set(new FieldFromSelect(fieldOneSelect).as(INT_FIELD), new FieldLiteral("blank").as(STRING_FIELD));

    String value1 = varCharCast("'A001003657'");
    String value2 = varCharCast("'blank'");
    String expectedSql = "UPDATE " + tableName("myUpdateTable") + " SET intField = (SELECT floatField FROM " + tableName(TEST_TABLE) + " stageName WHERE (stageName.stringField = " + stringLiteralPrefix() + value1 + ")), stringField = " + stringLiteralPrefix() + value2;
    assertEquals("Update from a select with alias", expectedSql, testDialect.convertStatementToSQL(updateStmt));
  }

  /**
   * Tests an update where the destination table is aliased.
   */
  @Test
  public void testUpdateUsingAliasedDestinationTable() {
    SelectStatement selectStmt = new SelectStatement(new FieldReference("settlementFrequency"))
    .from(new TableReference("FloatingRateDetail").as("B"))
    .where(
      eq(new FieldReference(new TableReference("A"), "floatingRateDetailId"), new FieldReference(new TableReference("B"), "id")));

    UpdateStatement updateStmt = new UpdateStatement(new TableReference("FloatingRateRate").as("A"))
    .set(new FieldFromSelect(selectStmt).as("settlementFrequency"));

    assertEquals("Update from a select with aliased destination",
      expectedUpdateUsingAliasedDestinationTable(),
      testDialect.convertStatementToSQL(updateStmt));
  }


  /**
   * Tests an update where the destination table is in a different schema.
   */
  @Test
  public void testUpdateUsingTargetTableInDifferentSchema() {
    SelectStatement selectStmt = new SelectStatement(new FieldReference("settlementFrequency"))
    .from(new TableReference("FloatingRateDetail").as("B"))
    .where(
      eq(new FieldReference(new TableReference("A"), "floatingRateDetailId"), new FieldReference(new TableReference("B"), "id")));

    UpdateStatement updateStmt = new UpdateStatement(new TableReference("MYSCHEMA", "FloatingRateRate").as("A"))
    .set(new FieldFromSelect(selectStmt).as("settlementFrequency"));

    assertEquals("Update from a select with the destination table in a different schema",
      expectedUpdateUsingTargetTableInDifferentSchema(),
      testDialect.convertStatementToSQL(updateStmt));
  }



  /**
   * Tests an update where the source table is in a different schema.
   */
  @Test
  public void testUpdateUsingSourceTableInDifferentSchema() {
    SelectStatement selectStmt = new SelectStatement(new FieldReference("settlementFrequency"))
    .from(new TableReference("MYSCHEMA", "FloatingRateDetail").as("B"))
    .where(
      eq(new FieldReference(new TableReference("A"), "floatingRateDetailId"), new FieldReference(new TableReference("B"), "id")));

    UpdateStatement updateStmt = new UpdateStatement(new TableReference("FloatingRateRate").as("A"))
    .set(new FieldFromSelect(selectStmt).as("settlementFrequency"));

    assertEquals("Update from a select with the destination table in a different schema",
      expectedUpdateUsingSourceTableInDifferentSchema(),
      testDialect.convertStatementToSQL(updateStmt));
  }


  /**
   * Tests a delete referring to an aliased table.
   */
  @Test
  public void testDeleteUsingAliasedTable() {
    DeleteStatement deleteStmt = new DeleteStatement(new TableReference("myDeleteTable").as("stageName"));

    String expectedSql = "DELETE FROM " + tableName("myDeleteTable") + " stageName";
    assertEquals("Delete with alias", expectedSql, testDialect.convertStatementToSQL(deleteStmt));
  }


  /**
   * Tests that a null statement causes an error.
   */
  @Test
  public void testNullStatementError() {
    try {
      SelectStatement stmt = null;
      testDialect.convertStatementToSQL(stmt);
      fail("Should not be able to get SQL from a null statement");
    } catch (IllegalArgumentException e) {
      // Expected exception
    }

  }


  /**
   * Tests that passing a null value for the metadata fails.
   */
  @Test
  public void testNullMetadataError() {
    InsertStatement stmt = new InsertStatement().into(new TableReference(TEST_TABLE));

    try {
      testDialect.convertStatementToSQL(stmt, null, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
      fail("Should have raised an exception when null metadata was supplied");
    } catch(IllegalArgumentException e) {
      // Expected exception
    }
  }


  /**
   * Tests that passing a null value for the metadata fails
   */
  @Test
  public void testMissingMetadataError() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("missingTable"));

    try {
      testDialect.convertStatementToSQL(stmt, metadata, SqlDialect.IdTable.withDeterministicName(ID_VALUES_TABLE));
      fail("Should have raised an exception when there was no metadata for the table being inserted into");
    } catch(IllegalArgumentException e) {
      // Expected exception
    }
  }


  /**
   * Tests concatenation in a select with {@linkplain FieldReference}s and
   * {@linkplain FieldLiteral}s.
   */
  @Test
  public void testSelectWithConcatenation() {
    SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldReference("assetDescriptionLine1"), new FieldLiteral(
        " "), new FieldReference("assetDescriptionLine2")).as("assetDescription")).from(new TableReference("schedule"));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedSelectWithConcatenation1(), result);

    stmt = new SelectStatement(new ConcatenatedField(new FieldReference("assetDescriptionLine1"), new FieldLiteral("XYZ"),
      new FieldReference("assetDescriptionLine2")).as("assetDescription")).from(new TableReference("schedule"));

    result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedSelectWithConcatenation2(), result);
  }


  /**
   * Tests concatenation in a select with {@linkplain Function}.
   */
  @Test
  public void testSelectWithConcatenationUsingFunction() {
    SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldReference("assetDescriptionLine1"), max(new FieldReference("scheduleStartDate"))).as("test")).from(new TableReference("schedule"));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedConcatenationWithFunction(), result);
  }


  /**
   * Tests concatenation in a select with {@linkplain CaseStatement}.
   */
  @Test
  public void testSelectWithConcatenationUsingCase() {
    WhenCondition whenCondition = new WhenCondition(eq(new FieldReference("taxVariationIndicator"), new FieldLiteral('Y')), new FieldReference("exposureCustomerNumber"));
    SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldReference("assetDescriptionLine1"),
      new CaseStatement(new FieldReference("invoicingCustomerNumber"), whenCondition)).as("test")).from(new TableReference(
          "schedule"));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedConcatenationWithCase(), result);
  }


  /**
   * Tests concatenation in a select with nested concatenations.
   */
  @Test
  public void testSelectWithNestedConcatenations() {
    SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldReference("field1"), new ConcatenatedField(
      new FieldReference("field2"), new FieldLiteral("XYZ"))).as("test")).from(new TableReference("schedule"));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedNestedConcatenations(), result);
  }


  /**
   * Check that we get an illegal argument exception when we try to concatenate
   * a single field.
   */
  @Test
  public void testConcatenateWithOneField() {
    try {
      new SelectStatement(new ConcatenatedField(new FieldReference("field1")).as("test")).from(new TableReference("schedule"));
      fail("Should have thrown an exception on construction");
    } catch (IllegalArgumentException e) {
      // Should have thrown an exception on construction
    }
  }


  /**
   * Test that IsNull functionality behaves as expected.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testIsNull() {
    String result = testDialect.getSqlFrom(isnull(new FieldLiteral("A"), new FieldLiteral("B")));
    assertEquals(expectedIsNull(), result);
  }


  /**
   * Test that YYYYMMDDToDate functionality behaves as expected.
   */
  @Test
  public void testYYYYMMDDToDate() {
    String result = testDialect.getSqlFrom(yyyymmddToDate(new FieldLiteral("20100101")));
    assertEquals(expectedYYYYMMDDToDate(), result);
  }

  /**
   * Test that getSqlFrom((ClobFieldLiteral)) Returns correctly.
   */
  @Test
  public void testClobFieldLiteralWithLongfield() {
    String result = testDialect.getSqlFrom(new ClobFieldLiteral(LONG_FIELD_STRING));
    assertEquals(expectedClobLiteralCast(), result);
  }


  /**
   * Test that YYYYMMDDToDate functionality behaves as expected.
   */
  @Test
  public void testDateToYyyymmdd() {
    String result = testDialect.getSqlFrom(dateToYyyymmdd(field("testField")));
    assertEquals(expectedDateToYyyymmdd(), result);
  }


  /**
   * Test that YYYYMMDDHHmmssToDate functionality behaves as expected.
   */
  @Test
  public void testDateToYyyymmddHHmmss() {
    String result = testDialect.getSqlFrom(dateToYyyyMMddHHmmss(field("testField")));
    assertEquals(expectedDateToYyyymmddHHmmss(), result);
  }


  /**
   * Test that now functionality behaves as expected.
   */
  @Test
  public void testNow() {
    String result = testDialect.getSqlFrom(now());
    assertEquals(expectedNow(), result);
  }


  /**
   * Test that AddDays functionality behaves as expected.
   */
  @Test
  public void testAddDays() {
    String result = testDialect.getSqlFrom(addDays(field("testField"), new FieldLiteral(-20)));
    assertEquals(expectedAddDays(), result);
  }


  /**
   * Test that AddMonths functionality behaves as expected.
   */
  @Test
  public void testAddMonths() {
    String result = testDialect.getSqlFrom(addMonths(field("testField"), new FieldLiteral(-3)));
    assertEquals(expectedAddMonths(), result);
  }



  /**
   * Test that Round functionality behaves as expected.
   */
  @Test
  public void testRound() {
    // Given
    Function round = round(new FieldReference("field1"), new FieldLiteral(2));
    SelectStatement stmt = new SelectStatement(round).from(new TableReference("schedule"));

    // When
    String result = testDialect.convertStatementToSQL(stmt);

    // Then
    assertEquals("Round script should match expected", expectedRound(), result);
  }


  /**
   * Test that DAYS_BETWEEN functionality behaves as expected
   */
  @Test
  public void testDaysBetween() {
    SelectStatement testStatement = select(daysBetween(field("dateOne"), field("dateTwo")))
        .from(tableRef("MyTable"));
    assertEquals(expectedDaysBetween(), testDialect.convertStatementToSQL(testStatement));
  }


  /**
   * Test the COALESCE functionality behaves as expected
   */
  @Test
  public void testCoalesce() {
    SelectStatement testStatement = select(coalesce(new NullFieldLiteral(), field("bob"))).from(tableRef("MyTable"));
    assertEquals(expectedCoalesce().toLowerCase(), testDialect.convertStatementToSQL(testStatement).toLowerCase());
  }


  /**
   * Test the GREATEST functionality behaves as expected
   */
  @Test
  public void testGreatest() {
    SelectStatement testStatement = select(greatest(new NullFieldLiteral(), field("bob"))).from(tableRef("MyTable"));
    assertEquals(expectedGreatest().toLowerCase(), testDialect.convertStatementToSQL(testStatement).toLowerCase());
  }


  /**
   * Test the LEAST functionality behaves as expected
   */
  @Test
  public void testLeast() {
    SelectStatement testStatement = select(least(new NullFieldLiteral(), field("bob"))).from(tableRef("MyTable"));
    assertEquals(expectedLeast().toLowerCase(), testDialect.convertStatementToSQL(testStatement).toLowerCase());
  }


  /**
   * Test that adding numbers returns as expected.
   */
  @Test
  public void testMathsPlus() {
    String result = testDialect.getSqlFrom(new MathsField(new FieldLiteral(1), MathsOperator.PLUS, new FieldLiteral(1)));
    assertEquals(expectedMathsPlus(), result);
  }


  /**
   * Test that adding numbers returns as expected.
   */
  @Test
  public void testMathsMinus() {
    String result = testDialect.getSqlFrom(new MathsField(new FieldLiteral(1), MathsOperator.MINUS, new FieldLiteral(1)));
    assertEquals(expectedMathsMinus(), result);
  }


  /**
   * Test that adding numbers returns as expected.
   */
  @Test
  public void testMathsDivide() {
    String result = testDialect.getSqlFrom(new MathsField(new FieldLiteral(1), MathsOperator.DIVIDE, new FieldLiteral(1)));
    assertEquals(expectedMathsDivide(), result);
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Since it is a chain of operations, and all of the operations takes a field
   * as a second operand, there should be no brackets in the generated SQL.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations1() {
    String result = testDialect.getSqlFrom(field("a").divideBy(field("b")).plus(field("c")));
    assertEquals(expectedSqlForMathOperations1(), result);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testRowNumberWithNoOrderByClause() {
    testDialect.getSqlFrom(WindowFunction.over(rowNumber()).partitionBy(field("a")).build());
  }


  /**
   * @return expected SQL for math operation 1
   */
  protected String expectedSqlForMathOperations1() {
    return "a / b + c";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Since it is a chain of operations, and all of the operations takes a field
   * or a literal as a second operand, there should be no brackets in the
   * generated SQL.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations2() {
    String result = testDialect.getSqlFrom(field("a").divideBy(field("b")).plus(literal(100)));
    assertEquals(expectedSqlForMathOperations2(), result);
  }


  /**
   * @return expected SQL for math operation 2
   */
  protected String expectedSqlForMathOperations2() {
    return "a / b + 100";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Bracket should be generated for subexpression "b+c". Even without explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations3() {
    String result = testDialect.getSqlFrom(field("a").divideBy(field("b").plus(field("c"))));
    assertEquals(expectedSqlForMathOperations3(), result);
  }


  /**
   * @return expected SQL for math operation 3
   */
  protected String expectedSqlForMathOperations3() {
    return "a / (b + c)";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Bracket should be generated for subexpression "b+100". Even without explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations4() {
    String result = testDialect.getSqlFrom(field("a").divideBy(field("b").plus(literal(100))));
    assertEquals(expectedSqlForMathOperations4(), result);
  }


  /**
   * @return expected SQL for math operation 4
   */
  protected String expectedSqlForMathOperations4() {
    return "a / (b + 100)";
  }


  /**
   * Tests that expression builder produces an output with brackets if a second
   * operand is Math operation.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations5() {
    String result = testDialect.getSqlFrom(field("a").multiplyBy(field("b").plus(field("c"))));
    assertEquals(expectedSqlForMathOperations5(), result);
  }


  /**
   * @return expected SQL for math operation 5
   */
  protected String expectedSqlForMathOperations5() {
    return "a * (b + c)";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Subexpression "c-d" should be put to the bracket implicitly, even without
   * explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations6() {
    AliasedField aPlusB = field("a").plus(field("b"));
    AliasedField cMinusD = field("c").minus(field("d"));
    String result = testDialect.getSqlFrom(aPlusB.divideBy(cMinusD));
    assertEquals(expectedSqlForMathOperations6(), result);
  }


  /**
   * @return expected SQL for math operation 6
   */
  protected String expectedSqlForMathOperations6() {
    return "a + b / (c - d)";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions
   * that use brackets.
   * <p>
   * Subexpression "a+b" is put to bracket explicitly, and
   * the subexpression "c-d" should be put to the bracket implicitly, even without explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations7() {
    AliasedField aPlusB = bracket(field("a").plus(field("b")));
    AliasedField cMinusD = field("c").minus(field("d"));
    String result = testDialect.getSqlFrom(aPlusB.divideBy(cMinusD));
    assertEquals(expectedSqlForMathOperations7(), result);
  }


  /**
   * @return expected SQL for math operation 7
   */
  protected String expectedSqlForMathOperations7() {
    return "(a + b) / (c - d)";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations8() {
    String result = testDialect.getSqlFrom(field("a").plus(field("b")).plus(field("c")).plus(field("d")).plus(field("e")));
    assertEquals(expectedSqlForMathOperations8(), result);
  }


  /**
   * @return expected SQL for math operation 8
   */
  protected String expectedSqlForMathOperations8() {
    return "a + b + c + d + e";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Bracket should be generated for subexpression "b/c". Even without explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations9() {
    AliasedField dDivByE = field("c").divideBy(field("d"));
    String result = testDialect.getSqlFrom(field("a").plus(field("b")).plus(dDivByE).plus(field("e")).plus(literal(100))
        .plus(field("f")).divideBy(literal(5)));
    assertEquals(expectedSqlForMathOperations9(), result);
  }


  /**
   * @return expected SQL for math operation 9
   */
  protected String expectedSqlForMathOperations9() {
    return "a + b + (c / d) + e + 100 + f / 5";
  }



  /**
   * Test for proper SQL mathematics operation generation from DSL expressions
   * that use brackets.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations10() {
    AliasedField dDivByE = field("c").divideBy(field("d"));
    AliasedField bracketed = bracket(field("a").plus(field("b")).plus(dDivByE).plus(field("e")).plus(literal(100)).plus(field("f")));
    String result = testDialect.getSqlFrom(bracketed.divideBy(literal(5)));
    assertEquals(expectedSqlForMathOperations10(), result);
  }


  /**
   * @return expected SQL for math operation 10
   */
  protected String expectedSqlForMathOperations10() {
    return "(a + b + (c / d) + e + 100 + f) / 5";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions
   * that use brackets.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations11() {
    String result = testDialect.getSqlFrom(bracket(field("a").divideBy(literal(100)).plus(literal(1))).divideBy(field("b")).plus(
      literal(100)));
    assertEquals(expectedSqlForMathOperations11(), result);
  }


  /**
   * @return expected SQL for math operation 11
   */
  protected String expectedSqlForMathOperations11() {
    return "(a / 100 + 1) / b + 100";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions
   * that use brackets.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations12() {
    String result = testDialect.getSqlFrom(bracket(field("a").plus(field("b"))).divideBy(field("c")));
    assertEquals(expectedSqlForMathOperations12(), result);
  }


  /**
   * @return expected SQL for math operation 12
   */
  protected String expectedSqlForMathOperations12() {
    return "(a + b) / c";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations13() {
    String result = testDialect.getSqlFrom(field("a").plus(field("b")).plus(field("c")).divideBy(literal(2)));
    assertEquals(expectedSqlForMathOperations13(), result);
  }


  /**
   * @return expected SQL for math operation 13
   */
  protected String expectedSqlForMathOperations13() {
    return "a + b + c / 2";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   * <p>
   * Bracket should be generated for subexpression "b+c". Even without explicit
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} call.
   * </p>
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations14() {
    String result = testDialect.getSqlFrom(field("a").plus(field("b").plus(field("c"))).divideBy(literal(2)));
    assertEquals(expectedSqlForMathOperations14(), result);
  }


  /**
   * @return expected SQL for math operation 14
   */
  protected String expectedSqlForMathOperations14() {
    return "a + (b + c) / 2";
  }


  /**
   * Expression that should be wrapped implicitly, is wrapped additionally with
   * a bracket() method.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations15() {
    String result = testDialect.getSqlFrom(field("a").plus(bracket(field("b").plus(field("c")))).divideBy(literal(2)));
    assertEquals(expectedSqlForMathOperations15(), result);
  }


  /**
   * @return expected SQL for math operation 15
   */
  protected String expectedSqlForMathOperations15() {
    return "a + (b + c) / 2";
  }


  /**
   * Test for proper SQL mathematics operation generation from DSL expressions.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperations16() {
    String result = testDialect.getSqlFrom(field("a").plus(field("b")).plus(field("c")).divideBy(literal(2)).plus(field("z")));
    assertEquals(expectedSqlForMathOperations16(), result);
  }


  /**
   * @return expected SQL for math operation 16
   */
  protected String expectedSqlForMathOperations16() {
    return "a + b + c / 2 + z";
  }


  /**
   * Regression test that checks if the DSL with Math expressions, that is used produces expected SQL.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperationsForExistingDataFix1() {
    Function dsl = round(field("doublevalue").divideBy(literal(1000)).multiplyBy(field("doublevalue")), literal(2));
    String sql = testDialect.getSqlFrom(dsl);
    assertEquals(expectedSqlForMathOperationsForExistingDataFix1(), sql);
  }


  /**
   * @return expected SQL for math operation for existing data fix 1
   */
  protected String expectedSqlForMathOperationsForExistingDataFix1() {
    return "ROUND(doublevalue / 1000 * doublevalue, 2)";
  }


  /**
   * Regression test that checks if the DSL with Math expressions, that is used
   * in Core and Aether modules produces expected SQL.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperationsForExistingDataFix2() {
    AliasedField dsl = floor(random().multiplyBy(new FieldLiteral(Math.pow(10, 6) - 1)));
    String sql = testDialect.getSqlFrom(dsl);
    assertEquals(expectedSqlForMathOperationsForExistingDataFix2(testDialect.getSqlForRandom()), sql);
  }


  /**
   * @param sqlForRandom SQL to create a random number
   * @return expected SQL for math operation for existing data fix 2
   */
  protected String expectedSqlForMathOperationsForExistingDataFix2(String sqlForRandom) {
    return "FLOOR(" + sqlForRandom + " * 999999.0)";
  }


  /**
   * Regression test that checks if the DSL with Math expressions, that is used
   * in ReportingSchema module produces expected SQL.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperationsForExistingDataFix3() {
    AliasedField dsl = max(field("assetLocationDate").multiplyBy(literal(100000)).plus(field("assetLocationTime")));
    String sql = testDialect.getSqlFrom(dsl);
    assertEquals(expectedSqlForMathOperationsForExistingDataFix3(), sql);
  }


  /**
   * @return the expected SQL for math operation for existing data fix 3
   */
  protected String expectedSqlForMathOperationsForExistingDataFix3() {
    return "MAX(assetLocationDate * 100000 + assetLocationTime)";
  }


  /**
   * Regression test that checks if the DSL with Math expressions produces expected SQL.
   *
   * Calling:
   *
   * <pre>
   * field(&quot;vatRate / (vatRate + 100)&quot;)
   * </pre>
   *
   * is actually a hack that was used as a workaround in order to create the
   * expected SQL below. Since
   * {@link org.alfasoftware.morf.sql.SqlUtils#bracket(MathsField)} is
   * available, it should be use to achieve the SQL bracketing.
   */
  @Test
  public void shouldGenerateCorrectSqlForMathOperationsForExistingDataFix4() {
    AliasedField dsl = field("invoiceLineReceived").multiplyBy(field("vatRate / (vatRate + 100)"));
    String sql = testDialect.getSqlFrom(dsl);
    assertEquals(expectedSqlForMathOperationsForExistingDataFix4(), sql);
  }


  /**
   * @return the expected SQL for math operation for existing data fix 4
   */
  protected String expectedSqlForMathOperationsForExistingDataFix4() {
    return "invoiceLineReceived * vatRate / (vatRate + 100)";
  }


  /**
   * Test that adding numbers returns as expected.
   */
  @Test
  public void testMathsMultiply() {
    String result = testDialect.getSqlFrom(new MathsField(new FieldLiteral(1), MathsOperator.MULTIPLY, new FieldLiteral(1)));
    assertEquals(expectedMathsMultiply(), result);
  }


  /**
   * Tests the output of a cast to a string.
   */
  @Test
  public void testCastToString() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.STRING, 10));
    assertEquals(expectedStringCast(), result);
  }


  /**
   * Tests the output of a cast of a function to a string.
   */
  @Test
  public void testCastFunctionToString() {
    String result = testDialect.getSqlFrom(cast(min(field("field"))).asString(8));
    assertEquals(expectedStringFunctionCast(), result);
  }


  /**
   * Tests the output of a cast to a big int.
   */
  @Test
  public void testCastToBigInt() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.BIG_INTEGER, 10));
    assertEquals(expectedBigIntCast(), result);
  }

  /**
   * Tests the output of a cast of a function to a big int.
   */
  @Test
  public void testCastFunctionToBigInt() {
    String result = testDialect.getSqlFrom(new Cast(min(field("value")), DataType.BIG_INTEGER, 10));
    assertEquals(expectedBigIntFunctionCast(), result);
  }


  /**
   * Tests the output of a cast to a boolean.
   */
  @Test
  public void testCastToBoolean() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.BOOLEAN, 10));
    assertEquals(expectedBooleanCast(), result);
  }


  /**
   * Tests the output of a cast to a date.
   */
  @Test
  public void testCastToDate() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.DATE, 10));
    assertEquals(expectedDateCast(), result);
  }


  /**
   * Tests the output of a cast to a date.
   */
  @Test
  public void testCastStringLiteralToInteger() {
    String result = testDialect.getSqlFrom(new Cast(new FieldLiteral("1234567890"), DataType.INTEGER, 10));
    assertEquals(expectedStringLiteralToIntegerCast(), result);
  }


  /**
   * Tests the output of a cast to a decimal.
   */
  @Test
  public void testCastToDecimal() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.DECIMAL, 10, 2));
    assertEquals(expectedDecimalCast(), result);
  }


  /**
   * Tests the output of a cast to an integer.
   */
  @Test
  public void testCastToInteger() {
    String result = testDialect.getSqlFrom(new Cast(new FieldReference("value"), DataType.INTEGER, 10));
    assertEquals(expectedIntegerCast(), result);
  }


  /**
   * Check that we can concatenate a number of string literals.
   */
  @Test
  public void testSelectWithMultipleLiteralFields() {
    SelectStatement stmt = new SelectStatement(new ConcatenatedField(new FieldLiteral("ABC"), new FieldLiteral(" "),
      new FieldLiteral("DEF")).as("assetDescription")).from(new TableReference("schedule"));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedConcatenationWithMultipleFieldLiterals(), result);
  }


  /**
   * Check that the optimiser hints work.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testHints() {
    assertEquals(
      expectedHints1(1000),
      testDialect.convertStatementToSQL(
        select()
        .from(new TableReference("SCHEMA2", "Foo"))
        .innerJoin(new TableReference("Bar"), field("a").eq(field("b")))
        .leftOuterJoin(new TableReference("Fo"), field("a").eq(field("b")))
        .innerJoin(new TableReference("Fum").as("Fumble"), field("a").eq(field("b")))
        .orderBy(field("a"))
        .useImplicitJoinOrder()
        .optimiseForRowCount(1000)
        .useIndex(new TableReference("SCHEMA2", "Foo"), "Foo_1")
        .useIndex(new TableReference("SCHEMA2", "Foo").as("aliased"), "Foo_2")
      )
    );
    assertEquals(
      expectedHints2(1000),
      testDialect.convertStatementToSQL(
        select(field("a"), field("b"))
        .from(tableRef("Foo"))
        .orderBy(field("a"))
        .forUpdate()
        .useIndex(tableRef("Foo"), "Foo_1")
        .optimiseForRowCount(1000)
        .useImplicitJoinOrder()
        .withParallelQueryPlan()
        .allowParallelDml()
        .withCustomHint(mock(CustomHint.class))
      )
    );
    assertEquals(
      expectedHints3(),
      testDialect.convertStatementToSQL(
        update(tableRef("Foo"))
        .set(field("b").as("a"))
        .useParallelDml()
      )
    );
    assertEquals(
       expectedHints3a(),
       testDialect.convertStatementToSQL(
         update(tableRef("Foo"))
         .set(field("b").as("a"))
         .useParallelDml(5)
       )
    );
    assertEquals(
      Lists.newArrayList(expectedHints4()),
      testDialect.convertStatementToSQL(
        insert()
        .into(tableRef("Foo"))
        .from(select(field("a"), field("b")).from(tableRef("Foo_1")))
        .useDirectPath()
      )
    );
    assertEquals(
        Lists.newArrayList(expectedHints4a()),
        testDialect.convertStatementToSQL(
          insert()
          .into(tableRef("Foo"))
          .from(select(field("a"), field("b")).from(tableRef("Foo_1")))
          .avoidDirectPath()
        )
    );
    assertEquals(
         Lists.newArrayList(expectedHints4b()),
         testDialect.convertStatementToSQL(
           insert()
           .into(tableRef("Foo"))
           .from(select(field("a"), field("b")).from(tableRef("Foo_1")))
           .useParallelDml()
         )
    );
    assertEquals(
         Lists.newArrayList(expectedHints4c()),
         testDialect.convertStatementToSQL(
           insert()
           .into(tableRef("Foo"))
           .from(select(field("a"), field("b")).from(tableRef("Foo_1")))
           .useParallelDml(5)
         )
    );
    assertEquals(
      Lists.newArrayList(expectedHints5()),
      testDialect.convertStatementToSQL(
        insert()
        .into(tableRef("Foo"))
        .from(select(field("a"), field("b")).from(tableRef("Foo_1")))
      )
    );
    assertEquals(
      expectedHints6(),
      testDialect.convertStatementToSQL(
        select(field("a"), field("b"))
        .from(tableRef("Foo"))
        .orderBy(field("a"))
        .withParallelQueryPlan(5)
      )
    );
    assertEquals(
      expectedHints6a(),
      testDialect.convertStatementToSQL(
        select(field("a"), field("b"))
        .from(tableRef("Foo"))
        .orderBy(field("a"))
        .withParallelQueryPlan(5)
        .allowParallelDml()
      )
    );
    assertEquals(
      expectedHints7(),
      testDialect.convertStatementToSQL(
        select()
        .from(new TableReference("SCHEMA2", "Foo"))
        .withCustomHint(provideCustomHint())
      )
    );

    assertEquals(
      expectedHints8(),
      testDialect.convertStatementToSQL(
        select()
        .from(new TableReference("SCHEMA2", "Foo"))
        .withCustomHint(() -> "CustomHint")
      )
    );

    assertEquals(
      expectedHints8a(),
      testDialect.convertStatementToSQL(
        select()
        .from(new TableReference("SCHEMA2", "Foo"))
        .withDialectSpecificHint(provideDatabaseType(), "index(customer cust_primary_key_idx)")
        .withDialectSpecificHint("SOMETHING_ELSE", "unused_hint()")
          )
        );
  }


  /**
   * This method can be overridden in specific dialects to test providing custom hints in each dialect
   * @return a mock CustomHint or an overridden, more specific, CustomHint
   */
  @SuppressWarnings("deprecation")
  protected CustomHint provideCustomHint() {
    return mock(CustomHint.class);
  }

  /**
   * This method can be overridden in specific dialects to test DialectSpecificHint in each dialect
   * @return a mock database type identifier value or an overridden, dialect specific, database type identfier
   */
  protected String provideDatabaseType() {
    return "SOME_DATABASE_IDENTIFIER";
  }


  /**
   * Check that we don't allow the use of the optimise for row count hint with a MERGE.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testOptimiseForRowCountOnMerge() {
    testDialect.convertStatementToSQL(
      merge()
      .into(tableRef("a"))
      .from(
        select()
        .from(tableRef("b"))
        .optimiseForRowCount(2)
      )
      .tableUniqueKey(field("id"))
    );
  }


  /**
   * Check that we don't allow the use of the use index hint with a MERGE.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUseIndexOnMerge() {
    testDialect.convertStatementToSQL(
      merge()
      .into(tableRef("a"))
      .from(
        select()
        .from(tableRef("b"))
        .useIndex(tableRef("b"), "b_1")
      )
      .tableUniqueKey(field("id"))
    );
  }


  /**
   * Check that we don't allow the use of the join order hint with a MERGE.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUseImplicitJoinOrderOnMerge() {
    testDialect.convertStatementToSQL(
      merge()
      .into(tableRef("a"))
      .from(
        select()
        .from(tableRef("b"))
        .useImplicitJoinOrder()
      )
      .tableUniqueKey(field("id"))
    );
  }


  /**
   * Check that we don't allow the use of the optimise for row count hint on a subquery.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testOptimiseForRowCountOnSubquery() {
    testDialect.convertStatementToSQL(
      select().from(select().from("Foo").optimiseForRowCount(1))
    );
  }


  /**
   * Check that we don't allow the use of the use index hint on a subquery.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUseIndexOnSubquery() {
    testDialect.convertStatementToSQL(
      select().from(select().from("Foo").useIndex(tableRef("Foo"), "Foo_1"))
    );
  }


  /**
   * Check that we don't allow the use of the join order hint on a subquery.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testUseImplicitJoinOrderOnSubquery() {
    testDialect.convertStatementToSQL(
      select().from(select().from("Foo").useImplicitJoinOrder())
    );
  }


  /**
   * Tests that substringing functionality works.
   */
  @Test
  public void testSubstring() {
    // Given
    Function substring = substring(new FieldReference("field1"), new FieldLiteral(1), new FieldLiteral(3));
    SelectStatement stmt = new SelectStatement(substring).from(new TableReference("schedule"));

    // When
    String result = testDialect.convertStatementToSQL(stmt);

    // Then
    assertEquals("Substring script should match expected", expectedSubstring(), result);
  }


  /**
   * Tests that Trim functionality works.
   */
  @Test
  public void testTrim() {
    // Given
    Function trim = trim(new FieldReference("field1"));
    SelectStatement selectStatement = new SelectStatement(trim).from(new TableReference("schedule"));

    // When
    String result = testDialect.convertStatementToSQL(selectStatement);

    // Then
    assertEquals("Trim script should match expected", expectedTrim(), result);
  }


  /**
   * Tests that Left Trim functionality works.
   */
  @Test
  public void testLeftTrim() {
    // Given
    Function leftTrim = leftTrim(new FieldReference("field1"));
    SelectStatement selectStatement = new SelectStatement(leftTrim).from(new TableReference("schedule"));

    // When
    String result = testDialect.convertStatementToSQL(selectStatement);

    // Then
    assertEquals("Left Trim script should match expected", expectedLeftTrim(), result);
  }


  /**
   * Tests that Right Trim functionality works.
   */
  @Test
  public void testRightTrim() {
    // Given
    Function rightTrim = rightTrim(new FieldReference("field1"));
    SelectStatement selectStatement = new SelectStatement(rightTrim).from(new TableReference("schedule"));

    // When
    String result = testDialect.convertStatementToSQL(selectStatement);

    // Then
    assertEquals("Right Trim script should match expected", expectedRightTrim(), result);
  }


  /**
   * Tests that the Left pad works.
   */
  @Test
  public void testGetSqlForLeftPad() {
    // Given
    Function leftPad = leftPad(new FieldReference(STRING_FIELD), new FieldLiteral(10), new FieldLiteral("j"));
    SelectStatement leftPadStatement = new SelectStatement(leftPad).from(new TableReference(TEST_TABLE));

    // When
    String result = testDialect.convertStatementToSQL(leftPadStatement);

    // Then
    assertEquals("Left pad script must match the expected", expectedLeftPad(), result);
  }


  /**
   * Tests that the right pad works.
   */
  @Test
  public void testGetSqlForRightPad() {
    // Given
    Function rightPad = rightPad(new FieldReference(STRING_FIELD), new FieldLiteral(10), new FieldLiteral("j"));
    SelectStatement rightPadStatement = new SelectStatement(rightPad).from(new TableReference(TEST_TABLE));

    // When
    String result = testDialect.convertStatementToSQL(rightPadStatement);

    // Then
    assertEquals("Right pad script must match the expected", expectedRightPad(), result);
  }

  /**
   * Tests the random function
   */
  @Test
  public void testRandom() {
    String result = testDialect.convertStatementToSQL(select(random()).from(tableRef("NEW1")));

    assertEquals("Random script should match expected", "SELECT " + expectedRandomFunction() + " FROM " + tableName("NEW1"), result);
  }


  /**
   * Tests that random string functionality builds the expected SQL string.
   */
  @Test
  public void testRandomString() {
    SelectStatement statement = new SelectStatement(randomString(new FieldLiteral(10))).from(new TableReference(
        TEST_TABLE));
    String actual = testDialect.convertStatementToSQL(statement);
    assertEquals("Random string script should match expected", "SELECT " + expectedRandomString() + " FROM " + tableName(TEST_TABLE), actual);
  }


  /**
   * Tests that LOWER functionality works.
   */
  @Test
  public void testLower() {
    SelectStatement statement = new SelectStatement(lowerCase(new FieldReference("field1"))).from(new TableReference(
        "schedule"));
    String actual = testDialect.convertStatementToSQL(statement);
    assertEquals("LowerCase script should match expected", expectedLower(), actual);
  }


  /**
   * Tests that UPPER functionality works.
   */
  @Test
  public void testUpper() {
    SelectStatement statement = new SelectStatement(upperCase(new FieldReference("field1"))).from(new TableReference(
        "schedule"));
    String actual = testDialect.convertStatementToSQL(statement);
    assertEquals("UpperCase script should match expected", expectedUpper(), actual);
  }


  /**
   * Tests that FLOOR functionality builds the expected SQL string.
   */
  @Test
  public void testFloor() {
    SelectStatement statement = new SelectStatement(floor(new FieldReference(FLOAT_FIELD))).from(new TableReference(
        TEST_TABLE));
    String actual = testDialect.convertStatementToSQL(statement);
    assertEquals("Floor script should match expected", expectedFloor(), actual);
  }


  /**
   * Tests that POWER functionality builds the expected SQL string.
   */
  @Test
  public void testPower() {
    SelectStatement statement = new SelectStatement(power(new FieldReference(FLOAT_FIELD), new FieldReference(INT_FIELD))).from(new TableReference(
        TEST_TABLE));
    String actual = testDialect.convertStatementToSQL(statement);
    assertEquals("Power script should match expected", expectedPower(), actual);
  }


  /**
   * Utility method for testing 'ALTER TABLE ... COLUMN ...' statements.
   */
  @SuppressWarnings("unchecked")
  private void testAlterTableColumn(String tableName, AlterationType alterationType, Column oldColumn, Column newColumn, List<String> expectedStatements) {
    Table modifiedTable;

    Collection<String> actualStatements;
    switch (alterationType) {
      case ADD:
        modifiedTable = new AddColumn(tableName, newColumn).apply(metadata).getTable(tableName);

        actualStatements = testDialect.alterTableAddColumnStatements(modifiedTable, newColumn);
        break;
      case ALTER:
        modifiedTable = new ChangeColumn(tableName, oldColumn, newColumn).apply(metadata).getTable(tableName);

        actualStatements = testDialect.alterTableChangeColumnStatements(modifiedTable, oldColumn, newColumn);
        break;
      case DROP:
        modifiedTable = new RemoveColumn(tableName, oldColumn).apply(metadata).getTable(tableName);

        actualStatements = testDialect.alterTableDropColumnStatements(modifiedTable, oldColumn);
        break;
      default:
        throw new UnsupportedOperationException(alterationType.toString());
    }
    compareStatements(expectedStatements, actualStatements);
  }


  /**
   * Utility method for testing 'ALTER TABLE ... COLUMN ...' statements.
   */
  private void testAlterTableColumn(AlterationType alterationType, Column newColumn, List<String> expectedStatements) {
    testAlterTableColumn(TEST_TABLE, alterationType, null, newColumn, expectedStatements);
  }


  /**
   * Utility method to get a column from the 'Test' table based on its name.
   */
  private Column getColumn(String tableName, String columnName) {
    for (Column column : metadata.getTable(tableName).columns()) {
      if (column.getName().equals(columnName)) {
        return column;
      }
    }
    return null;
  }


  /**
   * Test adding an integer column.
   */
  @Test
  public void testAddIntegerColumn() {
    testAlterTableColumn(AlterationType.ADD, column("intField_new", DataType.INTEGER).nullable(), expectedAlterTableAddIntegerColumnStatement());
  }


  /**
   * Test altering an integer column.
   */
  @Test
  public void testAlterIntegerColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, INT_FIELD), column(INT_FIELD, DataType.INTEGER), expectedAlterTableAlterIntegerColumnStatement());
  }


  /**
   * Test adding a string column.
   */
  @Test
  public void testAddStringColumn() {
    testAlterTableColumn(AlterationType.ADD, column("stringField_new", DataType.STRING, 6).nullable(), expectedAlterTableAddStringColumnStatement());
  }

  /**
   * Test adding a string column.
   */
  @Test
  public void testAddStringColumnWithDefault() {
    testAlterTableColumn(AlterationType.ADD, column("stringField_with_default", DataType.STRING, 6).defaultValue("N"), expectedAlterTableAddStringColumnWithDefaultStatement());
  }


  /**
   * Test altering a string column.
   */
  @Test
  public void testAlterStringColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, STRING_FIELD), column(STRING_FIELD, DataType.STRING, 6).nullable(), expectedAlterTableAlterStringColumnStatement());
  }


  /**
   * Test adding boolean column.
   */
  @Test
  public void testAddBooleanColumn() {
    testAlterTableColumn(AlterationType.ADD, column("booleanField_new", DataType.BOOLEAN).nullable(), expectedAlterTableAddBooleanColumnStatement());
  }


  /**
   * Test altering a boolean column.
   */
  @Test
  public void testAlterBooleanColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, BOOLEAN_FIELD), column(BOOLEAN_FIELD, DataType.BOOLEAN), expectedAlterTableAlterBooleanColumnStatement());
  }


  /**
   * Test adding a date column.
   */
  @Test
  public void testAddDateColumn() {
    testAlterTableColumn(AlterationType.ADD, column("dateField_new", DataType.DATE).nullable(), expectedAlterTableAddDateColumnStatement());
  }


  /**
   * Test altering a date column.
   */
  @Test
  public void testAlterDateColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, DATE_FIELD), column(DATE_FIELD, DataType.DATE), expectedAlterTableAlterDateColumnStatement());
  }


  /**
   * Test adding a floating point column.
   */
  @Test
  public void testAddDecimalColumn() {
    testAlterTableColumn(AlterationType.ADD, column("floatField_new", DataType.DECIMAL, 6, 3).nullable(), expectedAlterTableAddDecimalColumnStatement());
  }


  /**
   * Test altering a floating point column.
   */
  @Test
  public void testAlterDecimalColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, FLOAT_FIELD), column(FLOAT_FIELD, DataType.DECIMAL, 14, 3).nullable(), expectedAlterTableAlterDecimalColumnStatement());
  }


  /**
   * Test adding a big integer column.
   */
  @Test
  public void testAddBigIntegerColumn() {
    testAlterTableColumn(AlterationType.ADD, column("bigIntegerField_new", DataType.BIG_INTEGER).nullable(), expectedAlterTableAddBigIntegerColumnStatement());
  }


  /**
   * Test altering a big integer column.
   */
  @Test
  public void testAlterBigIntegerColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, BIG_INTEGER_FIELD), column(BIG_INTEGER_FIELD, DataType.BIG_INTEGER).nullable(), expectedAlterTableAlterBigIntegerColumnStatement());
  }


  /**
   * Test adding a blob column.
   */
  @Test
  public void testAddBlobColumn() {
    testAlterTableColumn(AlterationType.ADD, column("blobField_new", DataType.BLOB).nullable(), expectedAlterTableAddBlobColumnStatement());
  }


  /**
   * Test altering a blob column.
   */
  @Test
  public void testAlterBlobColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, BLOB_FIELD), column(BLOB_FIELD, DataType.BLOB), expectedAlterTableAlterBlobColumnStatement());
  }


  /**
   * Test adding a non-nullable column.
   */
  @Test
  public void testAddColumnNotNullable() {
    testAlterTableColumn(AlterationType.ADD, column("dateField_new", DataType.DATE).defaultValue("2010-01-01"), expectedAlterTableAddColumnNotNullableStatement());
  }


  /**
   * Test changing a nullable column to a non-nullable one.
   */
  @Test
  public void testAlterColumnFromNullableToNotNullable() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, DATE_FIELD), column(DATE_FIELD, DataType.DATE), expectedAlterTableAlterColumnFromNullableToNotNullableStatement());
  }


  /**
   * Test changing a non-nullable column to a non-nullable column (i.e. alter column statement without leaving nullability set to <code>false</code>.
   */
  @Test
  public void testAlterColumnFromNotNullableToNotNullable() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, FLOAT_FIELD), column(FLOAT_FIELD, DataType.DECIMAL, 20, 3), expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement());
  }


  /**
   * Test changing a column from not nullable to a nullable one.
   */
  @Test
  public void testAlterColumnFromNotNullableToNullable() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, FLOAT_FIELD), column(FLOAT_FIELD, DataType.DECIMAL, 20, 3).nullable(), expectedAlterTableAlterColumnFromNotNullableToNullableStatement());
  }


  /**
   * Test renaming a column and changing it from nullable to not nullable.
   */
  @Test
  public void testAlterColumnRenamingAndChangingNullability() {
    testAlterTableColumn(OTHER_TABLE, AlterationType.ALTER, getColumn(OTHER_TABLE, FLOAT_FIELD), column("blahField", DataType.DECIMAL, 20, 3).nullable(), expectedAlterColumnRenamingAndChangingNullability());
  }


  /**
   * Test renaming a column, changing the case only (e.g. from columnName to ColumnName).
   */
  @Test
  public void testAlterColumnChangingTypeAndCase() {
    testAlterTableColumn(OTHER_TABLE, AlterationType.ALTER, getColumn(OTHER_TABLE, FLOAT_FIELD), column("FloatField", DataType.DECIMAL, 20, 3), expectedAlterColumnChangingLengthAndCase());
  }


  /**
   * Test adding a column with default value.
   */
  @Test
  public void testAddColumnWithDefault() {
    testAlterTableColumn(AlterationType.ADD, column("floatField_new", DataType.DECIMAL, 6, 3).nullable().defaultValue("20.33"), expectedAlterTableAddColumnWithDefaultStatement());
  }


  /**
   * Test changing a column to have a default value.
   */
  @Test
  public void testAlterColumnWithDefault() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, BIG_INTEGER_FIELD), column(BIG_INTEGER_FIELD, DataType.BIG_INTEGER).nullable().defaultValue("54321"), expectedAlterTableAlterColumnWithDefaultStatement());
  }


  /**
   * Test changing a column to have a default value.
   */
  @Test
  public void testDropColumnWithDefault() {
    testAlterTableColumn(TEST_TABLE, AlterationType.DROP, getColumn(TEST_TABLE, BIG_INTEGER_FIELD), null, expectedAlterTableDropColumnWithDefaultStatement());
  }


  /**
   * Tests that after changing an index, column from that index can be changed afterwards.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testChangeIndexFollowedByChangeOfAssociatedColumn() {
    Schema schema;

    // alter an index
    // note the different case
    ChangeIndex changeIndex = new ChangeIndex(TEST_TABLE,
      index(TEST_1).columns(INT_FIELD, FLOAT_FIELD).unique(),
      index(TEST_1).columns("INTFIELD"));
    schema = changeIndex.apply(metadata);
    Table tableAfterChangeIndex = schema.getTable(TEST_TABLE);
    Collection<String> dropIndexStatements = testDialect.indexDropStatements(tableAfterChangeIndex, index(TEST_1).columns(INT_FIELD, FLOAT_FIELD).unique());
    Collection<String> addIndexStatements = testDialect.addIndexStatements(tableAfterChangeIndex, index(TEST_1).columns(INT_FIELD));

    // then alter a column in that index
    ChangeColumn changeColumn = new ChangeColumn(TEST_TABLE,
      column(INT_FIELD, DataType.INTEGER).nullable(),
      column(INT_FIELD, DataType.INTEGER));
    schema = changeColumn.apply(schema);
    Table tableAfterModifyColumn = schema.getTable(TEST_TABLE);
    Collection<String> changeColumnStatements = testDialect.alterTableChangeColumnStatements(tableAfterModifyColumn,
      column(INT_FIELD, DataType.INTEGER).nullable(),
      column(INT_FIELD, DataType.INTEGER));

    compareStatements(expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement(),
      dropIndexStatements, addIndexStatements, changeColumnStatements);
  }


  /**
   * Test changing column to be the primary key.
   */
  @Test
  public void testAlterColumnMakePrimary() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, DATE_FIELD), column(DATE_FIELD, DataType.DATE).nullable().primaryKey(), expectedAlterColumnMakePrimaryStatements());
  }

  /**
   *  Test renaming an indexed non-primary key column
   */
  @Test
  public void testAlterColumnRenameNonPrimaryIndexedColumn() {
    testAlterTableColumn(ALTERNATE_TABLE, AlterationType.ALTER, getColumn(ALTERNATE_TABLE, STRING_FIELD), column("blahField", DataType.STRING, 3).nullable(), expectedAlterColumnRenameNonPrimaryIndexedColumn());
  }



  /**
   * Test changing a column which is part of a composite primary key.
   */
  @Test
  public void testAlterPrimaryKeyColumnCompositeKey() {
    testAlterTableColumn(COMPOSITE_PRIMARY_KEY_TABLE, AlterationType.ALTER, getColumn(COMPOSITE_PRIMARY_KEY_TABLE, SECOND_PRIMARY_KEY),
      column(SECOND_PRIMARY_KEY, DataType.STRING, 5).primaryKey(), expectedAlterPrimaryKeyColumnCompositeKeyStatements());
  }


  /**
   * Test changing a column to remove it from a composite primary key.
   */
  @Test
  public void testAlterRemoveColumnFromCompositeKey() {
    testAlterTableColumn(COMPOSITE_PRIMARY_KEY_TABLE,
      AlterationType.ALTER,
      getColumn(COMPOSITE_PRIMARY_KEY_TABLE, SECOND_PRIMARY_KEY),
      column(SECOND_PRIMARY_KEY, DataType.STRING, 5).nullable(),
      expectedAlterRemoveColumnFromCompositeKeyStatements());
  }


  /**
   * Test changing a column which is the primary key.
   */
  @Test
  public void testAlterPrimaryKeyColumn() {
    testAlterTableColumn(TEST_TABLE, AlterationType.ALTER, getColumn(TEST_TABLE, "id"), column("renamedId", DataType.BIG_INTEGER).primaryKey(),
      expectedAlterPrimaryKeyColumnStatements());
  }


  /**
   * Tests removing the simple primary key column.
   */
  @Test
  public void testRemoveSimplePrimaryKeyColumn() {
    testAlterTableColumn(TEST_TABLE,
      AlterationType.DROP,
      getColumn(TEST_TABLE, "id"), null,
      expectedAlterRemoveColumnFromSimpleKeyStatements());
  }


  /**
   * Test adding an index over a single column.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testAddIndexStatementsOnSingleColumn() {
    Table table = metadata.getTable(TEST_TABLE);
    Index index = index("indexName").columns(table.columns().get(0).getName());
    compareStatements(
      expectedAddIndexStatementsOnSingleColumn(),
      testDialect.addIndexStatements(table, index));
  }


  /**
   * Test adding an index over multiple columns.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testAddIndexStatementsOnMultipleColumns() {
    Table table = metadata.getTable(TEST_TABLE);
    Index index = index("indexName").columns(table.columns().get(0).getName(), table.columns().get(1).getName());
    compareStatements(
      expectedAddIndexStatementsOnMultipleColumns(),
      testDialect.addIndexStatements(table, index));
  }


  /**
   * Test adding a unique index.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testAddIndexStatementsUnique() {
    Table table = metadata.getTable(TEST_TABLE);
    Index index = index("indexName").unique().columns(table.columns().get(0).getName());
    compareStatements(
      expectedAddIndexStatementsUnique(),
      testDialect.addIndexStatements(table, index));
  }


  /**
   * Test adding a unique index.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testAddIndexStatementsUniqueNullable() {
    Table table = metadata.getTable(TEST_TABLE);
    Index index = index("indexName").unique().columns(STRING_FIELD, INT_FIELD, FLOAT_FIELD, DATE_FIELD);
    compareStatements(
      expectedAddIndexStatementsUniqueNullable(),
      testDialect.addIndexStatements(table, index));
  }


  /**
   * Test dropping an index.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testIndexDropStatements() {
    Table table = metadata.getTable(TEST_TABLE);
    Index index = index("indexName").unique().columns(table.columns().get(0).getName());
    compareStatements(
      expectedIndexDropStatements(),
      testDialect.indexDropStatements(table, index));
  }


  /**
   * Tests that the syntax is correct for renaming a table.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testRenameTableStatements() {
    Table fromTable = metadata.getTable(TEST_TABLE);

    Table renamed = table("Renamed")
    .columns(
      idColumn(),
      versionColumn(),
      column(STRING_FIELD, DataType.STRING, 3).nullable(),
      column(INT_FIELD, DataType.DECIMAL, 8).nullable(),
      column(FLOAT_FIELD, DataType.DECIMAL, 13, 2),
      column(DATE_FIELD, DataType.DATE).nullable(),
      column(BOOLEAN_FIELD, DataType.BOOLEAN).nullable(),
      column(CHAR_FIELD, DataType.STRING, 1).nullable(),
      column(BLOB_FIELD, DataType.BLOB).nullable(),
      column(BIG_INTEGER_FIELD, DataType.BIG_INTEGER).nullable().defaultValue("12345"),
      column(CLOB_FIELD, DataType.CLOB).nullable()
        ).indexes(
          index(TEST_NK).unique().columns(STRING_FIELD),
          index(TEST_1).columns(INT_FIELD, FLOAT_FIELD).unique()
            );

    compareStatements(expectedRenameTableStatements(), testDialect.renameTableStatements(fromTable, renamed));
  }


  /**
   * Tests that the syntax is correct for renaming a table which has a long name.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testRenamingTableWithLongName() {

    String tableNameOver30 = "123456789012345678901234567890X";
    String indexNameOver30     = "123456789012345678901234567890X_PK";

    Table longNamedTable = table(tableNameOver30)
        .columns(
          idColumn(),
          versionColumn(),
          column("someField", DataType.STRING, 3).nullable()
       ).indexes(
          index(indexNameOver30).unique().columns("someField")
       );

    Table renamedTable = table("Blah")
        .columns(
          idColumn(),
          versionColumn(),
          column("someField", DataType.STRING, 3).nullable()
       ).indexes(
          index("Blah_PK").unique().columns("someField")
       );

    compareStatements(getRenamingTableWithLongNameStatements(), getTestDialect().renameTableStatements(longNamedTable, renamedTable));
  }


  /**
   * @return the expected statements for renaming a table with a long name.
   */
  protected abstract List<String> getRenamingTableWithLongNameStatements();


  /**
   * Tests that the syntax is correct for renaming an index.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testRenameIndexStatements() {
    AlteredTable alteredTable = new AlteredTable(testTable, null, null,
      FluentIterable.from(testTable.indexes()).transform(Index::getName).filter(i -> !i.equals(TEST_1)).append(TEST_2),
      ImmutableList.of(index(TEST_2).columns(INT_FIELD, FLOAT_FIELD).unique())
    );

    compareStatements(expectedRenameIndexStatements(), testDialect.renameIndexStatements(alteredTable, TEST_1, TEST_2));
  }


  /**
   * Tests that the syntax is correct for renaming an index.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testRenameTempIndexStatements() {
    AlteredTable alteredTable = new AlteredTable(testTempTable, null, null,
      FluentIterable.from(testTempTable.indexes()).transform(Index::getName).filter(i -> !i.equals(TEMP_TEST_1)).append(TEMP_TEST_2),
      ImmutableList.of(index(TEMP_TEST_2).columns(INT_FIELD, FLOAT_FIELD))
    );

    compareStatements(expectedRenameTempIndexStatements(), testDialect.renameIndexStatements(alteredTable, TEMP_TEST_1, TEMP_TEST_2));
  }


  /**
   * Tests that the analyse table statement
   */
  @Test
  public void testAnalyseTableStatement() {
    assertEquals("Analyse table scripts are not the same ", expectedAnalyseTableSql(), testDialect.getSqlForAnalyseTable(testTempTable));
  }

  /**
   * Tests a simple merge.
   */
  @Test
  public void testMergeSimple() {

    TableReference foo = new TableReference("foo").as("foo");
    TableReference somewhere = new TableReference("somewhere");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id"), somewhere.field("newBar").as("bar")).from(somewhere).alias("somewhere");

    MergeStatement stmt = new MergeStatement().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt);

    assertEquals("Select scripts are not the same", expectedMergeSimple(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Tests a more complex merge.
   */
  @Test
  public void testMergeComplex() {

    TableReference foo = new TableReference("foo").as("foo");
    TableReference somewhere = new TableReference("somewhere");
    TableReference join = new TableReference("join");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id"), join.field("joinBar").as("bar")).from(somewhere).innerJoin(join, eq(somewhere.field("newId"), join.field("joinId"))).alias("alias");

    MergeStatement stmt = new MergeStatement().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt);

    assertEquals("Select scripts are not the same", expectedMergeComplex(), testDialect.convertStatementToSQL(stmt));
  }


  @Test
  public void testMergeSourceInDifferentSchema() {
    TableReference foo = new TableReference("foo").as("foo");
    TableReference somewhere = new TableReference("MYSCHEMA", "somewhere");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id"), somewhere.field("newBar").as("bar")).from(somewhere).alias("somewhere");

    MergeStatement stmt = new MergeStatement().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt);

    assertEquals("Select scripts are not the same", expectedMergeSourceInDifferentSchema(), testDialect.convertStatementToSQL(stmt));
  }


  @Test
  public void testMergeTargetInDifferentSchema() {
    TableReference foo = new TableReference("MYSCHEMA", "foo").as("foo");
    TableReference somewhere = new TableReference("somewhere");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id"), somewhere.field("newBar").as("bar")).from(somewhere).alias("somewhere");

    MergeStatement stmt = new MergeStatement().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt);

    assertEquals("Select scripts are not the same", expectedMergeTargetInDifferentSchema(), testDialect.convertStatementToSQL(stmt));
  }


  /**
   * Ensure that we can merge into a table where all columns are in the primary key.
   */
  @Test
  public void testMergeWhenAllFieldsInPrimaryKey() {
    TableReference foo = new TableReference("foo").as("foo");
    TableReference somewhere = new TableReference("somewhere");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id")).from(somewhere).alias("somewhere");

    MergeStatement stmt = new MergeStatement().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt);

    assertEquals("Merge scripts are not the same", expectedMergeForAllPrimaryKeys(), testDialect.convertStatementToSQL(stmt));
  }


  @Test
  public void testMergeWithUpdateExpressions() {

    TableReference foo = new TableReference("foo").as("foo");
    TableReference somewhere = new TableReference("somewhere");

    SelectStatement sourceStmt = new SelectStatement(somewhere.field("newId").as("id"), somewhere.field("newBar").as("bar")).from(somewhere).alias("somewhere");

    MergeStatement stmt = MergeStatement.merge().into(foo).tableUniqueKey(foo.field("id")).from(sourceStmt)
        .ifUpdating((overrides, values) -> overrides
          .set(values.input("bar").plus(values.existing("bar")).as("bar")))
        .build();

    assertEquals("Select scripts are not the same", expectedMergeWithUpdateExpressions(), testDialect.convertStatementToSQL(stmt));
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testAddTableFromStatements() {

    Table table = table("SomeTable")
        .columns(
          column("someField", DataType.STRING, 3).primaryKey(),
          column("otherField", DataType.DECIMAL, 3)
       ).indexes(
          index("SomeTable_1").columns("otherField")
       );

    SelectStatement selectStatement = select(field("someField"), field("otherField")).from(tableRef("OtherTable"));

    compareStatements(expectedAddTableFromStatements(), getTestDialect().addTableFromStatements(table, selectStatement));
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testReplaceTableFromStatements() {
    Table table = table("SomeTable")
        .columns(
            column("someField", DataType.STRING, 3).primaryKey(),
            column("otherField", DataType.DECIMAL, 3),
            column("thirdField", DataType.DECIMAL, 5)
        ).indexes(
            index("SomeTable_1").columns("otherField")
        );

    Table tableWithAutonumber = table("SomeTable")
        .columns(
            column("someField", DataType.STRING, 3).primaryKey(),
            column("otherField", DataType.DECIMAL, 3).autoNumbered(1),
            column("thirdField", DataType.DECIMAL, 5)
        ).indexes(
            index("SomeTable_1").columns("otherField")
        );

    SelectStatement selectStatement = select(
        field("someField"),
        field("otherField"),
        cast(field("thirdField")).asType(DataType.DECIMAL, 5).as("thirdField"))
        .from(tableRef("OtherTable"));

    compareStatements(expectedReplaceTableFromStatements(), getTestDialect().replaceTableFromStatements(table, selectStatement));
    compareStatements(expectedReplaceTableWithAutonumber(), getTestDialect().replaceTableFromStatements(tableWithAutonumber, selectStatement));
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testReplaceTableSelectStatementValidation() {
    Table table = table("SomeTable")
            .columns(
                    column("someField", DataType.STRING, 3).primaryKey(),
                    column("otherField", DataType.DECIMAL, 3),
                    column("thirdField", DataType.DECIMAL, 5)
            ).indexes(
                    index("SomeTable_1").columns("otherField")
            );

    SelectStatement withIncorrectFieldCount = select(
            field("someField"),
            field("otherField"))
            .from(tableRef("OtherTable"));

    SelectStatement withIncorrectFieldName = select(
            field("someField"),
            field("otherField"),
            field("wrongField"))
            .from(tableRef("OtherTable"));


    IllegalArgumentException fieldCount =  assertThrows(IllegalArgumentException.class, () -> compareStatements(expectedReplaceTableFromStatements(), getTestDialect().replaceTableFromStatements(table, withIncorrectFieldCount)));
    IllegalArgumentException fieldName = assertThrows(IllegalArgumentException.class, () -> compareStatements(expectedReplaceTableFromStatements(), getTestDialect().replaceTableFromStatements(table, withIncorrectFieldName)));

    assertEquals("Number of table columns [3] does not match number of select columns [2].", fieldCount.getMessage());
    assertEquals("Table columns do not match select columns\n"
            + "Mismatching pairs:\n"
            + "    thirdField <> wrongField", fieldName.getMessage());
  }


  /**
   * On some databases our string literals need prefixing with N to be
   * correctly typed as a unicode string.
   *
   * @return prefix to insert before quoted string literal.
   */
  protected String stringLiteralPrefix() {
    return "";
  }


  /**
   * On some databases our string literals need suffixing with explicit escape
   * character key word.
   *
   * @return suffix to insert after quoted string literal.
   */
  protected String likeEscapeSuffix() {
    return " ESCAPE '\\'";
  }


  /**
   * On HSqlDb databases varchar casting will be necessary to prevent unwanted padding.
   *
   * @param value The string to cast
   * @return value cast to varchar if needed.
   */
  protected abstract String varCharCast(String value);


  /**
   * @return Expected SQL for {@link #testSelectOrderByNullsLastScript()}
   */
  protected String expectedSelectOrderByNullsLast() {
    return "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField NULLS LAST";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByNullsFirstDescendingScript()}
   */
  protected String expectedSelectOrderByNullsFirstDesc() {
    return "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField DESC NULLS FIRST";
  }


  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  protected String expectedSelectOrderByTwoFields() {
    return "SELECT stringField1, stringField2 FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField1 DESC NULLS FIRST, stringField2 NULLS LAST";
  }

  /**
   * @return Expected SQL for {@link #testSelectOrderByTwoFields()}
   */
  protected String expectedSelectFirstOrderByNullsLastDesc() {
    return "SELECT stringField FROM " + tableName(ALTERNATE_TABLE) + " ORDER BY stringField DESC NULLS LAST LIMIT 0,1";
  }


  /**
   * @return Expected SQL for {@link #testAlterBooleanColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterBooleanColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddBooleanColumn()}
   */
  protected abstract List<String> expectedAlterTableAddBooleanColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddStringColumn()}
   */
  protected abstract List<String> expectedAlterTableAddStringColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddStringColumnWithDefault()}
   */
  protected abstract List<String> expectedAlterTableAddStringColumnWithDefaultStatement();


  /**
   * @return Expected SQL for {@link #testAlterStringColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterStringColumnStatement();

  /**
   * @return Expected SQL for {@link #testAddIntegerColumn()}
   */
  protected abstract List<String> expectedAlterTableAddIntegerColumnStatement();


  /**
   * @return Expected SQL for {@link #testAlterIntegerColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterIntegerColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddDateColumn()}
   */
  protected abstract List<String> expectedAlterTableAddDateColumnStatement();


  /**
   * @return Expected SQL for {@link #testAlterDateColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterDateColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddDecimalColumn()}
   */
  protected abstract List<String> expectedAlterTableAddDecimalColumnStatement();


  /**
   * @return Expected SQL for {@link #testAlterDecimalColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterDecimalColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddBigIntegerColumn()}
   */
  protected abstract List<String> expectedAlterTableAddBigIntegerColumnStatement();


  /**
   * @return Expected SQL for {@link #testAlterBigIntegerColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterBigIntegerColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddBlobColumn()}
   */
  protected abstract List<String> expectedAlterTableAddBlobColumnStatement();


  /**
   * @return Expected SQL for {@link #testAlterBlobColumn()}
   */
  protected abstract List<String> expectedAlterTableAlterBlobColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddColumnNotNullable()}
   */
  protected abstract List<String> expectedAlterTableAddColumnNotNullableStatement();


  /**
   * @return Expected SQL for {@link #testAlterColumnFromNullableToNotNullable()}
   */
  protected abstract List<String> expectedAlterTableAlterColumnFromNullableToNotNullableStatement();

  /**
   * @return Expected SQL for {@link #testAlterColumnFromNotNullableToNotNullable()}
   */
  protected abstract List<String> expectedAlterTableAlterColumnFromNotNullableToNotNullableStatement();


  /**
   * @return Expected SQL for {@link #testAlterColumnFromNotNullableToNullable()}
   */
  protected abstract List<String> expectedAlterTableAlterColumnFromNotNullableToNullableStatement();


  /**
   * @return Expected SQL for {@link #testAlterColumnRenamingAndChangingNullability()}.
   */
  protected abstract List<String> expectedAlterColumnRenamingAndChangingNullability();


  /**
   * @return Expected SQL for {@link #testAlterColumnChangingTypeAndCase()}.
   */
  protected abstract List<String> expectedAlterColumnChangingLengthAndCase();


  /**
   * @return Expected SQL for {@link #testAddColumnWithDefault()}
   */
  protected abstract List<String> expectedAlterTableAddColumnWithDefaultStatement();


  /**
   * @return Expected SQL for {@link #testAlterColumnWithDefault()}
   */
  protected abstract List<String> expectedAlterTableAlterColumnWithDefaultStatement();


  /**
   * @return Expected SQL for {@link #testDropColumnWithDefault()}
   */
  protected List<String> expectedAlterTableDropColumnWithDefaultStatement() {
    return singletonList("ALTER TABLE Test DROP COLUMN bigIntegerField");
  }


  /**
   * @return Expected SQL for {@link #testChangeIndexFollowedByChangeOfAssociatedColumn()}
   */
  protected abstract List<String> expectedChangeIndexFollowedByChangeOfAssociatedColumnStatement();


  /**
   * @return Expected SQL for {@link #testAddIndexStatementsOnSingleColumn()}
   */
  protected abstract List<String> expectedAddIndexStatementsOnSingleColumn();


  /**
   * @return Expected SQL for {@link #testAddIndexStatementsOnMultipleColumns()}
   */
  protected abstract List<String> expectedAddIndexStatementsOnMultipleColumns();


  /**
   * @return Expected SQL for {@link #testAddIndexStatementsUnique()}
   */
  protected abstract List<String> expectedAddIndexStatementsUnique();


  /**
   * @return Expected SQL for {@link #testAddIndexStatementsUniqueNullable()}
   */
  protected abstract List<String> expectedAddIndexStatementsUniqueNullable();


  /**
   * @return Expected SQL for {@link #testIndexDropStatements()}
   */
  protected abstract List<String> expectedIndexDropStatements();


  /**
   * @return Expected SQL for {@link #testRenameTableStatements()}
   */
  protected abstract List<String> expectedRenameTableStatements();


  /**
   * @return Expected SQL for {@link #testRenameIndexStatements()}
   */
  protected abstract List<String> expectedRenameIndexStatements();


  /**
   * @return Expected SQL for {@link #testRenameTempIndexStatements()}
   */
  protected abstract List<String> expectedRenameTempIndexStatements();


  /**
   * @return Expected SQL for {@link #testAnalyseTableStatement()}
   */
  protected abstract Collection<String> expectedAnalyseTableSql();


  /**
   * @return Expected SQL for {@link #testAlterColumnMakePrimary()}
   */
  protected abstract List<String> expectedAlterColumnMakePrimaryStatements();


  /**
   * @return Expected SQL for {@link #testAlterColumnRenameNonPrimaryIndexedColumn()}
   */
  protected abstract List<String> expectedAlterColumnRenameNonPrimaryIndexedColumn();

  /**
   * @return Expected SQL for {@link #testAlterPrimaryKeyColumnCompositeKey()}
   */
  protected abstract List<String> expectedAlterPrimaryKeyColumnCompositeKeyStatements();


  /**
   * @return Expected SQL for {@link #testAlterRemoveColumnFromCompositeKey()}
   */
  protected abstract List<String> expectedAlterRemoveColumnFromCompositeKeyStatements();

  /**
   * @return Expected SQL for {@link #testRemoveSimplePrimaryKeyColumn()}.
   */
  protected abstract List<String> expectedAlterRemoveColumnFromSimpleKeyStatements();

  /**
   * @return Expected SQL for {@link #testAlterPrimaryKeyColumn()}
   */
  protected abstract List<String> expectedAlterPrimaryKeyColumnStatements();


  /**
   * Expected outcome for calling {@link #callPrepareStatementParameter} with a blob data type in {@link #testPrepareStatementParameter()}
   * @param blobColumn the parameter to verify
   * @throws SQLException exception
   */
  protected void verifyBlobColumnCallPrepareStatementParameter(SqlParameter blobColumn) throws SQLException {
    verify(callPrepareStatementParameter(blobColumn, null)).setBlob(Mockito.eq(blobColumn), Mockito.argThat(new ByteArrayMatcher(new byte[] {})));
    verify(callPrepareStatementParameter(blobColumn, "QUJD")).setBlob(Mockito.eq(blobColumn), Mockito.argThat(new ByteArrayMatcher(new byte[] {65 , 66 , 67})));
  }


  /**
   * @return Expected SQL for {@link #testUpdateWithLiteralValues()}
   */
  protected String expectedUpdateWithLiteralValues() {
    String value = varCharCast("'Value'");
    return String.format(
        "UPDATE %s SET stringField = %s%s, blobFieldOne = %s, blobFieldTwo = %s " +
        "WHERE ((field1 = %s) AND (field2 = %s) AND (field3 = %s) AND (field4 = %s) AND (field5 = %s) AND (field6 = %s) AND (field7 = %s%s) AND (field8 = %s%s) " +
        "AND (field9 = %s) AND (field10 = %s))",
        tableName(TEST_TABLE),
        stringLiteralPrefix(),
        value,
        expectedBlobLiteral(NEW_BLOB_VALUE_HEX),
        expectedBlobLiteral(NEW_BLOB_VALUE_HEX),
        expectedBooleanLiteral(true),
        expectedBooleanLiteral(false),
        expectedBooleanLiteral(true),
        expectedBooleanLiteral(false),
        expectedDateLiteral(),
        expectedDateLiteral(),
        stringLiteralPrefix(),
        value,
        stringLiteralPrefix(),
        value,
        expectedBlobLiteral(OLD_BLOB_VALUE_HEX),
        expectedBlobLiteral(OLD_BLOB_VALUE_HEX)
      );
  }


  /**
   * Tests the generation of a select statement with multiple union statements.
   */
  @Test
  public void testSelectWithUnionStatements() {
    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
    .from(new TableReference(OTHER_TABLE))
    .union(new SelectStatement(new FieldReference(STRING_FIELD)).from(new TableReference(TEST_TABLE)))
    .unionAll(new SelectStatement(new FieldReference(STRING_FIELD)).from(new TableReference(ALTERNATE_TABLE)))
    .orderBy(new FieldReference(STRING_FIELD));

    String result = testDialect.convertStatementToSQL(stmt);
    assertEquals("Select script should match expected", expectedSelectWithUnion(), result);
  }


  /**
   * Tests the generation of SQL string for a query with EXCEPT operator.
   */
  @Test
  public void testSelectWithExceptStatement() {
    assumeTrue("for dialects with no EXCEPT operation support the test will be skipped.", expectedSelectWithExcept() != null);

    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
      .from(new TableReference(TEST_TABLE))
      .except(new SelectStatement(new FieldReference(STRING_FIELD)).from(new TableReference(OTHER_TABLE)))
      .orderBy(new FieldReference(STRING_FIELD));
    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals("Select script should match expected", expectedSelectWithExcept(), result);
  }


  /**
   * Tests the generation of SQL string for a query with a DB-link.
   */
  @Test
  public void testSelectWithDbLink() {
    assumeTrue("for dialects with no EXCEPT operation support the test will be skipped.", expectedSelectWithDbLink() != null);

    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
        .from(new TableReference(null, TEST_TABLE, DBLINK_NAME));
    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals("Select script should match expected", expectedSelectWithDbLink(), result);
  }


  /**
   * Tests the generation of SQL string for a query with EXCEPT operator where a
   * former select uses DB-link.
   */
  @Test
  public void testSelectWithExceptStatementsWithDbLinkFormer() {
    assumeTrue("for dialects with no EXCEPT operation support the test will be skipped.", expectedSelectWithExceptAndDbLinkFormer() != null);

    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
        .from(new TableReference(null, TEST_TABLE, DBLINK_NAME))
        .except(new SelectStatement(new FieldReference(STRING_FIELD)).from(new TableReference(null, OTHER_TABLE)))
        .orderBy(new FieldReference(STRING_FIELD));
    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals("Select script should match expected", expectedSelectWithExceptAndDbLinkFormer(), result);
  }


  /**
   * Tests the generation of SQL string for a query with EXCEPT operator where a
   * latter select uses DB-link.
   */
  @Test
  public void testSelectWithExceptStatementsWithDbLinkLatter() {
    assumeTrue("for dialects with no EXCEPT operation support the test will be skipped.", expectedSelectWithExceptAndDbLinkLatter() != null);

    SelectStatement stmt = new SelectStatement(new FieldReference(STRING_FIELD))
        .from(new TableReference(null, TEST_TABLE))
        .except(new SelectStatement(new FieldReference(STRING_FIELD)).from(new TableReference(null, OTHER_TABLE, DBLINK_NAME)))
        .orderBy(new FieldReference(STRING_FIELD));
    String result = testDialect.convertStatementToSQL(stmt);

    assertEquals("Select script should match expected", expectedSelectWithExceptAndDbLinkLatter(), result);
  }


  /**
   * Tests a join with no ON criteria.
   */
  @Test
  public void testJoinNoCriteria() {
    SelectStatement testStatement = select().from(tableRef("TableOne")).crossJoin(tableRef("TableTwo"));
    assertEquals(testDialect.convertStatementToSQL(testStatement), expectedJoinOnEverything());
  }


  /**
   * Tests a join on to a sub-select.
   */
  @Test
  public void testJoinSubSelect() {
    final TableReference tableOne = tableRef("TableOne");
    final TableReference tableTwo = tableRef("Two");
    SelectStatement testStatement = select().from(tableOne).innerJoin(select().from("TableTwo").alias("Two"), tableOne.field("id").eq(tableTwo.field("id")));

    assertEquals(testDialect.convertStatementToSQL(testStatement), "SELECT * FROM " + tableName("TableOne") + " INNER JOIN (SELECT * FROM " + tableName("TableTwo") + ") Two ON (TableOne.id = Two.id)");
  }


  /**
   * Tests a count statement with an argument
   */
  @Test
  public void testCountArgument() {
    final TableReference tableOne = tableRef("TableOne");
    SelectStatement testStatement = select(count(field("name")), countDistinct(field("name"))).from(tableOne);

    assertEquals(testDialect.convertStatementToSQL(testStatement), "SELECT COUNT(name), COUNT(DISTINCT name) FROM " + tableName("TableOne"));
  }


  /**
   * Tests an average statement
   */
  @Test
  public void testAverage() {
    final TableReference tableOne = tableRef("TableOne");
    SelectStatement testStatement = select(average(field("name")), averageDistinct(field("name"))).from(tableOne);

    assertEquals("SELECT AVG(name), AVG(DISTINCT name) FROM " + tableName("TableOne"), testDialect.convertStatementToSQL(testStatement));
  }


  /**
   * Tests an INSERT INTO (...) VALUES (...) statement with a complex field.
   */
  @Test
  public void testInsertIntoValuesWithComplexField() {
    Schema schema = schema(table("TableOne").columns(column("id", DataType.INTEGER), column("value", DataType.INTEGER)));
    InsertStatement testStatement = insert().into(tableRef("TableOne")).values(literal(3).as("id"), literal(1).plus(literal(2)).as("value"));

    assertEquals(expectedSqlInsertIntoValuesWithComplexField(), testDialect.convertStatementToSQL(testStatement, schema, null));
  }


  /**
   * @return The expected SQL for Insert Into Values With Complex Field
   */
  protected List<String> expectedSqlInsertIntoValuesWithComplexField() {
    return Arrays.asList("INSERT INTO " + tableName("TableOne") + " (id, value) VALUES (3, 1 + 2)");
  }


  /**
   * Tests an INSERT with a date literal.
   */
  @Test
  public void testInsertDateLiteral() {
    Schema schema = schema(table("TableOne").columns(column("id", DataType.INTEGER), column("value", DataType.DATE)));
    InsertStatement testStatement = insert().into(tableRef("TableOne")).values(literal(3).as("id"), literal(new LocalDate(2010,1,2)).as("value"));

    assertEquals(Arrays.asList("INSERT INTO " + tableName("TableOne") + " (id, value) VALUES (3, " + expectedDateLiteral() + ")"), testDialect.convertStatementToSQL(testStatement, schema, null));
  }


  /**
   * Tests the logic used for transferring a boolean {@link Record} value to a
   * {@link PreparedStatement}.  For overriding in specific DB tests
   *
   * @throws SQLException when a database access error occurs
   */
  protected void verifyBooleanPrepareStatementParameter() throws SQLException {
    final SqlParameter booleanColumn = parameter("booleanColumn").type(DataType.BOOLEAN);
    verify(callPrepareStatementParameter(booleanColumn, null)).setObject(booleanColumn, null);
    verify(callPrepareStatementParameter(booleanColumn, "true")).setBoolean(booleanColumn, true);
    verify(callPrepareStatementParameter(booleanColumn, "false")).setBoolean(booleanColumn, false);
  }



  /**
   * Tests the logic used for transferring a {@link Record} value to a
   * {@link PreparedStatement}.
   *
   * @throws SQLException when a database access error occurs
   */
  @Test
  public void testPrepareStatementParameter() throws SQLException {

    final SqlParameter dateColumn = parameter(column("dateColumn", DataType.DATE));
    final SqlParameter decimalColumn = parameter(column("decimalColumn", DataType.DECIMAL, 9, 5));
    final SqlParameter stringColumn = parameter(column("stringColumn", DataType.STRING, 4));
    final SqlParameter integerColumn = parameter(column("integerColumn", DataType.INTEGER));
    final SqlParameter bigIntegerColumn = parameter(column("bigIntegerColumn", DataType.BIG_INTEGER));
    final SqlParameter blobColumn = parameter(column("blobColumn", DataType.BLOB));
    final SqlParameter clobColumn = parameter(column("clobColumn", DataType.CLOB));

    // Boolean
    verifyBooleanPrepareStatementParameter();

    // Date
    verify(callPrepareStatementParameter(dateColumn, null)).setObject(dateColumn, null);
    verify(callPrepareStatementParameter(dateColumn, "2012-12-01")).setDate(dateColumn, java.sql.Date.valueOf("2012-12-01"));

    // Decimal
    verify(callPrepareStatementParameter(decimalColumn, null)).setBigDecimal(decimalColumn, null);

    NamedParameterPreparedStatement mockStatement = callPrepareStatementParameter(decimalColumn, "3");
    ArgumentCaptor<BigDecimal> bigDecimalCapture = ArgumentCaptor.forClass(BigDecimal.class);
    verify(mockStatement).setBigDecimal(eq(decimalColumn), bigDecimalCapture.capture());
    assertTrue("BigDecimal not correctly set on statement.  Expected 3, was: " + bigDecimalCapture.getValue(), bigDecimalCapture.getValue().compareTo(new BigDecimal(3)) == 0);

    // String
    verify(callPrepareStatementParameter(stringColumn, null)).setString(stringColumn, null);
    verify(callPrepareStatementParameter(stringColumn, "")).setString(stringColumn, null);
    verify(callPrepareStatementParameter(stringColumn, "test")).setString(stringColumn, "test");

    // Integer
    verify(callPrepareStatementParameter(integerColumn, null)).setObject(integerColumn, null);

    mockStatement = callPrepareStatementParameter(integerColumn, "23");
    ArgumentCaptor<Integer>intCapture = ArgumentCaptor.forClass(Integer.class);
    verify(mockStatement).setInt(eq(integerColumn), intCapture.capture());
    assertEquals("Integer not correctly set on statement", 23, intCapture.getValue().intValue());

    // Big Integer
    verify(callPrepareStatementParameter(bigIntegerColumn, null)).setObject(bigIntegerColumn, null);

    mockStatement = callPrepareStatementParameter(bigIntegerColumn, "345345423234234234");
    ArgumentCaptor<Long> bigIntCapture = ArgumentCaptor.forClass(Long.class);
    verify(mockStatement).setLong(eq(bigIntegerColumn), bigIntCapture.capture());
    assertEquals("Big integer not correctly set on statement", 345345423234234234L, bigIntCapture.getValue().longValue());

    // Blob
    verifyBlobColumnCallPrepareStatementParameter(blobColumn);

    // Clob
    verify(callPrepareStatementParameter(clobColumn, null)).setString(clobColumn, null);
    verify(callPrepareStatementParameter(clobColumn, "")).setString(clobColumn, null);
    verify(callPrepareStatementParameter(clobColumn, "test")).setString(clobColumn, "test");

  }


  @Test
  public void testPortableFunction() {
    AliasedField function = PortableSqlFunction.builder()
            .withFunctionForDatabaseType(
                    "PGSQL",
                    "TRANSLATE",
                    new FieldReference("field"),
                    new FieldLiteral("1"),
                    new FieldLiteral("A"))
            .withFunctionForDatabaseType(
                    "H2",
                    "BTRIM",
                    new FieldReference("field"),
                    new FieldLiteral("2"),
                    new FieldLiteral("B"))
            .withFunctionForDatabaseType(
                    "ORACLE",
                    "REGEX_REPLACE",
                    new FieldReference("field"),
                    new FieldLiteral("3"),
                    new FieldLiteral("C"))
            .withFunctionForDatabaseType(
                    "MY_SQL",
                    "REVERSE",
                    new FieldReference("field"),
                    new FieldLiteral("4"),
                    new FieldLiteral("D"))
            .withFunctionForDatabaseType(
                    "SQL_SERVER",
                    "SOUNDEX",
                    new FieldReference("field"),
                    new FieldLiteral("5"),
                    new FieldLiteral("E"))
            .as("field")
            .build();


    UpdateStatement testStatement = UpdateStatement.update(new TableReference("Table")).set(function).build();

    assertEquals(expectedPortableStatement(), testDialect.convertStatementToSQL(testStatement));
  }


  /**
   * Tests SQL date conversion to string via databaseSafeStringtoRecordValue
   *
   * @throws SQLException If a SQL exception is thrown.
   */
  @Test
  public void testSqlDateConversion() throws SQLException {
    ResultSet rs = mock(ResultSet.class);

    LocalDate localDate1 = new LocalDate(2010, 1, 1);
    LocalDate localDate2 = new LocalDate(2010, 12, 21);
    LocalDate localDate3 = new LocalDate(100, 1, 1);
    LocalDate localDate4 = new LocalDate(9999, 12, 31);

    java.sql.Date date1 = new java.sql.Date(localDate1.toDate().getTime());
    java.sql.Date date2 = new java.sql.Date(localDate2.toDate().getTime());
    java.sql.Date date3 = new java.sql.Date(localDate3.toDate().getTime());
    java.sql.Date date4 = new java.sql.Date(localDate4.toDate().getTime());

    when(rs.getDate(1)).thenReturn(date1);
    when(rs.getDate(2)).thenReturn(date2);
    when(rs.getDate(3)).thenReturn(date3);
    when(rs.getDate(4)).thenReturn(date4);

    Record record = testDialect.resultSetToRecord(rs, ImmutableList.of(
      column("Date1", DataType.DATE),
      column("Date2", DataType.DATE),
      column("Date3", DataType.DATE),
      column("Date4", DataType.DATE)
    ));

    assertEquals(localDate1, record.getLocalDate("Date1"));
    assertEquals(localDate2, record.getLocalDate("Date2"));
    assertEquals(localDate3, record.getLocalDate("Date3"));
    assertEquals(localDate4, record.getLocalDate("Date4"));

    assertEquals(date1, record.getDate("Date1"));
    assertEquals(date2, record.getDate("Date2"));
    assertEquals(date3, record.getDate("Date3"));
    assertEquals(date4, record.getDate("Date4"));
  }


  /**
   * Tests that forced serial import option is used correctly.
   */
  @Test
  public void testUseForcedInsert() {
    assertEquals("Forced serial import should be set correctly", expectedForceSerialImport(), testDialect.useForcedSerialImport());
  }


  /**
   * Calls callPrepareStatementParameter with a mock {@link PreparedStatement} and returns
   * the mock for analysis.
   *
   * @param parameter The SQL parameter
   * @param value The value to set
   * @return The mocked {@link PreparedStatement}
   */
  protected NamedParameterPreparedStatement callPrepareStatementParameter(SqlParameter parameter, String value) {
    NamedParameterPreparedStatement mockStatement = mock(NamedParameterPreparedStatement.class);
    testDialect.prepareStatementParameters(mockStatement, ImmutableList.of(parameter), statementParameters().setString(parameter.getImpliedName(), value));
    return mockStatement;
  }


  /**
   * Compares expected and actual SQL statements and makes JUnit assertions that they are the same.
   *
   * @param expectedStatements Expected list of SQL statements.
   * @param actualStatements Iterables containing actual SQL statements generated.
   */
  @SuppressWarnings("unchecked")
  protected void compareStatements(List<String> expectedStatements, Iterable<String>... actualStatements) {
    List<String> actualStatementList = new ArrayList<>();
    for (Iterable<String> source : actualStatements) {
      for (String string : source) {
        actualStatementList.add(string);
      }
    }

    assertEquals(
      Joiner.on("\n").join(expectedStatements),
      Joiner.on("\n").join(actualStatementList)
    );
  }


  /**
   * Helper method to compare one statement to another.
   *
   * @param expected The expected statement.
   * @param actual Actual statement.
   */
  @SuppressWarnings("unchecked")
  protected void compareStatements(String expected, Iterable<String> actual) {
    compareStatements(Arrays.asList(expected), actual);
  }

  /**
   * @return The dialect to be tested.
   */
  protected abstract SqlDialect createTestDialect();


  /**
   * Many tests have common results apart from a table name decoration. This method allows for
   * those tests to be commonised and save a lot of duplication between descendent classes.
   *
   * <p>If no decoration is required for an SQL dialect descendant classes need not implement this method.</p>
   *
   * @param baseName Base table name.
   * @return Decorated name.
   */
  protected String tableName(String baseName) {
    return baseName;
  }


  /**
   * For tests using tables from different schema values.
   *
   * @param baseName Base table name.
   * @return Decorated name.
   */
  protected String differentSchemaTableName(String baseName) {
   return "MYSCHEMA." + baseName;
  }


  /**
   * A database platform may need to specify the null order.
   *
   * <p>If a null order is not required for a SQL dialect descendant classes need to implement this method.</p>
   *
   * @return the null order for an SQL dialect
   */
  protected String nullOrder() {
    return StringUtils.EMPTY;
  }

  /**
   * A database platform may need to specify the null order by direction.
   *
   * <p>If a null order is not required for a SQL dialect descendant classes need to implement this method.</p>
   *
   * @param descending the order direction
   * @return the null order for an SQL dialect
   */
  protected String nullOrderForDirection(@SuppressWarnings("unused") Direction descending) {
    return nullOrder();
  }


  /**
   * @return The expected SQL statements for creating the test database tables.
   */
  protected abstract List<String> expectedCreateTableStatements();


  /**
   * @return The expected SQL statements for creating the test database view.
   */
  protected List<String> expectedCreateViewStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah'))");
  }


  /**
   * @return The expected SQL statements for creating the test database sequence.
   */
  protected abstract List<String> expectedCreateSequenceStatements();


  /**
   * @return The expected SQL statements for creating the test database sequence.
   */
  protected abstract List<String> expectedCreateTemporarySequenceStatements();


  /**
   * @return The expected SQL statements for creating the test database sequence.
   */
  protected abstract List<String> expectedCreateSequenceStatementsWithNoStartWith();


  /**
   * @return The expected SQL statements for creating the test database sequence.
   */
  protected abstract List<String> expectedCreateTemporarySequenceStatementsWithNoStartWith();


  /**
   * @return The expected SQL statements for creating the test database view over a union select.
   */
  protected List<String> expectedCreateViewOverUnionSelectStatements() {
    return Arrays.asList("CREATE VIEW " + tableName("TestView") + " AS (SELECT stringField FROM " + tableName(TEST_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah') UNION ALL SELECT stringField FROM " + tableName(OTHER_TABLE) + " WHERE (stringField = " + stringLiteralPrefix() + "'blah'))");
  }


  /**
   * @return The expected SQL statement when performing the ANSI daysBetween call
   */
  protected String expectedDaysBetween() {
    return "SELECT (dateTwo - dateOne) DAY FROM MyTable";
  }


  /**
   * @return The expected SQL statement when performing the ANSI COALESCE call
   */
  protected String expectedCoalesce() {
    return "SELECT COALESCE(NULL, bob) FROM " + tableName("MyTable");
  }


  /**
   * @return The expected SQL statement when performing the ANSI GREATEST call
   */
  protected String expectedGreatest() {
    return "SELECT GREATEST(NULL, bob) FROM " + tableName("MyTable");
  }


  /**
   * @return The expected SQL statement when performing the ANSI LEAST call
   */
  protected String expectedLeast() {
    return "SELECT LEAST(NULL, bob) FROM " + tableName("MyTable");
  }


  /**
   * @return The expected SQL statements for creating the test temporary database tables.
   */
  protected abstract List<String> expectedCreateTemporaryTableStatements();


  /**
   * @return The expected SQL statements for creating test database tables with long names.
   */
  protected abstract List<String> expectedCreateTableStatementsWithLongTableName();


  /**
   * @return The expected SQL statements for dropping the test database tables.
   */
  protected abstract List<String> expectedDropTableStatements();


  /**
   * @return The expected SQL statements for dropping a single test database table.
   */
  protected abstract List<String> expectedDropSingleTable();


  /**
   * @return The expected SQL statements for dropping the test database tables.
   */
  protected abstract List<String> expectedDropTables();


  /**
   * @return The expected SQL statements for dropping the test database tables with parameters.
   */
  protected abstract List<String> expectedDropTablesWithParameters();


  /**
   * @return The expected SQL statements for dropping the test database view.
   */
  protected abstract List<String> expectedDropViewStatements();


  /**
   * @return The expected SQL statements for dropping the test database view.
   */
  protected List<String> expectedDropSequenceStatements() {
    return Arrays.asList("DROP SEQUENCE IF EXISTS " + tableName(SEQUENCE_NAME));
  }


  /**
   * @return The expected SQL statements for dropping the temporary test
   *         database tables.
   */
  protected abstract List<String> expectedDropTempTableStatements();


  /**
   * @return The expected SQL statements for clearing the test database tables.
   */
  protected abstract List<String> expectedTruncateTableStatements();


  /**
   * @return The expected SQL statements for clearing the temporary test database tables.
   */
  protected abstract List<String> expectedTruncateTempTableStatements();


  /**
   * @return The expected SQL statement for clearing the test database tables with a delete.
   */
  protected abstract List<String> expectedDeleteAllFromTableStatements();


  /**
   * @return The expected SQL for a parameterised insert to the Test table.
   */
  protected abstract String expectedParameterisedInsertStatement();


  /**
   * @return The expected SQL for a parameterised insert to the Test table in a different schema.
   */
  protected abstract String expectedParameterisedInsertStatementWithTableInDifferentSchema();


  /**
   * @return The expected SQL for a parameterised insert to the Test table with no column values specified.
   */
  protected abstract String expectedParameterisedInsertStatementWithNoColumnValues();


  /**
   * @return The expected SQL for a specified value insert to the Test table.
   */
  protected abstract List<String> expectedSpecifiedValueInsert();

  /**
   * @return The expected SQL for a specified value insert to the Test table in a different schema.
   */
  protected abstract List<String> expectedSpecifiedValueInsertWithTableInDifferentSchema();


  /**
   * @return The expected SQL for an insert to Test that requires an auto generated id.
   */
  protected abstract List<String> expectedAutoGenerateIdStatement();


  /**
   * @return The expected SQL for an insert to Test that requires an auto
   *         generated id and version.
   */
  protected abstract List<String> expectedInsertWithIdAndVersion();


  /**
   * Verify on the expected SQL statements to be run after insert for the test database table.
   * @param sqlScriptExecutor The script executor to use
   * @param connection The connection to use
   */
  @SuppressWarnings("unused")
  protected void verifyPostInsertStatementsInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    verifyNoMoreInteractions(sqlScriptExecutor);
  }


  /**
   * @return The expected SQL statements to be run prior to insert for the test database table.
   */
  protected List<String> expectedPreInsertStatementsInsertingUnderAutonumLimit() {
    return Collections.emptyList();
  }


  /**
   * Verify on the expected SQL statements to be run after insert for the test database table.
   * @param sqlScriptExecutor The script executor to use
   * @param connection The connection to use
   */
  @SuppressWarnings("unused")
  protected void verifyPostInsertStatementsNotInsertingUnderAutonumLimit(SqlScriptExecutor sqlScriptExecutor,Connection connection) {
    verifyNoMoreInteractions(sqlScriptExecutor);
  }


  /**
   * @return The expected SQL statements to be run prior to insert for the test database table.
   */
  protected List<String> expectedPreInsertStatementsNotInsertingUnderAutonumLimit() {
    return Collections.emptyList();
  }


  protected abstract List<String> expectedAddTableFromStatements();


  protected abstract List<String> expectedReplaceTableFromStatements();


  protected abstract List<String> expectedReplaceTableWithAutonumber();


  /**
   * @return The expected SQL for an insert to Test that inserts an empty string (i.e. NULL).
   */
  protected abstract String expectedEmptyStringInsertStatement();


  /**
   * @return The expected SQL for a simple concatenation operation.
   */
  protected abstract String expectedSelectWithConcatenation1();


  /**
   * @return The expected SQL for another simple concatenation operation.
   */
  protected abstract String expectedSelectWithConcatenation2();


  /**
   * @return The expected SQL for a concatenation operation which uses a function.
   */
  protected abstract String expectedConcatenationWithFunction();


  /**
   * @return The expected SQL for a concatenation which uses a case statement.
   */
  protected abstract String expectedConcatenationWithCase();


  /**
   * @return The expected SQL for a nested concatenation.
   */
  protected abstract String expectedNestedConcatenations();


  /**
   * @return The expected SQL for a concatenation made up of literals only.
   */
  protected abstract String expectedConcatenationWithMultipleFieldLiterals();


  /**
   * @return The expected SQL for a concatenation made up of literals only.
   */
  protected abstract String expectedIsNull();


  /**
   * @return The expected SQL for conversion of YYYYMMDD into a date
   */
  protected abstract String expectedYYYYMMDDToDate();


  /**
   * @return The expected SQL for conversion of a date to a YYYYMMDD integer
   */
  protected abstract String expectedDateToYyyymmdd();


  /**
   * @return The expected SQL for conversion of a date to a YYYYMMDDHHmmss integer
   */
  protected abstract String expectedDateToYyyymmddHHmmss();


  /**
   * @return The expected SQL for conversion of a ClobFieldLiteral to string.
   */
  protected abstract String expectedClobLiteralCast();


  /**
   * @return The expected SQL for now function returning UTC timestamp.
   */
  protected abstract String expectedNow();


  /**
   * @return The expected SQL for adding days
   */
  protected abstract String expectedAddDays();


  /**
   * @return The expected SQL for adding days
   */
  protected abstract String expectedAddMonths();


  /**
   * @return The expected SQL for maths addition.
   */
  protected abstract String expectedMathsPlus();


  /**
   * @return The expected SQL for maths subtraction.
   */
  protected abstract String expectedMathsMinus();


  /**
   * @return The expected SQL for maths division.
   */
  protected abstract String expectedMathsDivide();


  /**
   * @return The expected SQL for maths multiplication.
   */
  protected abstract String expectedMathsMultiply();


  /**
   * @return The expected SQL for a cast to a string.
   */
  protected abstract String expectedStringCast();


  /**
   * @return The expected SQL for a cast of a function to a string.
   */
  protected abstract String expectedStringFunctionCast();


  /**
   * @return The expected SQL for a cast to a big int.
   */
  protected abstract String expectedBigIntCast();

  /**
   * @return The expected SQL for a cast of a function to a big int.
   */
  protected abstract String expectedBigIntFunctionCast();


  /**
   * @return The expected SQL for a cast to a boolean.
   */
  protected abstract String expectedBooleanCast();


  /**
   * @return The expected SQL for a cast to a date.
   */
  protected abstract String expectedDateCast();


  /**
   * @return The expected SQL for a cast to a decimal.
   */
  protected abstract String expectedDecimalCast();


  /**
   * @return The expected SQL for a cast to an integer.
   */
  protected abstract String expectedIntegerCast();


  /**
   * @return The expected SQL for a cast from a string literal to an integer.
   */
  protected abstract String expectedStringLiteralToIntegerCast();


  /**
   * @return The expected SQL for selecting with an UNION statement.
   */
  protected abstract String expectedSelectWithUnion();


  /**
   * @return The expected SQL for selecting with an EXCEPT statement, or
   *         {@code null} if EXCEPT operation is unsupported.
   */
  protected abstract String expectedSelectWithExcept();


  /**
   * @return The expected SQL for selecting with a DB Link or {@code null} if DB
   *         Link is unsupported.
   */
  protected abstract String expectedSelectWithDbLink();


  /**
   * @return The expected SQL for selecting with an EXCEPT statement and DB Link
   *         (for the former statement), or {@code null} if DB-Link or EXCEPT
   *         operation is unsupported.
   */
  protected abstract String expectedSelectWithExceptAndDbLinkFormer();


  /**
   * @return The expected SQL for selecting with an EXCEPT statement and DB Link
   *         (for the latter statement), or {@code null} if DB-Link or EXCEPT
   *         operation is unsupported.
   */
  protected abstract String expectedSelectWithExceptAndDbLinkLatter();


  /**
   * @return The expected SQL for selecting with a substring statement.
   */
  protected abstract String expectedSubstring();


  /**
   * @return The expected SQL for requesting the next value of a sequence.
   */
  protected abstract String expectedNextValForSequence();


  /**
   * @return The expected SQL for requesting the current value of a sequence
   */
  protected abstract String expectedCurrValForSequence();


  /**
   * @return The expected SQL for updating the autonumber table.
   */
  protected abstract List<String> expectedAutonumberUpdate();

  /**
   * @return The expected SQL for updating the autonumber table for a column not called 'id'.
   */
  protected abstract List<String> expectedAutonumberUpdateForNonIdColumn();


  /**
   * @return The expected SQL for performing an update with a minimum aggregate
   */
  protected abstract String expectedUpdateWithSelectMinimum();


  /**
   * @return The expected SQL for performing an update with a destination table
   *         which is aliased.
   */
  protected abstract String expectedUpdateUsingAliasedDestinationTable();


  /**
   * @return The expected SQL for performing an update with a destination table which lives in a different schema.
   */
  protected String expectedUpdateUsingTargetTableInDifferentSchema() {
    return "UPDATE " + differentSchemaTableName("FloatingRateRate") + " A SET settlementFrequency = (SELECT settlementFrequency FROM " + tableName("FloatingRateDetail") + " B WHERE (A.floatingRateDetailId = B.id))";
  }


  /**
   * @return The expected SQL for performing an update with a source table which lives in a different schema.
   */
  protected String expectedUpdateUsingSourceTableInDifferentSchema() {
    return "UPDATE " + tableName("FloatingRateRate") + " A SET settlementFrequency = (SELECT settlementFrequency FROM MYSCHEMA.FloatingRateDetail B WHERE (A.floatingRateDetailId = B.id))";
  }


  /**
   * @return the expected SQL for performing a simple Merge
   */
  protected abstract String expectedMergeSimple();


  /**
   * @return the expected SQL for performing a more complex Merge
   */
  protected abstract String expectedMergeComplex();


  /**
   * @return the expected SQL for performing a merge where the source table lives in a different schema
   */
  protected abstract String expectedMergeSourceInDifferentSchema();


  /**
   * @return the expected SQL for performing a merge where the target table lives in a different schema
   */
  protected abstract String expectedMergeTargetInDifferentSchema();


  /**
   * @return the expected SQL for performing a merge where all fields are in the primary key
   */
  protected abstract String expectedMergeForAllPrimaryKeys();


  /**
   * @return the expected SQL for performing a merge with explicit update expressions
   */
  protected abstract String expectedMergeWithUpdateExpressions();


  /**
   * @return the expected SQL for generating a pseudo-random string
   */
  protected abstract String expectedRandomString();


  /**
   * @return the expected SQL for generating a select statement of literal fields with a where clause
   */
  protected abstract String expectedSelectLiteralWithWhereClauseString();


  /**
   * @return The expected SQL for a join with no ON criteria
   */
  protected String expectedJoinOnEverything() {
    return "SELECT * FROM " + tableName("TableOne") + " INNER JOIN " + tableName("TableTwo") + " ON 1=1";
  }


  /**
   * @return The expected SQL for a Trim
   */
  protected String expectedTrim() {
    return "SELECT TRIM(field1) FROM " + tableName("schedule");
  }


  /**
   * @return The expected SQL for a Left Trim
   */
  protected String expectedLeftTrim() {
    return "SELECT LTRIM(field1) FROM " + tableName("schedule");
  }


  /**
   * @return The expected SQL for a Left Trim
   */
  protected String expectedRightTrim() {
    return "SELECT RTRIM(field1) FROM " + tableName("schedule");
  }


  /**
   * @return the expected SQL for Left pad
   */
  protected String expectedLeftPad() {
    return "SELECT LPAD(stringField, 10, 'j') FROM " + tableName(TEST_TABLE);
  }


  /**
   * @return the expected SQL for Left pad
   */
  protected String expectedRightPad() {
    return "SELECT RPAD(stringField, 10, 'j') FROM " + tableName(TEST_TABLE);
  }

  /**
   * @return The expected SQL for the MOD operator.
   */
  protected String expectedSelectModSQL() {
    return "SELECT MOD(intField, 5) FROM " + tableName(TEST_TABLE);
  }


  /**
   * @return The expected SQL for retrieving the row number
   */
  protected String expectedRowNumber() {
    return "ROW_NUMBER()";
  }


  /**
   * @return The expected SQL for rounding
   */
  protected String expectedRound() {
    return "SELECT ROUND(field1, 2) FROM " + tableName("schedule");
  }


  /**
   * @return The expected date literal.
   */
  protected String expectedDateLiteral() {
    return "DATE '2010-01-02'";
  }


  /**
   * @param value the boolean value to translate.
   * @return The expected boolean literal.
   */
  protected String expectedBooleanLiteral(boolean value) {
    return value ? "1" : "0";
  }


  /**
   * @param value the blob value to translate.
   * @return The expected blob literal.
   */
  protected String expectedBlobLiteral(String value) {
    return String.format("'%s'", value);
  }


  /**
   * @return The expected random function sql.
   */
  protected String expectedRandomFunction() {
    return "RAND()";
  }


  /**
   * @return The expected sql.
   */
  protected String expectedSelectSome() {
    return "SELECT MAX(booleanField) FROM " + tableName(TEST_TABLE);
  }


  /**
   * @return The expected sql.
   */
  protected String expectedSelectEvery() {
    return "SELECT MIN(booleanField) FROM " + tableName(TEST_TABLE);
  }


  /**
   * @return The expected SQL for the LOWER function.
   */
  protected String expectedLower() {
    return "SELECT LOWER(field1) FROM " + tableName("schedule");
  }


  /**
   * @return The expected SQL for the UPPER function.
   */
  protected String expectedUpper() {
    return "SELECT UPPER(field1) FROM " + tableName("schedule");
  }


  /**
   * @return The expected SQL for the FLOOR function
   */
  protected String expectedFloor() {
    return "SELECT FLOOR(floatField) FROM " + tableName(TEST_TABLE);
  }


  /**
   * @return The expected SQL for the POWER function
   */
  private Object expectedPower() {
    return "SELECT POWER(floatField, intField) FROM " + tableName(TEST_TABLE);
  }


  /**
   * @param value for the where criterion
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  protected abstract String expectedDeleteWithLimitAndWhere(String value);


  /**
   * @param value for the where criterion
   * @param value2 for the where criterion
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  protected abstract String expectedDeleteWithLimitAndComplexWhere(String value, String value2);


  /**
   * @return The expected SQL for a delete statement with a limit and where criterion.
   */
  protected abstract String expectedDeleteWithLimitWithoutWhere();


  /**
   * @param rowCount The number of rows for which to optimise the query plan.
   * @return The expected SQL for the {@link SelectStatement#optimiseForRowCount(int)} directive.
   */
  protected abstract String expectedHints1(int rowCount);

  /**
   * @param rowCount The number of rows for which to optimise the query plan.
   * @return The expected SQL for the {@link SelectStatement#optimiseForRowCount(int)} directive.
   */
  protected String expectedHints2(@SuppressWarnings("unused") int rowCount) {
    return "SELECT a, b FROM " + tableName("Foo") + " ORDER BY a FOR UPDATE";
  }


  /**
   * @return The expected SQL for the {@link UpdateStatement#useParallelDml()} directive.
   */
  protected String expectedHints3() {
    return "UPDATE " + tableName("Foo") + " SET a = b";
  }


  /**
   * @return The expected SQL for the {@link UpdateStatement#useParallelDml(int)} directive.
   */
  protected String expectedHints3a() {
    return "UPDATE " + tableName("Foo") + " SET a = b";
  }


  /**
   * @return The expected SQL for the {@link InsertStatement#useDirectPath()} directive.
   */
  protected String expectedHints4() {
    return  "INSERT INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @return The expected SQL for the {@link InsertStatement#avoidDirectPath()} directive.
   */
  protected String expectedHints4a() {
    return  "INSERT INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @return The expected SQL for the {@link InsertStatement#useParallelDml()} ()} directive.
   */
  protected String expectedHints4b() {
    return  "INSERT INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @return The expected SQL for the {@link InsertStatement#useParallelDml(int)} directive.
   */
  protected String expectedHints4c() {
    return  "INSERT INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @return The expected SQL when no hint directive is used on the {@link InsertStatement}.
   */
  private String expectedHints5() {
    return  "INSERT INTO " + tableName("Foo") + " SELECT a, b FROM " + tableName("Foo_1");
  }


  /**
   * @return The expected SQL for the {@link SelectStatement#withParallelQueryPlan(int)} directive.
   */
  protected String expectedHints6() {
    return "SELECT a, b FROM " + tableName("Foo") + " ORDER BY a";
  }


  /**
   * @return The expected SQL for the {@link SelectStatement#withParallelQueryPlan(int)} and {@link SelectStatement#allowParallelDml()} directive.
   */
  protected String expectedHints6a() {
    return "SELECT a, b FROM " + tableName("Foo") + " ORDER BY a";
  }


  /**
   * @return The expected SQL for the {@link SelectStatement#withCustomHint(CustomHint customHint)} directive. Testing the OracleDialect adds the hints successfully.
   */
  protected  String expectedHints7() {
    return "SELECT * FROM SCHEMA2.Foo"; //NOSONAR
  }


  /**
   * @return The expected SQL for the {@link SelectStatement#withCustomHint(CustomHint customHint)} directive. Testing all dialcts do not react to an empty hint being supplied.
   */
  protected  String expectedHints8() {
    return "SELECT * FROM SCHEMA2.Foo"; //NOSONAR
  }

  /**
   * @return The expected SQL for the {@link SelectStatement#withDialectSpecificHint(String, String)} directive. Testing all dialcts do not react to an empty hint being supplied.
   */
  protected  String expectedHints8a() {
    return "SELECT * FROM SCHEMA2.Foo"; //NOSONAR
  }


  /**
   * @return The expected SQL for the {@link PortableSqlFunction} function, testing that the dialect-specific function is used.
   */
  protected abstract String expectedPortableStatement();

  /**
   * @return The expected value for the force serial import setting.
   */
  protected boolean expectedForceSerialImport() {
    return false;
  }

  /**
   * @return the testDialect
   */
  public SqlDialect getTestDialect() {
    return testDialect;
  }


  /**
   * Helper to allow lists of SQL strings to be compared in Eclipse.
   *
   * @param message The message to show on failure.
   * @param expected The expected list of strings.
   * @param actual The actual list of strings.
   */
  private void assertSQLEquals(String message, List<String> expected, List<String> actual) {
    Assert.assertEquals(message, StringUtils.join(expected, "\n"), StringUtils.join(actual, "\n"));
  }


  /**
   * Tests formatting of numerical values in a {@link Record}.
   *
   * @throws SQLException when a database access error occurs
   */
  @Test
  public void testDecimalFormatter() throws SQLException {
    assertEquals("Do nothing if no trailing zeroes", "123.123", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "123.123"));
    assertEquals("Remove trailing zeroes from genuine decimal", "123.123", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "123.12300"));
    assertEquals("Ignore zeroes that are not trailing", "0.00003", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "000.00003"));
    assertEquals("Remove trailing zeroes from zero value decimal", "0", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "0.0000"));
    assertNull("Nulls get passed through even for BigDecimals", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, null));
    assertEquals("Do nothing to zero value integer", "0", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "0"));
    assertEquals("Do nothing to zero ending integer", "200", checkDatabaseSafeStringToRecordValue(DataType.DECIMAL, "200"));
    assertEquals("Boolean: 0 --> false", "false", checkDatabaseSafeStringToRecordValue(DataType.BOOLEAN, "0"));
    assertEquals("Boolean: 1 --> true", "true", checkDatabaseSafeStringToRecordValue(DataType.BOOLEAN, "1"));
    assertEquals("Boolean: null --> null", null, checkDatabaseSafeStringToRecordValue(DataType.BOOLEAN, null));
  }


  /**
   * Tests formatting of binary values in record derived from a {@link ResultSet}.
   *
   * @throws SQLException when a database access error occurs
   */
  @Test
  public void testBinaryFormatter() throws SQLException {
    assertEquals("Value not transformed into Base64", "REVG", checkDatabaseByteArrayToRecordValue(new byte[] {68,69,70}));
    assertEquals("Value not transformed into Base64", "//79", checkDatabaseByteArrayToRecordValue(new byte[] { -1, -2, -3 }));
    assertNull("Null should result in null value", checkDatabaseByteArrayToRecordValue(null));
    assertEquals(
      "Value not transformed into Base64",
      BASE64_ENCODED,
      checkDatabaseByteArrayToRecordValue(BYTE_ARRAY));
  }


  /**
   * Tests the {@link SqlDialect#formatSqlStatement(String)} performs
   * correctly.
   */
  @Test
  public void testFormatSqlStatement() {
    expectedSqlStatementFormat();
  }


  @Test
  public void testComment() {
    String commentSQL = testDialect.convertCommentToSQL("Hello!");

    assertTrue(testDialect.sqlIsComment(commentSQL));
    assertFalse(testDialect.sqlIsComment("select a from b"));
    assertFalse("Multi-line SQL can have comments at the top", testDialect.sqlIsComment(commentSQL+ "\nSome real SQL!"));
  }


  /**
   * Provides the tests for the correct format expected.
   * This method can be overridden for dialect specific formatting.
   */
  protected void expectedSqlStatementFormat() {
    // When
    String statement1 = testDialect.formatSqlStatement("END;");
    String statement2 = testDialect.formatSqlStatement("test");

    // Then
    assertEquals("The SQL statement should be [END;;]" , "END;;", statement1);
    assertEquals("The SQL statement should be [test;]" , "test;", statement2);
  }


  @Test
  public void testUsesNVARCHARforStrings() {
    assertEquals(expectedUsesNVARCHARforStrings(), testDialect.usesNVARCHARforStrings());
  }


  @Test
  public void testWindowFunctions() {
    List<AliasedField> windowFunctions = windowFunctions().toList();
    List<String> expectedSql = expectedWindowFunctionStatements();
    assertEquals("Incorrect test setup, the expected number of window function statements did not match the window function test cases",windowFunctions.size(),expectedSql.size());

   for(int i = 0; i < windowFunctions.size(); i++) {
      assertEquals(expectedSql.get(i),testDialect.getSqlFrom(windowFunctions.get(i)));
    }
  }


  /**
   * @return The expected SQL statements resulting from converting the elements of windowFunctions()
   */
  protected List<String> expectedWindowFunctionStatements(){
    String paddedNullOrder = StringUtils.isEmpty(nullOrder())? StringUtils.EMPTY : " "+nullOrder();
    String paddedNullOrderDesc = StringUtils.isEmpty(nullOrder())? StringUtils.EMPTY : " "+nullOrderForDirection(Direction.DESCENDING);
    return Lists.newArrayList(
      "COUNT(*) OVER ()",
      "COUNT(*) OVER (PARTITION BY field1)",
      "SUM(field1) OVER (PARTITION BY field2, field3 ORDER BY field4"+paddedNullOrder+")",
      "MAX(field1) OVER (PARTITION BY field2, field3 ORDER BY field4"+paddedNullOrder+")",
      "MIN(field1) OVER (PARTITION BY field2, field3 ORDER BY field4 DESC"+paddedNullOrderDesc+", field5"+paddedNullOrder+")",
      "MIN(field1) OVER ( ORDER BY field2"+paddedNullOrder+")",
      "ROW_NUMBER() OVER (PARTITION BY field2, field3 ORDER BY field4"+paddedNullOrder+")",
      "ROW_NUMBER() OVER ( ORDER BY field2"+paddedNullOrder+")",
      "(SELECT MIN(field1) OVER ( ORDER BY field2"+paddedNullOrder+") AS window FROM "+tableName("srcTable")+")"
    );
  }


  /**
   * The window functions to test
   */
  private FluentIterable<AliasedField> windowFunctions(){
    return FluentIterable.from(Lists.newArrayList(
      windowFunction(count()).build(),
      windowFunction(count()).partitionBy(field("field1")).build(),
      windowFunction(sum(field("field1"))).partitionBy(field("field2"),field("field3")).orderBy(field("field4")).build(),
      windowFunction(max(field("field1"))).partitionBy(field("field2"),field("field3")).orderBy(field("field4").asc()).build(),
      windowFunction(min(field("field1"))).partitionBy(field("field2"),field("field3")).orderBy(field("field4").desc(),field("field5")).build(),
      windowFunction(min(field("field1"))).orderBy(field("field2")).build(),
      windowFunction(rowNumber()).partitionBy(field("field2"),field("field3")).orderBy(field("field4")).build(),
      windowFunction(rowNumber()).orderBy(field("field2")).build(),
      select( windowFunction(min(field("field1"))).orderBy(field("field2")).build().as("window")).from(tableRef("srcTable")).asField()
      ));
  }


  /**
   * Override to set the expected NVARCHAR behaviour.
   * @return whether to use NVARCHAR for strings or not
   */
  protected boolean expectedUsesNVARCHARforStrings() {
    return false;
  }


  /**
   * Checks the @link org.alfasoftware.morf.jdbc.SqlDialect.getSchemaConsistencyStatements(SchemaResource)} mechanism.
   */
  @Test
  public void testSchemaConsistencyStatements() {
    final SchemaResource schemaResource = createSchemaResourceForSchemaConsistencyStatements();

    assertThat(
      testDialect.getSchemaConsistencyStatements(schemaResource),
      expectedSchemaConsistencyStatements());
  }

  protected SchemaResource createSchemaResourceForSchemaConsistencyStatements() {
    final SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.empty());
    return schemaResource;
  }

  protected org.hamcrest.Matcher<java.lang.Iterable<? extends String>> expectedSchemaConsistencyStatements() { //NOSONAR // Remove usage of generic wildcard type // The generic wildcard type comes from hamcrest and is therefore unavoidable
    return emptyIterable();
  }



  /**
   * Checks the @link org.alfasoftware.morf.jdbc.SqlDialect.getSchemaConsistencyStatements(SchemaResource)} mechanism fallback.
   */
  @Test
  public void testSchemaConsistencyStatementsOnNoDatabaseMetaDataProvider() {
    final SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.empty());

    assertThat(
      testDialect.getSchemaConsistencyStatements(schemaResource),
      empty());
  }



  /**
   * Checks the @link org.alfasoftware.morf.jdbc.SqlDialect.getSchemaConsistencyStatements(SchemaResource)} mechanism fallback.
   */
  @Test
  public void testSchemaConsistencyStatementsOnWrongDatabaseMetaDataProvider() {
    final SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(mock(AdditionalMetadata.class)));

    assertThat(
      testDialect.getSchemaConsistencyStatements(schemaResource),
      empty());
  }


  /**
   * Format a value through the result set record for testing.
   *
   * @param value The value to format.
   * @return The formatted value.
   */
  private String checkDatabaseSafeStringToRecordValue(DataType dataType, String value) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);

    when(resultSet.getBigDecimal(anyInt())).thenReturn(value == null ? null : new BigDecimal(value));
    if (value == null) {
      when(resultSet.wasNull()).thenReturn(true);
    } else {
      when(resultSet.getBoolean(anyInt())).thenReturn(value.equals("1"));
    }

    return testDialect.resultSetToRecord(resultSet, ImmutableList.of(column("a", dataType))).getString("a");
  }


  /**
   * Format a value through the result set record for testing.
   *
   * @param value The value to format.
   * @return The formatted value.
   */
  private String checkDatabaseByteArrayToRecordValue(final byte[] value) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);

    when(resultSet.getBytes(anyInt())).thenReturn(value == null ? null : value);

    return testDialect.resultSetToRecord(resultSet, ImmutableList.of(column("a", DataType.BLOB))).getString("a");
  }


  /**
   * Tests non-null values are returned correctly from resultsets
   *
   * @throws SQLException If a SQL exception is thrown.
   */
  @Test
  public void testResultSetToRecord() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);

    List<DataType> dataTypes = Arrays.asList(DataType.values());
    List<Column> columns = dataTypes
        .stream()
        .filter(d -> !d.equals(DataType.NULL))
        .map(d -> column(d.name() + "Test", d))
        .collect(toList());

    when(resultSet.getLong(dataTypes.indexOf(DataType.BIG_INTEGER) + 1)).thenReturn(1L);
    when(resultSet.getBytes(dataTypes.indexOf(DataType.BLOB) + 1)).thenReturn(BYTE_ARRAY);
    when(resultSet.getString(dataTypes.indexOf(DataType.STRING) + 1)).thenReturn("test");
    when(resultSet.getBoolean(dataTypes.indexOf(DataType.BOOLEAN) + 1)).thenReturn(true);
    when(resultSet.getInt(dataTypes.indexOf(DataType.INTEGER) + 1)).thenReturn(3);
    when(resultSet.getBigDecimal(dataTypes.indexOf(DataType.DECIMAL) + 1)).thenReturn(new BigDecimal("1.23"));
    when(resultSet.getDate(dataTypes.indexOf(DataType.DATE) + 1)).thenReturn(java.sql.Date.valueOf("2010-07-02"));

    Record record = testDialect.resultSetToRecord(resultSet, columns);

    assertEquals(1, record.getLong(DataType.BIG_INTEGER.name() + "Test").longValue());
    assertEquals("test", record.getString(DataType.STRING.name() + "Test"));
    assertEquals(true, record.getBoolean(DataType.BOOLEAN.name() + "Test"));
    assertEquals(3, record.getInteger(DataType.INTEGER.name() + "Test").intValue());
    assertEquals(1.23D, record.getDouble(DataType.DECIMAL.name() + "Test").doubleValue(), 0.0001);

    assertEquals(new BigDecimal("1.23"), record.getBigDecimal(DataType.DECIMAL.name() + "Test"));
    assertEquals(new BigDecimal("1.23"), record.getObject(column(DataType.DECIMAL.name() + "Test", DataType.DECIMAL, 13, 2)));

    assertEquals(BASE64_ENCODED, record.getString(DataType.BLOB.name() + "Test"));
    assertArrayEquals(BYTE_ARRAY, record.getByteArray(DataType.BLOB.name() + "Test"));
    assertArrayEquals(BYTE_ARRAY, (byte[])record.getObject(column(DataType.BLOB.name() + "Test", DataType.BLOB)));

    assertEquals(java.sql.Date.valueOf("2010-07-02"), record.getDate(DataType.DATE.name() + "Test"));
    assertEquals(new LocalDate(2010, 7, 2), record.getLocalDate(DataType.DATE.name() + "Test"));
    assertEquals(java.sql.Date.valueOf("2010-07-02"), record.getObject(column(DataType.DATE.name() + "Test", DataType.DATE)));
  }


  /**
   * Checks that we return all nulls in the resulting record if the result set returns all nulls.
   *
   * @throws SQLException If a SQL exception is thrown.
   */
  @Test
  public void testResultSetToRecordNulls() throws SQLException {
    List<Column> columns = Arrays.asList(DataType.values())
        .stream()
        .filter(d -> !d.equals(DataType.NULL))
        .map(d -> column(d.name() + "Test", d))
        .collect(toList());

    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.wasNull()).thenReturn(true);

    Record record = testDialect.resultSetToRecord(resultSet, columns);

    columns.forEach(c -> assertNull(record.getObject(c)));
  }


  /**
   * Matches an InputStream to a byte array.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  private static final class ByteArrayMatcher implements ArgumentMatcher<byte[]> {

    /**
     * expected byte value of argument
     */
    private final byte[] expectedBytes;


    /**
     * @param expectedBytes expected byte value of argument.
     */
    public ByteArrayMatcher(final byte[] expectedBytes) {
      super();
      this.expectedBytes = Arrays.copyOf(expectedBytes, expectedBytes.length);
    }

    /**
     * {@inheritDoc}
     * @see org.mockito.ArgumentMatcher#matches(java.lang.Object)
     */
    @Override
    public boolean matches(final byte[] argument) {
      return Arrays.equals(argument, expectedBytes);
    }
  }


  /**
   * Enumeration of possible alterations for a column.
   */
  protected enum AlterationType {

    /**
     * Addition
     */
    ADD,

    /**
     * Alteration
     */
    ALTER,

    /**
     * Removal
     */
    DROP }
}
