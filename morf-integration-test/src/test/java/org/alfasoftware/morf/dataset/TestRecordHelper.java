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

package org.alfasoftware.morf.dataset;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.List;

import org.joda.time.LocalDate;
import org.junit.Test;

import org.alfasoftware.morf.jdbc.RecordValueToDatabaseSafeStringConverter;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import com.google.common.collect.ImmutableList;

/**
 * A testcase for the record helper.
 *
 *  @author Copyright (c) Alfa Financial Software 2011
 */
public class TestRecordHelper implements RecordValueToDatabaseSafeStringConverter {

  /**
   * Test that the helper joins the values for the record correctly
   */
  @Test
  public void testJoinRecordValues() {
    // We will need a list of column names
    List<String> columnNames = ImmutableList.<String> of("nonEmptyStringField", "emptyStringField", "nullStringField",
      "floatField", "dateField", "booleanField");

    // And a table with those fields
    Table table = table("fooTable")
      .columns(
        idColumn(),
        versionColumn(),
        column("nonEmptyStringField", DataType.STRING, 10), // as is
        column("emptyStringField", DataType.STRING, 10),    // empty strings stored as null
        column("nullStringField", DataType.STRING, 10),     // null values are handled independently of data type
        column("floatField", DataType.DECIMAL, 13, 2),      // as is
        column("dateField", DataType.DATE, 8),              // as is
        column("booleanField", DataType.BOOLEAN, 10)        // converted to lowercase
      );

    // Tab delimiter will do
    String delimiter = "\t";

    // ... now a record with some interesting values for the columns
    // Need to defer the translation to the dialect!
    assertEquals(
      "Joined record", "bar\t\\N\t\\N\t123.45\t20100101\t1",
      RecordHelper.joinRecordValues(table, new MockRecord(table, "1", "1", "bar", "", null, "123.45", "20100101", "True"),
        columnNames, delimiter, this));

    // a single space needs to be handled too, and falses
    assertEquals(
      "Joined record", "bar\t \t\\N\t123.45\t20100101\t0",
      RecordHelper.joinRecordValues(table, new MockRecord(table, "1", "1", "bar", " ", null, "123.45", "20100101", "False"),
        columnNames, delimiter, this));
  }


  /**
   * Test that the data type conversion returns
   */
  @Test
  public void testConvertToComparableType() {

    assertEquals("foo", RecordHelper.convertToComparableType(column("blah", DataType.STRING, 10), "foo"));

    assertEquals(new BigDecimal("123"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 10, 0), "123"));
    assertEquals(new BigDecimal("123.00"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 13, 2), "123"));
    assertEquals(new BigDecimal("123.46"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 13, 2), "123.456"));
  }


  /**
   * Tests {@link RecordHelper#recordValueToJavaType(String, Class)}
   */
  @Test
  public void testRecordValueToJavaType() {
    assertEquals(1, RecordHelper.recordValueToJavaType("1", Integer.class).intValue());
    assertEquals(349573485703495780L, RecordHelper.recordValueToJavaType("349573485703495780", Long.class).longValue());
    assertEquals("String", RecordHelper.recordValueToJavaType("String", String.class));
    assertEquals(true, RecordHelper.recordValueToJavaType("true", Boolean.class).booleanValue());
    assertEquals(false, RecordHelper.recordValueToJavaType("false", Boolean.class).booleanValue());
    assertEquals(5.2232D, RecordHelper.recordValueToJavaType("5.2232", Double.class).doubleValue(), 0.000001);
    assertEquals(new LocalDate(2041, 12, 31), RecordHelper.recordValueToJavaType("2041-12-31", LocalDate.class));
  }


  /**
   * Tests {@link RecordHelper#javaTypeToRecordValue(Object)}
   */
  @Test
  public void testJavaTypeToRecordValue() {
    assertEquals("1", RecordHelper.javaTypeToRecordValue(1));
    assertEquals("349573485703495780", RecordHelper.javaTypeToRecordValue(349573485703495780L));
    assertEquals("String", RecordHelper.javaTypeToRecordValue("String"));
    assertEquals("true", RecordHelper.javaTypeToRecordValue(true));
    assertEquals("false", RecordHelper.javaTypeToRecordValue(false));
    assertEquals("5.2232", RecordHelper.javaTypeToRecordValue(5.2232D));
    assertEquals("2041-12-31", RecordHelper.javaTypeToRecordValue(new LocalDate(2041, 12, 31)));
  }


  /**
   * @see org.alfasoftware.morf.dataset.RecordHelper.RecordValueToDatabaseSafeStringConverter#recordValueToDatabaseSafeString(org.alfasoftware.morf.metadata.Column, java.lang.String)
   */
  @Override
  public String recordValueToDatabaseSafeString(Column column, String sourceValue) {
    Comparable<?> comparableType = RecordHelper.convertToComparableType(column, sourceValue);
    if (comparableType == null) {
      return "\\N";
    } else if (comparableType instanceof Boolean) {
      return (Boolean)comparableType ? "1" : "0";
    } else {
      return comparableType.toString();
    }
  }
}
