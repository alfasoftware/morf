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

import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.joda.time.LocalDate;
import org.junit.Test;

/**
 * A testcase for the record helper.
 *
 *  @author Copyright (c) Alfa Financial Software 2011
 */
public class TestRecordHelper {

  /**
   * Test that the helper joins the values for the record correctly
   */
  @Test
  public void testJoinRecordValues() {

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

    String delimiter = "\t";
    String valueForNull = "\\N";

    assertEquals(
      "Joined record", "1\t1\tbar\t\\N\t\\N\t123.45\t2010-01-01\ttrue",
      RecordHelper.joinRecordValues(
        table.columns(),
        record()
          .setInteger(idColumn().getName(), 1)
          .setInteger(versionColumn().getName(), 1)
          .setString("nonEmptyStringField", "bar")
          .setString("emptyStringField", "")
          .setString("nullStringField", null)
          .setString("floatField", "123.45")
          .setDate("dateField", java.sql.Date.valueOf("2010-01-01"))
          .setBoolean("booleanField", true),
        delimiter,
        valueForNull
      )
    );

    // Also check a single space and falses
    assertEquals(
      "Joined record", "1\t1\tbar\t \t\\N\t123.45\t2010-01-01\tfalse",
      RecordHelper.joinRecordValues(
        table.columns(),
        record()
          .setInteger(idColumn().getName(), 1)
          .setInteger(versionColumn().getName(), 1)
          .setString("nonEmptyStringField", "bar")
          .setString("emptyStringField", " ")
          .setString("nullStringField", null)
          .setString("floatField", "123.45")
          .setDate("dateField", java.sql.Date.valueOf("2010-01-01"))
          .setBoolean("booleanField", false),
        delimiter,
        valueForNull
      )
    );
  }


  /**
   * Test that the data type conversion returns
   */
  @Test
  public void testConvertToComparableType() {

    assertEquals("foo", RecordHelper.convertToComparableType(column("blah", DataType.STRING, 10), record().setString("blah", "foo")));

    assertEquals(new BigDecimal("123"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 10, 0), record().setString("blah", "123")));
    assertEquals(new BigDecimal("123.00"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 13, 2), record().setString("blah", "123")));
    assertEquals(new BigDecimal("123.46"), RecordHelper.convertToComparableType(column("blah", DataType.DECIMAL, 13, 2), record().setString("blah", "123.456")));
  }

  /**
   * Test that the data type conversion returns
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testConvertToComparableTypeOld() {

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
    assertEquals("0.00000000001", RecordHelper.javaTypeToRecordValue(new BigDecimal("0.00000000001")));
  }
}
