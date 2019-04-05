package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;

import org.alfasoftware.morf.dataset.BaseRecordMatcher;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordDecorator;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.LocalDate;
import org.junit.Test;


/**
 * Tests {@link DataSetUtils}.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
public class TestDataSetUtils {

  private static final String INTEGER_COLUMN = "A";
  private static final String STRING_COLUMN = "B";
  private static final String BIG_DECIMAL_COLUMN = "C";
  private static final String BOOLEAN_COLUMN = "D";
  private static final String DATE_COLUMN = "E";
  private static final String LOCAL_DATE_COLUMN = "F";
  private static final String LONG_COLUMN = "H";
  private static final String BLOB_COLUMN = "I";
  private static final String UNTYPED_COLUMN = "J";

  private static final Date DATE = java.sql.Date.valueOf("2010-07-02");
  private static final Boolean BOOLEAN = true;
  private static final BigDecimal BIG_DECIMAL = new BigDecimal("10.000");
  private static final String STRING = "2";
  private static final Integer INTEGER = 1;
  private static final LocalDate LOCAL_DATE = new LocalDate();
  private static final Long LONG = 3333333333333333333L;
  private static final byte[] BYTE_ARRAY = new byte[]{ -127, 127 };
  private static final String VALUE = "erg";

  @SuppressWarnings("deprecation")
  private static final RecordBuilder BASE_RECORD = record()
      .setInteger(INTEGER_COLUMN, INTEGER)
      .setString(STRING_COLUMN, STRING)
      .setBigDecimal(BIG_DECIMAL_COLUMN, BIG_DECIMAL)
      .setBoolean(BOOLEAN_COLUMN, BOOLEAN)
      .setDate(DATE_COLUMN, DATE)
      .setLocalDate(LOCAL_DATE_COLUMN, LOCAL_DATE)
      .setLong(LONG_COLUMN, LONG)
      .setByteArray(BLOB_COLUMN, BYTE_ARRAY)
      .value(UNTYPED_COLUMN, VALUE);

  private static final RecordBuilder BASE_RECORD_BY_OBJECT = record()
      .setObject(INTEGER_COLUMN, INTEGER)
      .setObject(STRING_COLUMN, STRING)
      .setObject(BIG_DECIMAL_COLUMN, BIG_DECIMAL)
      .setObject(BOOLEAN_COLUMN, BOOLEAN)
      .setObject(DATE_COLUMN, DATE)
      .setObject(LOCAL_DATE_COLUMN, LOCAL_DATE)
      .setObject(LONG_COLUMN, LONG)
      .setObject(BLOB_COLUMN, BYTE_ARRAY)
      .setObject(UNTYPED_COLUMN, VALUE);


  @Test
  public void testValues() {
    assertThat(
      BASE_RECORD.getValues(),
      containsInAnyOrder(
        new DataValueBean(LOCAL_DATE_COLUMN, LOCAL_DATE),
        new DataValueBean(LONG_COLUMN, LONG),
        new DataValueBean(BLOB_COLUMN.toLowerCase(), BYTE_ARRAY), // Ensure equals is type insensitive
        new DataValueBean(UNTYPED_COLUMN, VALUE),
        new DataValueBean(INTEGER_COLUMN, INTEGER),
        new DataValueBean(STRING_COLUMN, STRING),
        new DataValueBean(BIG_DECIMAL_COLUMN, BIG_DECIMAL),
        new DataValueBean(BOOLEAN_COLUMN, BOOLEAN),
        new DataValueBean(DATE_COLUMN, DATE)
      )
    );
  }


  @Test
  public void testValuesDecorator() {
    assertThat(
      RecordDecorator.of( // Nested decorators
          RecordDecorator.of(BASE_RECORD)
              .setString(STRING_COLUMN.toLowerCase(), "Overriden") // Ensure overriding honours case insensitivity
              .setBoolean(BOOLEAN_COLUMN, !BOOLEAN))
                  .setLong(LONG_COLUMN, 125123L)
                  .getValues(),
      containsInAnyOrder(
        new DataValueBean(BIG_DECIMAL_COLUMN, BIG_DECIMAL),
        new DataValueBean(BOOLEAN_COLUMN, !BOOLEAN),
        new DataValueBean(DATE_COLUMN, DATE),
        new DataValueBean(LOCAL_DATE_COLUMN, LOCAL_DATE),
        new DataValueBean(INTEGER_COLUMN, INTEGER),
        new DataValueBean(STRING_COLUMN, "Overriden"),
        new DataValueBean(LONG_COLUMN, 125123L),
        new DataValueBean(BLOB_COLUMN, BYTE_ARRAY),
        new DataValueBean(UNTYPED_COLUMN, VALUE)
      )
    );
  }


  /**
   * Check that getters get the right responses for all the type conversions they should support.
   */
  @Test
  public void testValueAndObjectGetters() {
    assertThat(BASE_RECORD, originalMatcher());
    assertThat(BASE_RECORD_BY_OBJECT, originalMatcher());
  }


  @Test
  public void testIntegerGetters() {
    assertEquals(-1, record().setInteger(INTEGER_COLUMN, -1).getInteger(INTEGER_COLUMN).intValue());
    assertEquals(0, record().setInteger(INTEGER_COLUMN, 0).getInteger(INTEGER_COLUMN).intValue());
    assertEquals(1, record().setInteger(INTEGER_COLUMN, 1).getInteger(INTEGER_COLUMN).intValue());

    assertEquals(INTEGER, BASE_RECORD.getInteger(INTEGER_COLUMN));
    assertEquals(INTEGER.longValue(), BASE_RECORD.getLong(INTEGER_COLUMN).longValue());
    assertEquals(INTEGER.doubleValue(), BASE_RECORD.getDouble(INTEGER_COLUMN).doubleValue(), 0.00001);
    assertEquals(new BigDecimal(INTEGER), BASE_RECORD.getBigDecimal(INTEGER_COLUMN));
  }


  @Test
  public void testBigDecimalGetters() {
    assertEquals(BigDecimal.ZERO, record().setBigDecimal(BIG_DECIMAL_COLUMN, BigDecimal.ZERO).getBigDecimal(BIG_DECIMAL_COLUMN));
    assertEquals(BigDecimal.ONE, record().setBigDecimal(BIG_DECIMAL_COLUMN, BigDecimal.ONE).getBigDecimal(BIG_DECIMAL_COLUMN));

    assertEquals(BIG_DECIMAL.longValue(), BASE_RECORD.getLong(BIG_DECIMAL_COLUMN).longValue());
    assertEquals(BIG_DECIMAL.intValue(), BASE_RECORD.getInteger(BIG_DECIMAL_COLUMN).intValue());
    assertEquals(BIG_DECIMAL.doubleValue(), BASE_RECORD.getDouble(BIG_DECIMAL_COLUMN).doubleValue(), 0.00001);
    assertEquals(BIG_DECIMAL, BASE_RECORD.getBigDecimal(BIG_DECIMAL_COLUMN));
  }


  @Test
  public void testLongGetters() {
    assertEquals(-1L, record().setLong(LONG_COLUMN, -1L).getLong(LONG_COLUMN).longValue());
    assertEquals(0L, record().setLong(LONG_COLUMN, 0L).getLong(LONG_COLUMN).longValue());
    assertEquals(1L, record().setLong(LONG_COLUMN, 1L).getLong(LONG_COLUMN).longValue());

    assertEquals(LONG, BASE_RECORD.getLong(LONG_COLUMN));
    assertEquals(LONG.intValue(), BASE_RECORD.getInteger(LONG_COLUMN).intValue());
    assertEquals(LONG.doubleValue(), BASE_RECORD.getDouble(LONG_COLUMN).doubleValue(), 0.00001);
    assertEquals(new BigDecimal(LONG), BASE_RECORD.getBigDecimal(LONG_COLUMN));
  }


  @Test
  public void testByteArrayGetters() {
    assertArrayEquals(BYTE_ARRAY, BASE_RECORD.getByteArray(BLOB_COLUMN));
  }


  @Test
  public void testDoubleGetters() {
    String col = "CoL";

    assertEquals(0.1123D, record().setDouble(col, 0.1123D).getDouble(col).doubleValue(), 0.00001);
    assertEquals(new BigDecimal("0.1123"), record().setDouble(col, 0.1123D).getBigDecimal(col).setScale(4, RoundingMode.HALF_UP));
  }


  @Test
  public void testStringGetters() {
    String col = "CoL";

    assertEquals("010.00",                record().setString(col, "010.00").getString(col));
    assertEquals(10L,                     record().setString(col, "10")    .getLong(col).longValue());
    assertEquals(10,                      record().setString(col, "10")    .getInteger(col).intValue());
    assertEquals(10D,                     record().setString(col, "10")    .getDouble(col).doubleValue(), 0.00001);
    assertEquals(new BigDecimal("10.00"), record().setString(col, "010.00").getBigDecimal(col));

    assertEquals(new LocalDate(2009, 12, 31),         record().setString(col, "2009-12-31").getLocalDate(col));
    assertEquals(java.sql.Date.valueOf("2009-12-31"), record().setString(col, "2009-12-31").getDate(col));

    assertEquals(true,  record().setString(col, "true") .getBoolean(col));
    assertEquals(false, record().setString(col, "false").getBoolean(col));
    assertEquals(false, record().setString(col, "tru")  .getBoolean(col));

    byte[] blobValue = new byte[] { 1, 2, 3, 4, 5 };
    assertEquals(Base64.encodeBase64String(blobValue), record().setString(col, Base64.encodeBase64String(blobValue)).getString(col));
    assertArrayEquals(blobValue,                       record().setString(col, Base64.encodeBase64String(blobValue)).getByteArray(col));
  }


  @Test
  public void testBooleanGetters() {
    String col = "CoL";
    assertEquals(true,  record().setBoolean(col, true).getBoolean(col));
    assertEquals(false, record().setBoolean(col, false).getBoolean(col));
    assertEquals("true",  record().setBoolean(col, true).getString(col));
    assertEquals("false", record().setBoolean(col, false).getString(col));
  }


  @Test
  public void testDateGetters() {
    String col = "CoL";
    Date dateValue = java.sql.Date.valueOf("1977-10-10");
    LocalDate localDateValue = new LocalDate(1977, 10, 10);
    assertEquals(dateValue,  record().setDate(col, dateValue).getDate(col));
    assertEquals(dateValue.toString(), record().setDate(col, dateValue).getString(col));
    assertEquals(localDateValue, record().setDate(col, dateValue).getLocalDate(col));
  }


  @Test
  public void testLocalDateGetters() {
    String col = "CoL";
    Date dateValue = java.sql.Date.valueOf("1977-10-10");
    LocalDate localDateValue = new LocalDate(1977, 10, 10);
    assertEquals(localDateValue, record().setLocalDate(col, localDateValue).getLocalDate(col));
    assertEquals(localDateValue.toString(), record().setLocalDate(col, localDateValue).getString(col));
    assertEquals(dateValue, record().setLocalDate(col, localDateValue).getDate(col));
  }


  /**
   * Check all data types arrive unchanged if unmodified.
   */
  @Test
  public void testIdentityDecorator() {
    assertThat(
      RecordDecorator.of(BASE_RECORD),
      originalMatcher()
    );
  }


  /**
   * Check that a zero capacity decorator is permitted.
   */
  @Test
  public void testIdentityDecoratorWithZeroCapacity() {
    assertThat(
      RecordDecorator.ofWithInitialCapacity(BASE_RECORD, 0),
      originalMatcher()
    );
  }


  /**
   * Check that array resizing of decorators works.
   */
  @Test
  public void testDecoratorExpansion() {
    assertThat(
      RecordDecorator.ofWithInitialCapacity(BASE_RECORD, 1)
        .setString("additional1", "TEST1")
        .setString("additional2", "TEST2")
        .setString("additional3", "TEST3"),
      originalMatcher()
        .withValue("additional1", "TEST1")
        .withValue("ADDITIONAL2", "TEST2")
        .withValue("Additional3", "TEST3")
    );
  }


  /**
   * Ensures that we do indeed get null when there is no value.
   */
  @Test
  public void testNoValueFromDecorator() {
    assertNull(RecordDecorator.of(record()).getInteger(INTEGER_COLUMN));
  }


  /**
   * Ensures that we do indeed get null when null is explicitly specified in the underlying record.
   */
  @Test
  public void testNullValueFromDecorator() {
    assertNull(RecordDecorator.of(record().setInteger(INTEGER_COLUMN, null)).getInteger(INTEGER_COLUMN));
  }


  /**
   * Ensures that we can null-out a record even if the underlying has a value
   */
  @Test
  public void testNullOverrideFromDecorator() {
    assertNull(RecordDecorator.of(record().setInteger(INTEGER_COLUMN, 3)).setInteger(INTEGER_COLUMN, null).getInteger(INTEGER_COLUMN));
  }



  /**
   * Test overrides of all types using string input values.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testOverrideAndExtendUsingValue() {
    assertThat(
      RecordDecorator.of(BASE_RECORD)
        .value(INTEGER_COLUMN, "3")
        .value(BIG_DECIMAL_COLUMN, "4.23")
        .value(BOOLEAN_COLUMN, "false")
        .value(DATE_COLUMN, "1990-01-01")
        .value(LOCAL_DATE_COLUMN, "1990-01-02")
        .value(LONG_COLUMN, "2")
        .value(BLOB_COLUMN, Base64.encodeBase64String(new byte[] { -126, 126 })),
      mutatedMatcher()
    );
  }


  /*
   * Test overrides of all types using typed input values.
   */
  @Test
  public void testOverrideAndExtendUsingTyped() {
    assertThat(
      RecordDecorator.of(BASE_RECORD)
        .setInteger(INTEGER_COLUMN, 3)                                 // Type match
        .setString(BIG_DECIMAL_COLUMN, "4.23")                             // Type conversion
        .setBoolean(BOOLEAN_COLUMN, false)
        .setLocalDate(DATE_COLUMN, new LocalDate(1990, 1, 1))       // Type conversion
        .setDate(LOCAL_DATE_COLUMN, java.sql.Date.valueOf("1990-01-02"))  // Type conversion
        .setInteger(LONG_COLUMN, 2)                                 // Type conversion
        .setByteArray(BLOB_COLUMN, new byte[] { -126, 126 }),
      mutatedMatcher()
    );
  }


  /*
   * Tests that if we specify a value twice, we get the new value
   */
  @Test
  public void testOverwriteValue() {
    assertEquals("B", record().setString("a", "A").setString("a", "B").getString("a"));
  }


  /*
   * Tests that if we specify a value twice and the second is a null, we get null
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testOverwriteValueWithNull() {
    assertNull(record().setString("a", "A").value("a", null).getString("a"));
  }


  /*
   * Tests that if we specify a value twice where the first was a null, we get the new value
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testOverwriteNullWithValue() {
    assertEquals("B", record().setString("a", null).value("a", "B").getString("a"));
  }


  private BaseRecordMatcher originalMatcher() {
    return BaseRecordMatcher.create()
      .withValue(INTEGER_COLUMN, INTEGER.toString())
      .withObject(column(INTEGER_COLUMN, DataType.INTEGER), INTEGER)
      .withValue(STRING_COLUMN, STRING)
      .withObject(column(STRING_COLUMN, DataType.STRING), STRING)
      .withValue(BIG_DECIMAL_COLUMN, "10")
      .withObject(column(BIG_DECIMAL_COLUMN, DataType.DECIMAL, 13, BIG_DECIMAL.scale()), BIG_DECIMAL)
      .withValue(BOOLEAN_COLUMN, BOOLEAN.toString())
      .withObject(column(BOOLEAN_COLUMN, DataType.BOOLEAN), BOOLEAN)
      .withValue(DATE_COLUMN, DATE.toString())
      .withObject(column(DATE_COLUMN, DataType.DATE), DATE)
      .withValue(LOCAL_DATE_COLUMN, LOCAL_DATE.toString())
      .withObject(column(LOCAL_DATE_COLUMN, DataType.DATE), java.sql.Date.valueOf(LOCAL_DATE.toString()))
      .withValue(LONG_COLUMN, LONG.toString())
      .withObject(column(LONG_COLUMN, DataType.BIG_INTEGER), LONG)
      .withValue(BLOB_COLUMN, Base64.encodeBase64String(BYTE_ARRAY))
      .withObject(column(BLOB_COLUMN, DataType.BLOB), BYTE_ARRAY)
      .withValue(UNTYPED_COLUMN, VALUE)
      .withObject(column(UNTYPED_COLUMN, DataType.STRING), VALUE);
  }


  private BaseRecordMatcher mutatedMatcher() {
    return BaseRecordMatcher.create()
      .withValue(INTEGER_COLUMN, "3")
      .withObject(column(INTEGER_COLUMN, DataType.INTEGER), 3)
      .withValue(STRING_COLUMN, STRING)
      .withObject(column(STRING_COLUMN, DataType.STRING), STRING)
      .withValue(BIG_DECIMAL_COLUMN, "4.23")
      .withObject(column(BIG_DECIMAL_COLUMN, DataType.DECIMAL, 13, 2), new BigDecimal("4.23"))
      .withValue(BOOLEAN_COLUMN, "false")
      .withObject(column(BOOLEAN_COLUMN, DataType.BOOLEAN), false)
      .withValue(DATE_COLUMN, "1990-01-01")
      .withObject(column(DATE_COLUMN, DataType.DATE), java.sql.Date.valueOf("1990-01-01"))
      .withValue(LOCAL_DATE_COLUMN, "1990-01-02")
      .withObject(column(LOCAL_DATE_COLUMN, DataType.DATE), java.sql.Date.valueOf("1990-01-02"))
      .withValue(LONG_COLUMN, "2")
      .withObject(column(LONG_COLUMN, DataType.BIG_INTEGER), 2L)
      .withValue(BLOB_COLUMN, Base64.encodeBase64String(new byte[] { -126, 126 }))
      .withObject(column(BLOB_COLUMN, DataType.BLOB), new byte[] { -126, 126 })
      .withValue(UNTYPED_COLUMN, VALUE)
      .withObject(column(UNTYPED_COLUMN, DataType.STRING), VALUE);
  }
}