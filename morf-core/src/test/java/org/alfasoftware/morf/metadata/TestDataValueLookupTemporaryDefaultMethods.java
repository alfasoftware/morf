package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;

import org.joda.time.LocalDate;
import org.junit.Test;

/**
 * During the transition from the old {@link DataValueLookup} API (string based)
 * to the new typed API, we have a set of default method implementations in place.
 * This tests them.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
public class TestDataValueLookupTemporaryDefaultMethods {

  private static final String FIELD = "AaA";


  @SuppressWarnings("deprecation")
  @Test
  public void testGetValue() {
    String string = "_)(*&^%$%^";
    DataValueLookup dataValueLookup = newDataValueLookup(string);
    assertThat(dataValueLookup.getValue("B"), nullValue());
    assertThat(dataValueLookup.getValue(FIELD), equalTo(string));
    assertThat(dataValueLookup.getValue(FIELD.toUpperCase()), equalTo(string));
  }


  @Test
  public void testGetString() {
    String string = "_)(*&^%$%^";
    DataValueLookup dataValueLookup = newDataValueLookup(string);
    assertThat(dataValueLookup.getString("B"), nullValue());
    assertThat(dataValueLookup.getString(FIELD), equalTo(string));
    assertThat(dataValueLookup.getString(FIELD.toUpperCase()), equalTo(string));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), string);
  }


  @Test
  public void testGetInteger() {
    DataValueLookup dataValueLookup = newDataValueLookup("3");
    assertThat(dataValueLookup.getInteger("B"), nullValue());
    assertThat(dataValueLookup.getInteger(FIELD), equalTo(3));
    assertThat(dataValueLookup.getInteger(FIELD.toUpperCase()), equalTo(3));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.INTEGER)), 3);
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "3");
  }


  @Test(expected = NumberFormatException.class)
  public void testGetIntegerBadCast() {
    newDataValueLookup("E3").getInteger(FIELD);
  }


  @Test
  public void testGetLong() {
    DataValueLookup dataValueLookup = newDataValueLookup("333333333333333333");
    assertThat(dataValueLookup.getLong("B"), nullValue());
    assertThat(dataValueLookup.getLong(FIELD), equalTo(333333333333333333L));
    assertThat(dataValueLookup.getLong(FIELD.toUpperCase()), equalTo(333333333333333333L));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.BIG_INTEGER)), 333333333333333333L);
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "333333333333333333");
  }


  @Test
  public void testGetBooleanTrue() {
    DataValueLookup dataValueLookup = newDataValueLookup("true");
    assertThat(dataValueLookup.getBoolean("B"), nullValue());
    assertThat(dataValueLookup.getBoolean(FIELD), equalTo(true));
    assertThat(dataValueLookup.getBoolean(FIELD.toUpperCase()), equalTo(true));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.BOOLEAN)), true);
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "true");
  }


  @Test
  public void testGetBooleanFalse() {
    DataValueLookup dataValueLookup = newDataValueLookup("tru");
    assertThat(dataValueLookup.getBoolean("B"), nullValue());
    assertThat(dataValueLookup.getBoolean(FIELD), equalTo(false));
    assertThat(dataValueLookup.getBoolean(FIELD.toUpperCase()), equalTo(false));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.BOOLEAN)), false);
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "tru");
  }


  @Test
  public void testGetLocalDate() {
    DataValueLookup dataValueLookup = newDataValueLookup("2010-07-02");
    assertThat(dataValueLookup.getLocalDate("B"), nullValue());
    assertThat(dataValueLookup.getLocalDate(FIELD), equalTo(new LocalDate(2010, 7, 2)));
    assertThat(dataValueLookup.getLocalDate(FIELD.toUpperCase()), equalTo(new LocalDate(2010, 7, 2)));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DATE)), java.sql.Date.valueOf("2010-07-02"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "2010-07-02");
  }


  @Test
  public void testGetDate() {
    DataValueLookup dataValueLookup = newDataValueLookup("2010-07-02");
    assertThat(dataValueLookup.getDate("B"), nullValue());
    assertThat(dataValueLookup.getDate(FIELD), equalTo(java.sql.Date.valueOf("2010-07-02")));
    assertThat(dataValueLookup.getDate(FIELD.toUpperCase()), equalTo(java.sql.Date.valueOf("2010-07-02")));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DATE)), java.sql.Date.valueOf("2010-07-02"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "2010-07-02");
  }


  @Test
  public void testGetDouble() {
    DataValueLookup dataValueLookup = newDataValueLookup("123.567890");
    assertThat(dataValueLookup.getDouble("B"), nullValue());
    assertThat(dataValueLookup.getDouble(FIELD), equalTo(123.567890D));
    assertThat(dataValueLookup.getDouble(FIELD.toUpperCase()), equalTo(123.567890D));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DECIMAL, 13, 6)), new BigDecimal("123.567890"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DECIMAL, 13, 8)), new BigDecimal("123.56789000"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "123.567890");
  }


  @Test
  public void testGetBigDecimalString() {
    DataValueLookup dataValueLookup = newDataValueLookup("123.567890");
    assertThat(dataValueLookup.getBigDecimal("B"), nullValue());
    assertThat(dataValueLookup.getBigDecimal(FIELD), equalTo(new BigDecimal("123.567890")));
    assertThat(dataValueLookup.getBigDecimal(FIELD.toUpperCase()), equalTo(new BigDecimal("123.567890")));

    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DECIMAL, 13, 6)), new BigDecimal("123.567890"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.DECIMAL, 13, 8)), new BigDecimal("123.56789000"));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), "123.567890");
  }


  @Test(expected = IllegalStateException.class)
  public void testBigDecimalRoundingNecessary() {
    DataValueLookup dataValueLookup = newDataValueLookup("123.567890");
    dataValueLookup.getObject(column(FIELD, DataType.DECIMAL, 13, 0));
  }


  @Test
  public void testGetByteArray() {
    byte[] data = new byte[]{ -127, -126, -125, 125, 126, 127 };
    DataValueLookup dataValueLookup = newDataValueLookup(encodeToBase64String(data));
    assertThat(dataValueLookup.getByteArray("B"), nullValue());
    assertTrue(Arrays.equals(dataValueLookup.getByteArray(FIELD), data));
    assertTrue(Arrays.equals(dataValueLookup.getByteArray(FIELD.toUpperCase()), data));
    assertTrue(Arrays.equals((byte[])dataValueLookup.getObject(SchemaUtils.column(FIELD, DataType.BLOB)), data));
    assertEquals(dataValueLookup.getObject(column(FIELD, DataType.STRING)), encodeToBase64String(data));
  }


  private DataValueLookup newDataValueLookup(String string) {
    DataValueLookup dataValueLookup = new DataValueLookup() {
      @Override
      public String getValue(String name) {
        return name.equalsIgnoreCase(FIELD) ? string : null;
      }
    };
    return dataValueLookup;
  }


  private String encodeToBase64String(byte[] toEncode) {
    return new String(java.util.Base64.getEncoder().encode(toEncode));
  }
}