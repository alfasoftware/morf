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

package org.alfasoftware.morf.metadata;

import java.math.BigDecimal;

import org.apache.commons.codec.binary.Base64;
import org.joda.time.LocalDate;

/**
 * Standard implementations of {@link ValueConverter}.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
final class ValueConverters {

  /**
   * Base implementation of {@link AbstractValueConverter}.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  private abstract static class AbstractValueConverter<T> implements ValueConverter<T> {

    @Override
    public String stringValue(T value) {
      return value.toString();
    }

    @Override
    public BigDecimal bigDecimalValue(T value) {
      throw new NumberFormatException("Cannot express [" + value + "] as a BigDecimal");
    }

    /**
     * Always returns true or false in line with the contract of {@link Boolean#valueOf(String)}.
     */
    @Override
    public Boolean booleanValue(T value) {
      return Boolean.valueOf(value.toString());
    }

    @Override
    public byte[] byteArrayValue(T value) {
      throw new ClassCastException("Cannot express [" + value + "] as a byte array");
    }

    @Override
    public java.sql.Date dateValue(T value) {
      throw new ClassCastException("Cannot express [" + value + "] as a Date");
    }

    @Override
    public Double doubleValue(T value) {
      throw new NumberFormatException("Cannot express [" + value + "] as a Double");
    }

    @Override
    public Integer integerValue(T value) {
      throw new NumberFormatException("Cannot express [" + value + "] as an Integer");
    }

    @Override
    public org.joda.time.LocalDate localDateValue(T value) {
      throw new IllegalArgumentException("Cannot express [" + value + "] as a LocalDate");
    }

    @Override
    public Long longValue(T value) {
      throw new NumberFormatException("Cannot express [" + value + "] as a Long");
    }
  }


  /**
   * Common subtype for conversion of {@link Number} instances.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   * @param <T>
   */
  private static class AbstractNumberConverter<T extends Number> extends AbstractValueConverter<T> {

    @Override
    public Integer integerValue(T value) {
      return value.intValue();
    }

    @Override
    public Long longValue(T value) {
      return value.longValue();
    }

    @Override
    public Double doubleValue(T value) {
      return value.doubleValue();
    }

    @Override
    public Boolean booleanValue(T value) {
      return value.intValue() != 0;
    }

    /**
     * Naive implementation which requires a string conversion. Subtypes define more efficient implementations.
     */
    @Override
    public BigDecimal bigDecimalValue(T value) {
      return new BigDecimal(value.toString());
    }
  }


  /**
   * Further specialises {@link Number} for most efficient retrieval of
   * integers.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class IntegerConverter extends AbstractNumberConverter<Integer> {
    private static IntegerConverter INSTANCE = new IntegerConverter();
    static IntegerConverter instance() {
      return INSTANCE;
    }
    @Override
    public Integer integerValue(Integer value) {
      return value;
    }
    @Override
    public BigDecimal bigDecimalValue(Integer value) {
      return new BigDecimal(value);
    }
  }


  /**
   * Further specialises {@link Number} for most efficient retrieval of
   * longs.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class LongValueConverter extends AbstractNumberConverter<Long> {
    private static LongValueConverter INSTANCE = new LongValueConverter();
    static LongValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public Long longValue(Long value) {
      return value;
    }
    @Override
    public BigDecimal bigDecimalValue(Long value) {
      return new BigDecimal(value);
    }
  }


  /**
   * Further specialises {@link Number} for most efficient retrieval of
   * doubles.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class DoubleValueConverter extends AbstractNumberConverter<Double> {
    private static DoubleValueConverter INSTANCE = new DoubleValueConverter();
    static DoubleValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public Double doubleValue(Double value) {
      return value;
    }
    @Override
    public BigDecimal bigDecimalValue(Double value) {
      return BigDecimal.valueOf(value);
    }
  }


  /**
   * Further specialises {@link Number} for most efficient retrieval of
   * big decimals.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class BigDecimalValueConverter extends AbstractNumberConverter<BigDecimal> {
    private static BigDecimalValueConverter INSTANCE = new BigDecimalValueConverter();
    static BigDecimalValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public BigDecimal bigDecimalValue(BigDecimal value) {
      return value;
    }
    @Override
    public String stringValue(BigDecimal value) {

      String decimalString = value.toPlainString();

      // remove trailing zeros and clean up decimal point
      int decimalPointIdx = decimalString.indexOf('.');

      if (decimalPointIdx == -1) {
        return decimalString;
      }

      int idx;
      // Work back from the end of the string, looking for zeros, stopping when we find anything that isn't...
      for (idx = decimalString.length()-1; idx>=decimalPointIdx; idx--) {
        if (decimalString.charAt(idx) != '0') break;
      }

      // If the last thing is now the decimal point, step past that...
      if (idx == decimalPointIdx) {
        idx--;
      }

      return decimalString.substring(0, idx+1);
    }
  }


  /**
   * Efficient retrieval of booleans.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class BooleanValueConverter extends AbstractValueConverter<Boolean> {
    private static BooleanValueConverter INSTANCE = new BooleanValueConverter();
    static BooleanValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public Boolean booleanValue(Boolean value) {
      return value;
    }
  }


  /**
   * Efficient retrieval of dates.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class DateValueConverter extends AbstractValueConverter<java.sql.Date> {
    private static DateValueConverter INSTANCE = new DateValueConverter();
    static DateValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public java.sql.Date dateValue(java.sql.Date value) {
      return value;
    }
    @Override
    public LocalDate localDateValue(java.sql.Date value) {
      return LocalDate.fromDateFields(value);
    }
  }


  /**
   * Efficient retrieval of byte arrays.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class ByteArrayValueConverter extends AbstractValueConverter<byte[]> {
    private static ByteArrayValueConverter INSTANCE = new ByteArrayValueConverter();
    static ByteArrayValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public byte[] byteArrayValue(byte[] value) {
      return value;
    }
    @Override
    public String stringValue(byte[] value) {
      return Base64.encodeBase64String(value);
    }
  }


  /**
   * Efficient retrieval of joda dates.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class JodaLocalDateValueConverter extends AbstractValueConverter<org.joda.time.LocalDate> {
    private static JodaLocalDateValueConverter INSTANCE = new JodaLocalDateValueConverter();
    static JodaLocalDateValueConverter instance() {
      return INSTANCE;
    }
    @Override
    public org.joda.time.LocalDate localDateValue(LocalDate value) {
      return value;
    }
    @Override
    public java.sql.Date dateValue(LocalDate value) {
      return java.sql.Date.valueOf(value.toString());
    }
  }


  /**
   * Strings can represent anything. Here we do the conversions.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class StringValueConverter extends AbstractValueConverter<String> {
    private static StringValueConverter INSTANCE = new StringValueConverter();

    static StringValueConverter instance() {
      return INSTANCE;
    }

    @Override
    public String stringValue(String value) {
      return value;
    }

    @Override
    public BigDecimal bigDecimalValue(String value) {
      return new BigDecimal(value);
    }

    @Override
    public Boolean booleanValue(String value) {
      return Boolean.valueOf(value);
    }

    @Override
    public java.sql.Date dateValue(String value) {
      return java.sql.Date.valueOf(value);
    }

    @Override
    public Double doubleValue(String value) {
      return Double.valueOf(value);
    }

    @Override
    public Integer integerValue(String value) {
      return Integer.valueOf(value);
    }

    @Override
    public org.joda.time.LocalDate localDateValue(String value) {
      return org.joda.time.LocalDate.parse(value, DataValueLookupHelper.FROM_YYYY_MM_DD);
    }

    @Override
    public Long longValue(String value) {
      return Long.valueOf(value);
    }

    @Override
    public byte[] byteArrayValue(String value) {
      return Base64.decodeBase64(value);
    }
  }


  /**
   * Efficient retrieval of null values.
   *
   * @author Copyright (c) CHP Consulting Ltd. 2017
   */
  static final class NullValueConverter implements ValueConverter<Object> {
    private static final NullValueConverter INSTANCE = new NullValueConverter();

    static NullValueConverter instance() {
      return INSTANCE;
    }

    @Override public BigDecimal bigDecimalValue(Object o) { return null; }
    @Override public Boolean booleanValue(Object o) { return null; }
    @Override public byte[] byteArrayValue(Object o) { return null; } // NOPMD it is an intentional part of the contract to return null arrays.
    @Override public java.sql.Date dateValue(Object o) { return null; }
    @Override public Double doubleValue(Object o) { return null; }
    @Override public Integer integerValue(Object o) { return null; }
    @Override public org.joda.time.LocalDate localDateValue(Object o) { return null; }
    @Override public Long longValue(Object o) { return null; }
    @Override public String stringValue(Object o) { return null; }
  }


}

