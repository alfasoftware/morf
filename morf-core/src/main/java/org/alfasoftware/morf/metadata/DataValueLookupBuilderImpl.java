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
import java.util.ArrayList;
import java.util.Arrays;

import org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilder;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordDecorator;
import org.alfasoftware.morf.metadata.DataSetUtils.ValueMapper;
import org.alfasoftware.morf.metadata.ValueConverters.BigDecimalValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.BooleanValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.ByteArrayValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.DateValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.DoubleValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.IntegerConverter;
import org.alfasoftware.morf.metadata.ValueConverters.JodaLocalDateValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.LongValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.NullValueConverter;
import org.alfasoftware.morf.metadata.ValueConverters.StringValueConverter;
import org.joda.time.LocalDate;

/**
 * Implements {@link DataValueLookupBuilder}.
 * <p>
 * Note that this implementation skirts on the very edge of what could
 * plausibly be called OO. It's closer to C in many ways, using object arrays
 * to simulate node entries, and values combined with method references
 * instead of value types. This is completely deliberate. It used to be lovely
 * clean Java, but as it turns out, this code is called so intensely by batch
 * processing that the overhead of object churn was overwhelming and GC would
 * regularly hit 100% of CPU time. This code attempts to minimise creation of
 * new objects.
 * </p>
 * <p>
 * In normal operation, a new instance creates only a new 96-element object
 * array and nothing else.
 * </p>
 * <p>
 * <strong>Be careful with use of lambdas</strong>. They are used, but are
 * very carefully designed to be stateless, thus allowing the compiler to
 * inline them and avoid runtime allocation and GC.
 * </p>
 */
class DataValueLookupBuilderImpl implements DataValueLookupBuilder {

  /** We default storage to 16 columns.  This is more than most applications so as to minimise copying
   *  and resizing, but small enough that it has a relatively small footprint by default.
   */
  private static final int DEFAULT_INITIAL_SIZE = 16;

  /**
   * The indexes into our array "Y axis" at which the column name, column value and value converter are
   * found. Ooh, it's like JS, pre dot-notation.
   */
  private static final int NAME = 0;
  private static final int VALUE = 1;
  private static final int CONVERTER = 2;
  private static final int BLOCK_SIZE = 3;

  /** Storage is arranged in groups of BLOCK_SIZE elements, each block representing a column,
   *  so 0=Name of col 1, 1=Value of col 1, 2=Converter for col 1, 3=Name of col 2 etc. This
   *  is done to avoid multidimensional arrays which in Java are actually allocated as
   *  noncontinguous arrays of arrays and thus have a much higher construction and GC cost,
   *  or use of "node" objects which also have to be independently GCed. */
  private Object[] data;

  /** How many columns we have stored. */
  private int size;


  @Override
  public DataValueLookupBuilder withInitialColumnCount(int count) {
    if (data != null) {
      throw new IllegalStateException("Can't set column count after values have been specified.");
    }
    data = initialiseArray(count);
    return this;
  }

  @Override
  public DataValueLookupBuilder setNull(String columnName) {
    return set(columnName, null, NullValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setString(String columnName, String value) {
    return set(columnName, value, StringValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setInteger(String columnName, Integer value) {
    return set(columnName, value, IntegerConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setBoolean(String columnName, Boolean value) {
    return set(columnName, value, BooleanValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setLong(String columnName, Long value) {
    return set(columnName, value, LongValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setDouble(String columnName, Double value) {
    return set(columnName, value, DoubleValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setBigDecimal(String columnName, BigDecimal value) {
    return set(columnName, value, BigDecimalValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setDate(String columnName, java.sql.Date value) {
    return set(columnName, value, DateValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setLocalDate(String columnName, LocalDate value) {
    return set(columnName, value, JodaLocalDateValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder setByteArray(String columnName, byte[] value) {
    return set(columnName, value, ByteArrayValueConverter.instance());
  }

  @Override
  public DataValueLookupBuilder value(String columnName, String value) {
    return setString(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setObject(String columnName, Object value) {
    if (value == null) {
      return setNull(columnName);
    } else if (value instanceof String) {
      return setString(columnName, (String) value);
    } else if (value instanceof Integer) {
      return setInteger(columnName, (Integer) value);
    } else if (value instanceof Long) {
      return setLong(columnName, (Long) value);
    } else if (value instanceof BigDecimal) {
      return setBigDecimal(columnName, (BigDecimal) value);
    } else if (value instanceof Boolean) {
      return setBoolean(columnName, (Boolean) value);
    } else if (value instanceof Double) {
      return setDouble(columnName, (Double) value);
    } else if (value instanceof org.joda.time.LocalDate) {
      return setLocalDate(columnName, (LocalDate) value);
    } else if (value instanceof java.sql.Date) {
      return setDate(columnName, (java.sql.Date) value);
    } else if (value instanceof byte[]) {
      return setByteArray(columnName, (byte[]) value);
    } else {
      throw new IllegalArgumentException("Type [" + value.getClass().getCanonicalName() + "] not supported for values");
    }
  }

  @Override
  public String getValue(String name) {
    return getString(name);
  }

  @Override
  public Integer getInteger(String name) {
    return getAndConvertByName(name, (o, c) -> c.integerValue(o));
  }

  @Override
  public Boolean getBoolean(String name) {
    return getAndConvertByName(name, (o, c) -> c.booleanValue(o));
  }

  @Override
  public Long getLong(String name) {
    return getAndConvertByName(name, (o, c) -> c.longValue(o));
  }

  @Override
  public Double getDouble(String name) {
    return getAndConvertByName(name, (o, c) -> c.doubleValue(o));
  }

  @Override
  public String getString(String name) {
    return getAndConvertByName(name, (o, c) -> c.stringValue(o));
  }

  @Override
  public BigDecimal getBigDecimal(String name) {
    return getAndConvertByName(name, (o, c) -> c.bigDecimalValue(o));
  }

  @Override
  public java.sql.Date getDate(String name) {
    return getAndConvertByName(name, (o, c) -> c.dateValue(o));
  }

  @Override
  public LocalDate getLocalDate(String name) {
    return getAndConvertByName(name, (o, c) -> c.localDateValue(o));
  }

  @Override
  public byte[] getByteArray(String name) {
    return getAndConvertByName(name, ValueMapper.OBJECT_TO_BYTE_ARRAY);
  }

  @Override
  public String toString() {
    if (data == null) return "{}";
    StringBuilder builder = new StringBuilder('{');
    for (int i = 0 ; i < size ; i++) {
      if (i > 0) builder.append(", ");
      int offset = i * BLOCK_SIZE;
      builder.append(data[offset + NAME]).append("=").append(data[offset + VALUE]);
    }
    return builder.append('}').toString();
  }

  /**
   * Sets the value of a specified column, along with the converter to get the value
   * as different types.  Resizes or allocates the array as required.
   */
  private DataValueLookupBuilder set(String columnName, Object value, ValueConverter<?> converter) {
    if (data == null) {
      data = initialiseArray(DEFAULT_INITIAL_SIZE);
    } else if (size * BLOCK_SIZE == data.length) {
      grow();
    }
    int offset = size * BLOCK_SIZE;
    data[offset + NAME] = columnName;
    data[offset + VALUE] = value;
    data[offset + CONVERTER] = value == null ? NullValueConverter.instance() : converter;
    size++;
    return this;
  }

  /**
   * Creates the storage array.
   */
  private Object[] initialiseArray(int numberOfColumns) {
    return new Object[numberOfColumns * BLOCK_SIZE];
  }

  /**
   * Grow the array to accommodate new values. Based on {@link ArrayList}, but
   * without the overflow protection. We've got bigger problems if records get
   * that big.
   */
  private void grow() {
    data = Arrays.copyOf(data, (size + (size >> 1)) * BLOCK_SIZE);
  }

  /**
   * Fetches the value of the specified column, converting it to the target
   * type using the associated {@link ValueConverter} to the target type.
   */
  private final <STORED, RETURNED> RETURNED getAndConvertByName(String key, ValueMapper<STORED, RETURNED> mapper) {
    return getAndConvertByIndex(indexOf(key), mapper);
  }

  /**
   * O(n) search implementation.
   *
   * <p>We *could* create a hashtable here, but we would need to (a) define a
   * case-insensitive hash which avoids explicitly upper- or lower- casing the
   * input strings (because that has a cost in churn) and (b) maintain the
   * table ourselves, and there's no current evidence that this loop is a
   * significant cost in real usage. We can always do that if it looks
   * worthwhile.</p>
   *
   * <p>TODO WEB-66337 improve this</p>
   *
   * <p>We use -1 to indicate not found to avoid unnecessary boxing.</p>
   *
   * <p>Protected as it is used by {@link RecordDecorator}.</p>
   *
   * @param key The column name
   * @return The index at which the column is found.
   */
  protected int indexOf(String key) {
    // Search backwards, so that later values for the same column name override previously
    // entered values.
    for (int i = size - 1 ; i >= 0 ; i-- ) {
      if (key.equalsIgnoreCase((String) data[i * BLOCK_SIZE + NAME])) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Fetches the value at the specified index, converting it to the target
   * type using the associated {@link ValueConverter} to the target type.
   *
   * <p>Protected as it is used by {@link RecordDecorator}.</p>
   *
   * @param <STORED> The type actually stored in the internal array.
   * @param <RETURNED> The type being returned by the API call.
   * @param i The index.
   * @param mapper The mapper.
   * @return The value.
   */
  @SuppressWarnings("unchecked")
  protected final <STORED, RETURNED> RETURNED getAndConvertByIndex(int i, ValueMapper<STORED, RETURNED> mapper) {
    if (i == -1) return null;
    int offset = i * BLOCK_SIZE;
    return mapper.map((STORED) data[offset + VALUE], (ValueConverter<STORED>) data[offset + CONVERTER]);
  }
}
