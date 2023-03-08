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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.alfasoftware.morf.metadata.DataSetUtils.DataValueLookupBuilder;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

/**
 * Implements {@link DataValueLookupBuilder}.
 * <p>
 * This class has been carefully implemented to minimise object churn and
 * memory usage. In normal operation, a new instance creates only a new
 * 16-element object array, one object reference, and nothing else. All
 * metadata is commonised and interned.
 * </p>
 * <p>
 * <strong>Be careful with use of lambdas</strong>. They are used, but are
 * very carefully designed to be stateless, thus allowing the compiler to
 * inline them and avoid runtime allocation and GC.
 * </p>
 */
class DataValueLookupBuilderImpl implements DataValueLookupBuilder, Serializable {

  /** We default storage to 16 columns.  This is more than most applications so as to minimise copying
   *  and resizing, but small enough that it has a relatively small footprint by default.
   */
  private static final int DEFAULT_INITIAL_SIZE = 16;

  private static final long serialVersionUID = -96755592321546764L;

  @Nullable
  private Object[] data;

  /** Describes the record content. Highly interned. */
  @Nullable
  protected DataValueLookupMetadata metadata;

  /**
   * Default no-arg constructor.
   */
  DataValueLookupBuilderImpl() {}

  /**
   * Copy constructor
   *
   * @param copyFrom The object to copy.
   * @param capacity The additional capacity to reserve.
   */
  DataValueLookupBuilderImpl(DataValueLookupBuilderImpl copyFrom, int capacity) {
    this.metadata = copyFrom.metadata;
    if (copyFrom.data == null) {
      this.data = initialiseArray(capacity);
    } else {
      this.data = Arrays.copyOf(copyFrom.data, copyFrom.metadata.getColumnNames().size() + capacity);
    }
  }

  @Override
  public DataValueLookupBuilder withInitialColumnCount(int count) {
    if (data != null) {
      throw new IllegalStateException("Can't set column count after values have been specified.");
    }
    data = initialiseArray(count);
    return this;
  }

  private ValueConverter<?> toConverter(Object value) {
    if (value == null) {
      return NullValueConverter.instance();
    } else if (value instanceof String) {
      return StringValueConverter.instance();
    } else if (value instanceof BigDecimal) {
      return BigDecimalValueConverter.instance();
    } else if (value instanceof Date) {
      return DateValueConverter.instance();
    } else if (value instanceof LocalDate) {
      return JodaLocalDateValueConverter.instance();
    } else if (value instanceof Long) {
      return LongValueConverter.instance();
    } else if (value instanceof Integer) {
      return IntegerConverter.instance();
    } else if (value instanceof Boolean) {
      return BooleanValueConverter.instance();
    } else if (value instanceof Double) {
      return DoubleValueConverter.instance();
    } else if (value instanceof byte[]) {
      return ByteArrayValueConverter.instance();
    } else {
      throw new UnsupportedOperationException("Type [" + value.getClass().getName() + "] not known");
    }
  }

  @Override
  public DataValueLookupBuilder setNull(String columnName) {
    return set(columnName, null);
  }

  @Override
  public DataValueLookupBuilder setString(String columnName, String value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setInteger(String columnName, Integer value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setBoolean(String columnName, Boolean value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setLong(String columnName, Long value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setDouble(String columnName, Double value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setBigDecimal(String columnName, BigDecimal value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setDate(String columnName, java.sql.Date value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setLocalDate(String columnName, LocalDate value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setByteArray(String columnName, byte[] value) {
    return set(columnName, value);
  }

  @Override
  public DataValueLookupBuilder value(String columnName, String value) {
    return setString(columnName, value);
  }

  @Override
  public DataValueLookupBuilder setObject(String columnName, Object value) {
    return set(columnName, value);
  }

  @Override
  public String getValue(String name) {
    return getString(name);
  }

  @Override
  public Iterable<? extends DataValue> getValues() {

    if (metadata == null)
      return Collections.emptyList();

    return () -> new Iterator<DataValue>() {

      private int i;
      private final Iterator<CaseInsensitiveString> keyIter = metadata.getColumnNames().iterator();

      @Override
      public boolean hasNext() {
        return keyIter.hasNext();
      }

      @Override
      public DataValue next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return new DataValueBean(keyIter.next(), data[i++]);
      }

    };
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
    return Iterables.toString(getValues());
  }

  @Override
  public int hashCode() {
    return DataValueLookup.defaultHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return DataValueLookup.defaultEquals(this, obj);
  }

  /**
   * Sets the value of a specified column. Resizes or allocates the array as required.
   */
  private DataValueLookupBuilder set(String columnName, Object value) {

    CaseInsensitiveString key = CaseInsensitiveString.of(columnName);

    // No data yet - initialise
    if (metadata == null) {
      metadata = DataValueLookupMetadataRegistry.intern(key);
      data = initialiseArray(DEFAULT_INITIAL_SIZE);
      setAtIndex(0, value);
      return this;
    }

    // Overwrite the existing value if it exists
    Integer existingIndex = metadata.getIndexInArray(key);
    if (existingIndex != null) {
      setAtIndex(existingIndex, value);
      return this;
    }

    // Expand the array if required
    int newIndex = metadata.getColumnNames().size();
    if (newIndex == data.length) {
      grow();
    }

    // Update the metadata and store
    metadata = DataValueLookupMetadataRegistry.appendAndIntern(metadata, key);
    setAtIndex(newIndex, value);

    return this;
  }

  /**
   * Writes the given value at the specified index. Assumes that the array
   * has sufficient size.
   */
  private void setAtIndex(int index, Object value) {
    data[index] = value;
  }

  /**
   * Creates the storage array.
   */
  private Object[] initialiseArray(int numberOfColumns) {
    return new Object[numberOfColumns];
  }

  /**
   * Grow the array to accommodate new values. Based on {@link ArrayList}, but
   * without the overflow protection. We've got bigger problems if records get
   * that big.
   */
  private void grow() {
    int size = metadata.getColumnNames().size();
    data = Arrays.copyOf(data, size + (size >> 1));
  }

  /**
   * Fetches the value of the specified column, converting it to the target
   * type using the associated {@link ValueConverter} to the target type.
   */
  private <STORED, RETURNED> RETURNED getAndConvertByName(String columnName, ValueMapper<STORED, RETURNED> mapper) {
    if (metadata == null) return null;
    return getAndConvertByIndex(metadata.getIndexInArray(CaseInsensitiveString.of(columnName)), mapper);
  }


  /**
   * Fetches the value at the specified index, converting it to the target
   * type using the associated {@link ValueConverter} to the target type.
   *
   * @param <STORED> The type actually stored in the internal array.
   * @param <RETURNED> The type being returned by the API call.
   * @param i The index.
   * @param mapper The mapper.
   * @return The value.
   */
  @SuppressWarnings("unchecked")
  private <STORED, RETURNED> RETURNED getAndConvertByIndex(Integer i, ValueMapper<STORED, RETURNED> mapper) {
    if (i == null) return null;
    STORED value = (STORED) data[i];
    return mapper.map(value, (ValueConverter<STORED>) toConverter(value));
  }


  /**
   * Validation method, for testing only.
   *
   * @param other The other.
   * @return true if equivalent metadata.
   */
  @VisibleForTesting
  boolean hasSameMetadata(DataValueLookupBuilderImpl other) {
    return other.metadata.equals(this.metadata);
  }
}
