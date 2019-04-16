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
import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.joda.time.LocalDate;

import com.google.common.collect.Iterables;


/**
 * Defines a set of values for columns.
 *
 * <p>{@code equals} and {@code hashCode} have a strictly defined behaviour which should hold true
 * for all implementations, regardless of implementation class, similarly to the Collections classes
 * such as {@link List}.  These are implemented in {@link #defaultEquals(DataValueLookup, Object)}
 * and {@link #defaultHashCode(DataValueLookup)} and can simply be called by implementations
 * in a single line.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface DataValueLookup {

  /**
   * Returns the value of the column with the given name as a generic formatted
   * string.
   *
   * @param name of the column.
   * @return the value of the named column.
   * @deprecated Use the getter method for the type you require.  Avoid the temptation to use {@link #getString(String)}
   *             for all cases (although that does work as a straight replacement for {@link #getValue(String)}), since
   *             you are potentially doing unnecessary string conversion.  If you need an integer, use
   *             {@link #getInteger(String)}.  If you need a boolean, use {@link #getBoolean(String)}.  This will skip
   *             conversion if possible and thus be more efficient.
   */
  @Deprecated
  public String getValue(String name);


  /**
   * Returns all the key/value pairs stored.
   *
   * @return An iterable of data values.
   */
  public default Iterable<? extends DataValue> getValues() {
    throw new UnsupportedOperationException(
        "Data value lookup type " + getClass().getName() + " currently lacks supported for getValues()");
  }


  /**
   * Gets the value as an integer.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default Integer getInteger(String name) {
    String value = getValue(name);
    return value == null ? null : Integer.valueOf(value);
  }


  /**
   * Gets the value as a long.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default Long getLong(String name) {
    String value = getValue(name);
    return value == null ? null : Long.valueOf(value);
  }


  /**
   * Gets the value as a boolean.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default Boolean getBoolean(String name) {
    String value = getValue(name);
    return value == null ? null : Boolean.valueOf(value);
  }


  /**
   * Gets the value as a Joda {@link LocalDate}.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default org.joda.time.LocalDate getLocalDate(String name) {
    String value = getValue(name);
    return value == null ? null : org.joda.time.LocalDate.parse(value, DataValueLookupHelper.FROM_YYYY_MM_DD);
  }


  /**
   * Gets the value as a {@link java.sql.Date}.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default java.sql.Date getDate(String name) {
    String value = getValue(name);
    return value == null ? null : java.sql.Date.valueOf(value);
  }


  /**
   * Gets the value as a double. Will attempt conversion where possible and
   * throw a suitable conversion exception if the conversion fails. May return
   * {@code null} if the value is not set or is explicitly set to {@code null}.
   * <p>
   * Warning: this returns a floating-point value which cannot represent values
   * precisely. Use for scaling factors or measurements but not use for precise
   * decimal amounts such as monetary amounts. Use
   * {@link DataValueLookup#getBigDecimal(String)} in those cases.
   * </p>
   *
   * @param name The column name.
   * @return The value.
   */
  public default Double getDouble(String name) {
    String value = getValue(name);
    return value == null ? null : Double.valueOf(value);
  }


  /**
   * Gets the value as a {@link BigDecimal}.  Will attempt conversion where possible
   * and throw a suitable conversion exception if the conversion fails.
   * May return {@code null} if the value is not set or is explicitly set
   * to {@code null}.
   *
   * @param name The column name.
   * @return The value.
   */
  public default BigDecimal getBigDecimal(String name) {
    String value = getValue(name);
    return value == null ? null : new BigDecimal(value);
  }


  /**
   * Gets the value as a byte array.  If the original data is a byte array,
   * it is returned verbatim.  Otherwise a conversion is attempted.
   * If converted to or from a string, the value will be converted to or
   * from a base 64-encoded string to preserve the content.  That is:
   *
   * <pre><code>// Equal: Byte arrays are preserved as-is
Arrays.equals(myString.getBytes(), DataSetUtils.record().setByteArray("foo", myString.getBytes()).getByteArray("foo"));

// Not equal: The string gets base64 encoded when read as a byte array
!Arrays.equals(myString.getBytes(), DataSetUtils.record().setString("foo", myString).getByteArray("foo"));

// Equal:
Arrays.equals(Base64.encode(myString), DataSetUtils.record().setString("foo", myString).getByteArray("foo"));</code></pre>
   *
   * @param name The column name.
   * @return The value.
   */
  public default byte[] getByteArray(String name) {
    String value = getValue(name);
    return value == null ? null : Base64.decodeBase64(value);
  }


  /**
   * Gets the value as a string. Intended to allow the record to be easily serialised.
   * Always succeeds, and uses the following conversions from the underlying type:
   *
   * <dl>
   *  <dt>BigDecimal</dt><dd>Uses {@link BigDecimal#toPlainString()}</dd>
   *  <dt>Byte Array</dt><dd>Converted to a MIME Base 64 string</dd>
   *  <dt>Others</dt><dd>Use {@link Object#toString()}</dd>
   * </dl>
   *
   * @param name The column name.
   * @return The value.
   */
  public default String getString(String name) {
    return getValue(name);
  }


  /**
   * Gets the value as either a long, integer, boolean, date, local date, big decimal,
   * byte array or string according to the type definition when called.
   *
   * <p>Just dispatches to the corresponding typed method (e.g. {@link #getBoolean(String)}).
   *
   * <p>Most useful when interacting with {@link ResultSet}. In order to facilitate this
   * use, dates are always returned as a {@link java.sql.Date} rather than a {@link org.joda.time.LocalDate}</p>
   *
   * @param column The column.
   * @return The value.
   */
  public default Object getObject(Column column) {
    switch (column.getType()) {
      case BIG_INTEGER:
        return getLong(column.getName());
      case BOOLEAN:
        return getBoolean(column.getName());
      case INTEGER:
        return getInteger(column.getName());
      case DATE:
        return getDate(column.getName());
      case DECIMAL:
        BigDecimal result = getBigDecimal(column.getName());
        try {
          return result == null ? null : result.setScale(column.getScale());
        } catch (ArithmeticException e) {
          throw new IllegalStateException(String.format(
            "Value of decimal column [%s] has a value of [%s] which must be rounded to fit into (%d,%d). " +
            "To read it with this precision, ensure it is written with rounding pre-applied.",
            column.getName(),
            result.toPlainString(),
            column.getWidth(),
            column.getScale()
          ), e);
        }
      case BLOB:
        return getByteArray(column.getName());
      case CLOB:
      case STRING:
        return getString(column.getName());
      default:
        throw new UnsupportedOperationException("Column [" + column.getName() + "] type [" + column.getType() + "] not known");
    }
  }


  /**
   * Default hashCode implementation for instances.
   *
   * @param obj The object.
   * @return The hashCode.
   */
  public static int defaultHashCode(DataValueLookup obj) {
    final int prime = 31;
    int result = 1;
    for (DataValue value : obj.getValues()) {
      result = prime * result + value.hashCode();
    }
    return result;
  }


  /**
   * Default equals implementation for instances.
   *
   * @param obj1 this
   * @param obj2 the other
   * @return true if equivalent.
   */
  public static boolean defaultEquals(DataValueLookup obj1, Object obj2) {
    if (obj1 == obj2) return true;
    if (obj2 == null) return false;
    if (!(obj2 instanceof DataValueLookup)) return false;
    DataValueLookup other = (DataValueLookup) obj2;
    return Iterables.elementsEqual(obj1.getValues(), other.getValues());
  }
}