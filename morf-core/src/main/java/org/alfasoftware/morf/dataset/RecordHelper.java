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

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.ColumnType;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

/**
 * A helper class for {@link Record}s.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class RecordHelper {

  /**
   * Static class not intended to be instantiated
   */
  private RecordHelper() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Date/time formatter for record format dates
   */
  private static final DateTimeFormatter FROM_YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");


  /**
   * Creates a copy of a {@link Record}.  Since {@link Record} does not guarantee immutability, use
   * this for safety when holding onto records.
   *
   * @param record The record to copy.
   * @param columns The columns in the record.
   * @return The copy.
   */
  public static Record copy(Record record, Iterable<Column> columns) {
    RecordBuilder result = DataSetUtils.record();
    for (Column column : columns) {
      result.setObject(column.getName(), record.getObject(column));
    }
    return result;
  }


  /**
   * Joins the values of a record  with a specified delimiter and direct string conversion.
   * Where a null occurs, the <code>valueForNull</code> string will be used in the
   * output.
   *
   * @param columns The columns from the record which should be joined.
   * @param record the record to join
   * @param delimiter The delimiter to use.
   * @param valueForNull The value to use in the output string instead of {@code null}.
   * @return a string representation of the record's values
   */
  public static String joinRecordValues(Iterable<Column> columns, Record record, String delimiter, String valueForNull) {
    return FluentIterable.from(columns)
        .transform(Column::getName)
        .transform(record::getString)
        .transform(v -> StringUtils.isEmpty(v) ? valueForNull : v)
        .join(Joiner.on(delimiter));
  }


  /**
   * Convert a string record value to a type which is safe for comparisons.
   *
   * @param column The column source of the data
   * @param record The record containing the value.
   * @return A type-converted value.
   */
  public static Comparable<?> convertToComparableType(Column column, DataValueLookup record) {
    switch (column.getType()) {
      case STRING:
        // Because of Oracle's empty string handling, relegate all empty strings to null for consistency...
        String string = record.getString(column.getName());
        return StringUtils.isEmpty(string) ? null : string;
      case BLOB:
      case DATE: // TODO can probably just return a LocalDate now, or just use record.getObject, or move this into Record. To consider.
      case CLOB:
        return record.getString(column.getName());
      case BOOLEAN:
        return record.getBoolean(column.getName());
      case DECIMAL:
      case BIG_INTEGER:
      case INTEGER:
        BigDecimal value = record.getBigDecimal(column.getName());
        return value == null ? null : value.setScale(column.getScale(), RoundingMode.HALF_UP); // if the data contains more decimal places than the column allows, we round them off
      default:
        throw new UnsupportedOperationException("Unexpected DataType [" + column.getType() + "] on column [" + column.getName() + "] - Cannot parse value [" + record.getString(column.getName()) + "]");
    }
  }


  /**
   * Convert a string record value to a type which is safe for comparisons.
   *
   * @param column The column source of the data
   * @param stringValue The value
   * @return A type-converted value.
   * @deprecated Use {@link #convertToComparableType(Column, DataValueLookup)}
   */
  @Deprecated
  public static Comparable<?> convertToComparableType(ColumnType column, String stringValue) {

    if (stringValue == null) {
      return null;
    }

    // -- Because of Oracle's empty string handling, relegate all empty strings to null for consistency...
    //
    if (column.getType().equals(DataType.STRING) && stringValue.isEmpty()) {
      return null;
    }

    switch (column.getType()) {
      case STRING:
      case BLOB:
      case DATE:
      case CLOB:
        return stringValue;
      case BOOLEAN:
        return Boolean.valueOf(stringValue);
      case DECIMAL:
      case BIG_INTEGER:
      case INTEGER:
        return new BigDecimal(stringValue).setScale(column.getScale(), RoundingMode.HALF_UP); // if the data contains more decimal places than the column allows, we round them off
      default:
        throw new UnsupportedOperationException("Unexpected DataType [" + column.getType() + "] on column [" + (column instanceof Column ? ((Column) column).getName() : "?") + "] - Cannot parse value [" + stringValue + "]");
    }
  }


  /**
   * Take a java value (int, long, boolean, {@link String}, {@link LocalDate}) and convert it into a string format
   * suitable for inclusion in a {@link Record}.
   *
   * @param value The java value.
   * @return The {@link Record} value string
   */
  public static String javaTypeToRecordValue(Object value) {
    if (value == null) {
      return null;
    } else if (BigDecimal.class.isInstance(value)) {
      return BigDecimal.class.cast(value).toPlainString();
    } else {
      return value.toString();
    }
  }


  /**
   * Take a string value retrieved from a {@link Record} and convert it to a java value of the specified
   * type.
   *
   * @param stringValue The value retrieved from a {@link Record}.
   * @param type The Java class to use for the result.
   * @param <T> The Java type corresponding to the supplied Class
   * @return The typed java value.
   */
  @SuppressWarnings("unchecked")
  public static <T> T recordValueToJavaType(String stringValue, Class<T> type) {
    if (type == Integer.class) {
      return (T)Integer.valueOf(stringValue);
    } else if (type == Long.class) {
      return (T)Long.valueOf(stringValue);
    } else if (type == Boolean.class) {
      return (T)Boolean.valueOf(stringValue);
    } else if (type == LocalDate.class) {
      return (T)LocalDate.parse(stringValue, FROM_YYYY_MM_DD);
    } else if (type == Double.class) {
      return (T)Double.valueOf(stringValue);
    } else {
      return (T)stringValue;
    }
  }
}
