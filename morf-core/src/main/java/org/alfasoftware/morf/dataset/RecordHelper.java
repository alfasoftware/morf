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
import java.util.Collection;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.alfasoftware.morf.jdbc.RecordValueToDatabaseSafeStringConverter;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.TableHelper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * A helper class for {@link Record}s.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class RecordHelper {

  /**
   * Date/time formatter for record format dates
   */
  private static final DateTimeFormatter FROM_YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");


  /**
   * Joins the values of a record associated with a table with a specified delimiter. Where
   * a null occurs, the <code>valueForNull</code> string will be used in the output.
   *
   * @param table the table from which the record is sourced
   * @param record the record to join
   * @param columnNames the names of the columns in the order in which they should appear in the output
   * @param delimiter the value to use to delimit columns in the record
   * @param converter an implementation of {@link RecordValueToDatabaseSafeStringConverter} which can convert the {@link Record}
   *                  field values into database-compatible ones.
   * @return a string representation of the record's values
   */
  public static String joinRecordValues(Table table, Record record, Collection<String> columnNames, String delimiter, RecordValueToDatabaseSafeStringConverter converter) {
    StringBuilder dataSetBuilder = new StringBuilder();

    boolean firstIteration = true;

    for (String currentColumnName : columnNames) {
      if (!firstIteration) {
        // Tab delimit the column data
        dataSetBuilder.append(delimiter);
      }

      firstIteration = false;

      // Add the safe version of the value to the output
      dataSetBuilder.append(
        converter.recordValueToDatabaseSafeString(
          TableHelper.columnWithName(table, currentColumnName),
          record.getValue(currentColumnName)
        )
      );
    }

    return dataSetBuilder.toString();
  }


  /**
   * Joins the values of a record associated with a table with a comma delimeter and direct string
   * conversion. Where a null occurs, the <code>valueForNull</code> string will be used in the
   * output.
   *
   * @param table the table from which the record is sourced
   * @param record the record to join
   * @return a string representation of the record's values
   */
  public static String joinRecordValues(Table table, Record record) {
    return joinRecordValues(
      table,
      record,
      Collections2.transform(table.columns(), new Function<Column, String>() {
        @Override public String apply(Column column) {
          return column.getName();
        }
      }),
      ",",
      new RecordValueToDatabaseSafeStringConverter() {
        @Override
        public String recordValueToDatabaseSafeString(Column column, String sourceValue) {
          return sourceValue;
        }
      }
    );
  }


  /**
   * Convert a string record value to a type which is safe for comparisons.
   *
   * @param column The column source of the data
   * @param stringValue The value
   * @return A type-converted value.
   */
  public static Comparable<?> convertToComparableType(Column column, String stringValue) {

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
        throw new UnsupportedOperationException("Unexpected DataType [" + column.getType() + "] on column [" + column.getName() + "] - Cannot parse value [" + stringValue + "]");
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
    } else {
      return value.toString();
    }
  }


  /**
   * Take a string value retrieved from a {@link Record} and convert it to a java value of the specified
   * type.
   *
   * @param stringValue The value retrieved from a {@link Record}.
   * @param type The Java type class to use for the result.
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
