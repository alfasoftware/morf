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

package org.alfasoftware.morf.jdbc;

import java.sql.ResultSet;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataType;


/**
 * Provides a method by which to take a value from a {@link ResultSet} matching the
 * specified {@link DataType}, and convert it to the standard string representation
 * as used on a {@link Record}.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public interface DatabaseSafeStringToRecordValueConverter {

  /**
   * Takes a value from a {@link ResultSet} matching the specified {@link DataType},
   * and converts it to the standard string representation as used on a {@link Record}.
   *
   * @param type The data type of the field
   * @param resultSet The result set from which to fetch the field value
   * @param columnIndex The column index at which the field can be located
   * @param columnName The name of the column, for error reporting.
   * @return the {@link Record}-format string representation of the value.
   */
  public String databaseSafeStringtoRecordValue(DataType type, ResultSet resultSet, int columnIndex, String columnName);
}
