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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;

/**
 * Implementation of {@link Record} which provides data from a JDBC
 * {@link ResultSet} object.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class ResultSetRecord implements Record {

  /**
   * The result set from which we provide data.
   */
  private final ResultSet resultSet;

  /**
   * An implementation which determines how to map database-side data types
   * to {@link Record}-style string representations.
   */
  private final DatabaseSafeStringToRecordValueConverter databaseSafeStringToRecordValueConverter;

  /**
   * Store lookups between column names and indexes for optimal access to the result set
   */
  private final Map<String, Integer> columnIndexes = new HashMap<String, Integer>();

  /**
   * Store lookups between column names and types for optimal access to the result set
   */
  private final Map<String, DataType> columnTypes = new HashMap<String, DataType>();

  /**
   * Holds the values of the last record read. This allows the underlying result set to be advanced.
   */
  private final Map<String, String> values = new HashMap<String, String>();

  /**
   * @param table Meta data for the database table.
   * @param resultSet The result set from which to provide record values.
   * @param databaseSafeStringToRecordValueConverter method to convert a value to a String
   */
  public ResultSetRecord(Table table, ResultSet resultSet, DatabaseSafeStringToRecordValueConverter databaseSafeStringToRecordValueConverter) {
    super();
    this.resultSet = resultSet;
    this.databaseSafeStringToRecordValueConverter = databaseSafeStringToRecordValueConverter;

    for (Column column : table.columns()) {
      try {
        columnIndexes.put(column.getName().toUpperCase(), resultSet.findColumn(column.getName()));
        columnTypes.put(column.getName().toUpperCase(), column.getType());
      } catch(SQLException ex) {
        throw new IllegalStateException("Could not retrieve column [" + column.getName() + "] for table [" + table.getName() + "]", ex);
      }
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
   */
  @Override
  public String getValue(String name) {
    return values.get(name.toUpperCase());
  }


  /**
   * Reads the current values out of the result set and caches them.
   */
  public void cacheValues() {
    for (Entry<String, Integer> indexEntry : columnIndexes.entrySet()) {
      String value = databaseSafeStringToRecordValueConverter.databaseSafeStringtoRecordValue(
        columnTypes.get(indexEntry.getKey()),
        resultSet,
        indexEntry.getValue(),
        indexEntry.getKey()
      );
      values.put(indexEntry.getKey(), value);
    }
  }
}
