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

package org.alfasoftware.morf.jdbc.mysql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;

/**
 * Provides meta data from a MySQL database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
class MySqlMetaDataProvider extends DatabaseMetaDataProvider {

  /**
   * @param connection The database connection from which meta data should be provided.
   * @param schemaName The name of the schema in which the data is stored.
   */
  public MySqlMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  /**
   * SqlServerDialect maps CLOB data types to NVARCHAR(MAX) but NVARCHAR sqlTypes are mapped as Strings in the {@link DatabaseMetaDataProvider}.
   * This method uses the column width to determine whether a sqlType == Type.NVARCHAR should be mapped to a String
   * or to a CLOB data type. If the column with is large (> ~ 1G) it will be mapped to a CLOB data type. Otherwise
   * is will be mapped as a String.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#dataTypeFromSqlType(int, java.lang.String, int)
   */
  @Override
  protected DataType dataTypeFromSqlType(int sqlType, String typeName, int width) {
    if (sqlType == Types.LONGVARCHAR && width > 1<<30) {
      return DataType.CLOB;
    } else {
      return super.dataTypeFromSqlType(sqlType, typeName, width);
    }
  }


  /**
   * MySQL can (and must) provide the auto-increment start value from the column remarks.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#setAdditionalColumnMetadata(java.lang.String, org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder, java.sql.ResultSet)
   */
  @Override
  protected ColumnBuilder setAdditionalColumnMetadata(String tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData) throws SQLException {
    columnBuilder = super.setAdditionalColumnMetadata(tableName, columnBuilder, columnMetaData);
    if (columnBuilder.isAutoNumbered()) {
      int startValue = getAutoIncrementStartValue(columnMetaData.getString(COLUMN_REMARKS));
      return columnBuilder.autoNumbered(startValue == -1 ? 1 : startValue);
    } else {
      return columnBuilder;
    }
  }
}
