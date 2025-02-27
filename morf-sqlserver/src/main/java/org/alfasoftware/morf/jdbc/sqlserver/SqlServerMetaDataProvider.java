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

package org.alfasoftware.morf.jdbc.sqlserver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;

/**
 * Specialisation of {@link DatabaseMetaDataProvider} for SQL Server.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class SqlServerMetaDataProvider extends DatabaseMetaDataProvider {

  /**
   * Query to fetch identity columns.
   */
  private static final String AUTONUM_START_QUERY =
      "select t.name as tableName, i.name as columnName, CAST(i.seed_value AS INTEGER) as seed_value" +
      " from sys.identity_columns i" +
      " inner join sys.tables t on i.object_id = t.object_id" +
      " inner join sys.schemas s on t.schema_id = s.schema_id" +
      " where s.name='%s'";

  /**
   * List of identity columns and their seed values in the database, by column.
   */
  private final Map<String, Map<String, Integer>> identityColumns = new HashMap<>();


  /**
   * Construct a {@link SqlServerMetaDataProvider}.
   *
   * @param connection Database connection for retrieving metadata.
   * @param schemaName name of schema in database containing alfa tables (null = the "dbo" schema)
   */
  public SqlServerMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName == null ? "dbo" : schemaName);
    fetchIdentityColumns();
  }


  /**
   * Fetch the column extended properties
   */
  private void fetchIdentityColumns() {
    try {
      Statement statement = connection.createStatement();
      try {
        ResultSet resultSet = statement.executeQuery(String.format(AUTONUM_START_QUERY, schemaName));
        try {
          while (resultSet.next()) {
            String tableName = resultSet.getString(1);
            String columnName = resultSet.getString(2);
            if (!identityColumns.containsKey(tableName)) {
              identityColumns.put(tableName, new HashMap<String, Integer>());
            }
            identityColumns.get(tableName).put(columnName, resultSet.getInt(3));
          }
        } finally {
          resultSet.close();
        }
      } finally {
        statement.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error fetching identity columns", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(RealName)
   */
  @Override
  protected boolean isPrimaryKeyIndex(RealName indexName) {
    return indexName.getDbName().endsWith("_PK");
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
    if (sqlType == Types.NVARCHAR && width > 1<<30) {
      return DataType.CLOB;
    } else {
      return super.dataTypeFromSqlType(sqlType, typeName, width);
    }
  }


  /**
   * SQL Server does not return information on auto-increment columns in any way which JDBC can pick up,
   * so we need a customised method for fetching this information.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#setAdditionalColumnMetadata(RealName, ColumnBuilder, ResultSet)
   */
  @Override
  protected ColumnBuilder setAdditionalColumnMetadata(RealName tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData)
      throws SQLException {
    if (identityColumns.containsKey(tableName.getDbName())) {
      Map<String, Integer> tableAutoNumStarts = identityColumns.get(tableName.getDbName());
      if (tableAutoNumStarts.containsKey(columnBuilder.getName())) {
        return columnBuilder.autoNumbered(tableAutoNumStarts.get(columnBuilder.getName()));
      }
    }
    return columnBuilder;
  }


  /**
   * @see DatabaseMetaDataProvider#buildSequenceSql(String)
   */
  @Override
  protected String buildSequenceSql(String schemaName) {
    StringBuilder sequenceSqlBuilder = new StringBuilder();
    sequenceSqlBuilder.append("SELECT name FROM sys.all_objects WHERE type='SO'");

    if (schemaName!= null && !schemaName.isBlank()) {
      sequenceSqlBuilder.append(" AND SCHEMA_NAME(schema_id)=?");
    }

    return sequenceSqlBuilder.toString();
  }

}
