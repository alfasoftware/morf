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

package org.alfasoftware.morf.jdbc.h2;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;

/**
 * Database meta-data layer for H2.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class H2MetaDataProvider extends DatabaseMetaDataProvider {


  private static final List<String> systemTables = Arrays.asList("CONSTANTS",
          "ENUM_VALUES",
          "INDEXES",
          "INDEX_COLUMNS",
          "INFORMATION_SCHEMA_CATALOG_NAME",
          "IN_DOUBT",
          "LOCKS",
          "QUERY_STATISTICS",
          "RIGHTS",
          "ROLES",
          "SESSIONS",
          "SESSION_STATE",
          "SETTINGS",
          "SYNONYMS",
          "USERS");

    /**
   * @param connection DataSource to provide meta data for.
   */
  public H2MetaDataProvider(Connection connection) {
    super(connection, null);
  }


  /**
   * H2 reports its primary key indexes as PRIMARY_KEY_49 or similar.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(java.lang.String)
   */
  @Override
  protected boolean isPrimaryKeyIndex(RealName indexName) {
    return indexName.getDbName().startsWith("PRIMARY_KEY");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isIgnoredTable(java.lang.String)
   */
  @Override
  protected boolean isIgnoredTable(RealName tableName) {
    // Ignore temporary tables
    return tableName.getDbName().startsWith(H2Dialect.TEMPORARY_TABLE_PREFIX) ;
  }

  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isSystemTable(java.lang.String)
   */
  @Override
  protected boolean isSystemTable(@SuppressWarnings("unused") RealName tableName) {
    // Ignore System Tables
    return  systemTables.contains(tableName.getDbName());
  }

  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isSystemView(java.lang.String)
   */
  @Override
  protected boolean isSystemView(@SuppressWarnings("unused") RealName viewName) {
    // Ignore System Views
    return  systemTables.contains(viewName.getDbName());
  }

  /**
   * H2 can (and must) provide the auto-increment start value from the column remarks.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#setAdditionalColumnMetadata(java.lang.String, org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder, java.sql.ResultSet)
   */
  @Override
  protected ColumnBuilder setAdditionalColumnMetadata(RealName tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData) throws SQLException {
    columnBuilder = super.setAdditionalColumnMetadata(tableName, columnBuilder, columnMetaData);
    if (columnBuilder.isAutoNumbered()) {
      int startValue = getAutoIncrementStartValue(columnMetaData.getString(COLUMN_REMARKS));
      return columnBuilder.autoNumbered(startValue == -1 ? 1 : startValue);
    } else {
      return columnBuilder;
    }
  }
}
