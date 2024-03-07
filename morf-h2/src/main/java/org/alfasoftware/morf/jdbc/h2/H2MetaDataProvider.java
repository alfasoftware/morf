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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Database meta-data layer for H2.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class H2MetaDataProvider extends DatabaseMetaDataProvider {

  private static final Log log = LogFactory.getLog(H2MetaDataProvider.class);

    /**
   * @param connection DataSource to provide meta data for.
   */
  public H2MetaDataProvider(Connection connection) {
    super(connection, null);
  }


  /**
   * H2 reports its primary key indexes as PRIMARY_KEY_49 or similar.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(RealName)
   */
  @Override
  protected boolean isPrimaryKeyIndex(RealName indexName) {
    return indexName.getDbName().startsWith("PRIMARY_KEY");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isIgnoredTable(RealName)
   */
  @Override
  protected boolean isIgnoredTable(RealName tableName) {
    // Ignore temporary tables
    return tableName.getDbName().startsWith(H2Dialect.TEMPORARY_TABLE_PREFIX);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isIgnoredSequence(RealName)
   */
  @Override
  protected boolean isSystemSequence(RealName sequenceName) {
    // Ignore system sequences
    return sequenceName.getDbName().startsWith(H2Dialect.SYSTEM_SEQUENCE_PREFIX);
  }


  /**
   * H2 can (and must) provide the auto-increment start value from the column remarks.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#setAdditionalColumnMetadata(RealName, ColumnBuilder, ResultSet)
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


  /**
   * @see DatabaseMetaDataProvider#buildSequenceSql(String)
   */
  @Override
  protected String buildSequenceSql(String schemaName) {
    StringBuilder sequenceSqlBuilder = new StringBuilder();
    sequenceSqlBuilder.append("SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES");

    if (schemaName != null && !schemaName.isBlank()) {
      sequenceSqlBuilder.append(" WHERE SEQUENCE_SCHEMA =?");
    }

    return sequenceSqlBuilder.toString();
  }
}
