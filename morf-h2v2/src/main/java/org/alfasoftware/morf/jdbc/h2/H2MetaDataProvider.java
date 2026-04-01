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
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.parseDeferredIndexesFromComment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.alfasoftware.morf.metadata.Table;

/**
 * Database meta-data layer for H2.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class H2MetaDataProvider extends DatabaseMetaDataProvider {

  /** Stores raw table comments keyed by uppercase table name, for deferred index parsing. */
  private final Map<String, String> tableComments = new HashMap<>();


  /**
   * @param connection DataSource to provide meta data for.
   */
  public H2MetaDataProvider(Connection connection) {
    super(connection, "PUBLIC");
  }

  /**
   * @param connection DataSource to provide meta data for.
   * @param schemaName The schema to connect to.
   */
  public H2MetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  @Override
  protected RealName readTableName(ResultSet tableResultSet) throws SQLException {
    String tableName = tableResultSet.getString(TABLE_NAME);
    String comment = tableResultSet.getString(TABLE_REMARKS);
    if (comment != null && !comment.isEmpty()) {
      tableComments.put(tableName.toUpperCase(), comment);
    }
    return super.readTableName(tableResultSet);
  }


  @Override
  protected Table loadTable(AName tableName) {
    Table base = super.loadTable(tableName);
    String comment = tableComments.get(base.getName().toUpperCase());
    List<Index> deferredFromComment = parseDeferredIndexesFromComment(comment);
    if (deferredFromComment.isEmpty()) {
      return base;
    }
    Set<String> physicalNames = base.indexes().stream()
        .map(i -> i.getName().toUpperCase())
        .collect(Collectors.toSet());
    List<Index> merged = new ArrayList<>(base.indexes());
    for (Index deferred : deferredFromComment) {
      if (!physicalNames.contains(deferred.getName().toUpperCase())) {
        merged.add(deferred);
      }
    }
    List<Index> finalIndexes = merged;
    return new Table() {
      @Override public String getName() { return base.getName(); }
      @Override public List<Column> columns() { return base.columns(); }
      @Override public List<Index> indexes() { return finalIndexes; }
      @Override public boolean isTemporary() { return base.isTemporary(); }
    };
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


  private static final Set<String> SYSTEM_TABLES = Set.of(
          "CHECK_CONSTRAINTS",
          "COLLATIONS",
          "COLUMNS",
          "COLUMN_PRIVILEGES",
          "CONSTANTS",
          "CONSTRAINT_COLUMN_USAGE",
          "DOMAINS",
          "DOMAIN_CONSTRAINTS",
          "ELEMENT_TYPES",
          "ENUM_VALUES",
          "FIELDS",
          "INDEXES",
          "INDEX_COLUMNS",
          "INFORMATION_SCHEMA_CATALOG_NAME",
          "IN_DOUBT",
          "KEY_COLUMN_USAGE",
          "LOCKS",
          "PARAMETERS",
          "QUERY_STATISTICS",
          "REFERENTIAL_CONSTRAINTS",
          "RIGHTS",
          "ROLES",
          "ROUTINES",
          "SCHEMATA",
          "SEQUENCES",
          "SESSIONS",
          "SESSION_STATE",
          "SETTINGS",
          "SYNONYMS",
          "TABLES",
          "TABLE_CONSTRAINTS",
          "TABLE_PRIVILEGES",
          "TRIGGERS",
          "USERS",
          "VIEWS"
  );


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isSystemTable(RealName)
   */
  @Override
  protected boolean isSystemTable(RealName tableName) {
    // Ignore system tables
    return SYSTEM_TABLES.contains(tableName.getDbName());
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
