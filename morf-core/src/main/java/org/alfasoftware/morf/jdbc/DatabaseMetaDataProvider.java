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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Provides meta data based on a database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class DatabaseMetaDataProvider implements Schema {

  private static final Log log = LogFactory.getLog(DatabaseMetaDataProvider.class);

  // Column numbers for DatabaseMetaData.getColumns() ResultSet
  protected static final int COLUMN_TABLE_NAME = 3;
  protected static final int COLUMN_NAME = 4;
  protected static final int COLUMN_DATA_TYPE = 5;
  protected static final int COLUMN_TYPE_NAME = 6;
  protected static final int COLUMN_SIZE = 7;
  protected static final int COLUMN_DECIMAL_DIGITS = 9;
  protected static final int COLUMN_REMARKS = 12;
  protected static final int COLUMN_IS_NULLABLE = 18;
  protected static final int COLUMN_IS_AUTOINCREMENT = 23;

  // Column numbers for DatabaseMetaData.getTables() ResultSet
  protected static final int TABLE_SCHEM = 2;
  protected static final int TABLE_NAME = 3;
  protected static final int TABLE_TYPE = 4;
  protected static final int TABLE_REMARKS = 5;

  // Column numbers for DatabaseMetaData.getIndexInfo() ResultSet
  protected static final int INDEX_NON_UNIQUE = 4;
  protected static final int INDEX_NAME = 6;
  protected static final int INDEX_COLUMN_NAME = 9;

  // Column numbers for DatabaseMetaData.getPrimaryKeys() ResultSet
  protected static final int PRIMARY_COLUMN_NAME = 4;
  protected static final int PRIMARY_KEY_SEQ = 5;


  protected final Connection connection;
  protected final String schemaName;


  private final Supplier<Map<String, Map<String, ColumnBuilder>>> allColumns = Suppliers.memoize(this::loadAllColumns);

  private final Supplier<Map<String, String>> tableNames = Suppliers.memoize(this::loadAllTableNames);
  private final LoadingCache<String, Table> tableCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::loadTable));

  private final Supplier<Map<String, String>> viewNames = Suppliers.memoize(this::loadAllViewNames);
  private final LoadingCache<String, View> viewCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::loadView));


  /**
   * @param connection The database connection from which meta data should be provided.
   * @param schemaName The name of the schema in which the data is stored. This might be null.
   */
  protected DatabaseMetaDataProvider(Connection connection, String schemaName) {
    super();
    this.connection = connection;
    this.schemaName = schemaName;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    return tableNames.get().isEmpty();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String tableName) {
    return tableNames.get().containsKey(tableName.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String tableName) {
    return tableCache.getUnchecked(tableName.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    return tableNames.get().values();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return tableNames().stream().map(this::getTable).collect(Collectors.toList());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String viewName) {
    return viewNames.get().containsKey(viewName.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String viewName) {
    return viewCache.getUnchecked(viewName.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    return viewNames.get().values();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return viewNames().stream().map(this::getView).collect(Collectors.toList());
  }


  /**
   * Creates a map of all table names,
   * indexed by their upper-case names.
   */
  protected Map<String, String> loadAllTableNames() {
    final ImmutableMap.Builder<String, String> tableNameMappings = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet tableResultSet = databaseMetaData.getTables(null, schemaName, null, tableTypesForTables())) {
        while (tableResultSet.next()) {
          String tableName = readTableName(tableResultSet);
          try {
            String tableSchemaName = tableResultSet.getString(TABLE_SCHEM);
            String tableType = tableResultSet.getString(TABLE_TYPE);

            boolean systemTable = isSystemTable(tableName);
            boolean ignoredTable = isIgnoredTable(tableName);

            if (log.isDebugEnabled()) {
              log.debug("Found table [" + tableName + "] of type [" + tableType + "] in schema [" + tableSchemaName + "]"
                  + (systemTable ? " - SYSTEM TABLE" : "") + (ignoredTable ? " - IGNORED" : ""));
            }

            if (!systemTable && !ignoredTable) {
              tableNameMappings.put(tableName.toUpperCase(), tableName);
            }
          }
          catch (SQLException e) {
            throw new RuntimeSqlException("Error reading metadata for table ["+tableName+"]", e);
          }
        }

        return tableNameMappings.build();
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }


  /**
   * Types for {@link DatabaseMetaData#getTables(String, String, String, String[])}
   * used by {@link #loadAllTableNames()}.
   */
  protected String[] tableTypesForTables() {
    return new String[] { "TABLE" };
  }


  /**
   * Retrieves table name from a result set.
   */
  protected String readTableName(ResultSet tableResultSet) throws SQLException {
    return tableResultSet.getString(TABLE_NAME);
  }


  /**
   * Identify whether or not the table is one owned by the system, or owned by
   * our application. The default implementation assumes that all tables we can
   * access in the schema are under our control.
   *
   * @param tableName The table which we are accessing.
   * @return <var>true</var> if the table is owned by the system
   */
  protected boolean isSystemTable(@SuppressWarnings("unused") String tableName) {
    return false;
  }


  /**
   * Identify whether or not the specified table should be ignored in the metadata. This is
   * typically used to filter temporary tables.
   *
   * @param tableName The table which we are accessing.
   * @return <var>true</var> if the table should be ignored, false otherwise.
   */
  protected boolean isIgnoredTable(@SuppressWarnings("unused") String tableName) {
    return false;
  }


  /**
   * Creates a map of maps of all table columns,
   * first indexed by their upper-case table names,
   * and then indexed by their upper-case column names.
   */
  protected Map<String, Map<String, ColumnBuilder>> loadAllColumns() {
    final Map<String, ImmutableMap.Builder<String, ColumnBuilder>> columnMappingBuilders = Maps.toMap(tableNames.get().keySet(), k -> ImmutableMap.builder());

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet columnResultSet = databaseMetaData.getColumns(null, schemaName, null, null)) {
        while (columnResultSet.next()) {
          String tableName = columnResultSet.getString(COLUMN_TABLE_NAME);
          if (!columnMappingBuilders.containsKey(tableName.toUpperCase())) {
            continue;
          }

          String columnName = readColumnName(columnResultSet);
          try {
            String typeName = columnResultSet.getString(COLUMN_TYPE_NAME);
            int typeCode = columnResultSet.getInt(COLUMN_DATA_TYPE);
            int width = columnResultSet.getInt(COLUMN_SIZE);
            int scale = columnResultSet.getInt(COLUMN_DECIMAL_DIGITS);
            DataType dataType = dataTypeFromSqlType(typeCode, typeName, width);

            ColumnBuilder column = SchemaUtils.column(columnName, dataType, width, scale);
            column = setColumnNullability(tableName, column, columnResultSet);
            column = setColumnAutonumbered(tableName, column, columnResultSet);
            column = setColumnDefaultValue(tableName, column, columnResultSet);
            column = setAdditionalColumnMetadata(tableName, column, columnResultSet);

            if (log.isDebugEnabled()) {
              log.debug("Found column [" + column + "] on table [" + tableName + "]");
            }

            columnMappingBuilders.get(tableName.toUpperCase()).put(columnName.toUpperCase(), column);
          }
          catch (SQLException e) {
            throw new RuntimeSqlException("Error reading metadata for column ["+columnName+"] on table ["+tableName+"]", e);
          }
        }

        // Maps.transformValues creates a view over the given map of builders
        // Therefore we need to make a copy to avoid building the builders repeatedly
        return ImmutableMap.copyOf(Maps.transformValues(columnMappingBuilders, v -> v.build()));
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }


  /**
   * Retrieves column name from a result set.
   */
  protected String readColumnName(ResultSet columnResultSet) throws SQLException {
    return columnResultSet.getString(COLUMN_NAME);
  }


  /**
   * Converts a given SQL data type to a {@link DataType}.
   */
  protected DataType dataTypeFromSqlType(int typeCode, String typeName, int width) {
    switch (typeCode) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return DataType.INTEGER;
      case Types.BIGINT:
        return DataType.BIG_INTEGER;
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return DataType.DECIMAL;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NVARCHAR:
        return DataType.STRING;
      case Types.BOOLEAN:
      case Types.BIT:
        return DataType.BOOLEAN;
      case Types.DATE:
        return DataType.DATE;
      case Types.BLOB:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return DataType.BLOB;
      case Types.NCLOB:
      case Types.CLOB:
        return DataType.CLOB;
      default:
        throw new IllegalArgumentException("Unknown SQL data type [" + typeName + "] (type " + typeCode + " width " + width + ")");
    }
  }


  /**
   * Sets column nullability from a result set.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnNullability(String tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    boolean nullable = "YES".equals(columnResultSet.getString(COLUMN_IS_NULLABLE));
    return nullable ? column.nullable() : column;
  }


  /**
   * Sets column being autonumbered from a result set.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnAutonumbered(String tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    boolean autoNumbered = "YES".equals(columnResultSet.getString(COLUMN_IS_AUTOINCREMENT));
    return autoNumbered ? column.autoNumbered(-1) : column;
  }


  /**
   * Sets column default value.
   *
   * Note: Uses an empty string for any column other than version.
   * Database-schema level default values are not supported by ALFA's domain model
   * hence we don't want to include a default value in the definition of tables.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnDefaultValue(String tableName, ColumnBuilder column, ResultSet columnResultSet) {
    String defaultValue = "version".equalsIgnoreCase(column.getName()) ? "0" : "";
    return column.defaultValue(defaultValue);
  }


  /**
   * Sets additional column information.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setAdditionalColumnMetadata(String tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    return column;
  }


  /**
   * Loads a table.
   */
  protected Table loadTable(String tableNameKey) {
    final String realTableName = tableNames.get().get(tableNameKey);
    if (realTableName == null) {
      throw new IllegalArgumentException("Table [" + tableNameKey + "] not found.");
    }

    final Map<String, Integer> primaryKey = loadTablePrimaryKey(realTableName);
    final Supplier<List<Column>> columns = Suppliers.memoize(() -> loadTableColumns(realTableName, primaryKey));
    final Supplier<List<Index>> indexes = Suppliers.memoize(() -> loadTableIndexes(realTableName));

    return new Table() {
      @Override
      public String getName() {
        return realTableName;
      }

      @Override
      public List<Column> columns() {
        return columns.get();
      }

      @Override
      public List<Index> indexes() {
        return indexes.get();
      }

      @Override
      public boolean isTemporary() {
        return false;
      }
    };
  }


  /**
   * Loads the primary key column names for the given table name,
   * as a map of upper-case names and respective positions within the key.
   */
  protected Map<String, Integer> loadTablePrimaryKey(String tableName) {
    final ImmutableMap.Builder<String, Integer> columns = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, schemaName, tableName)) {
        while (primaryKeyResultSet.next()) {
          int sequenceNumber = primaryKeyResultSet.getShort(PRIMARY_KEY_SEQ) - 1;
          String columnName = primaryKeyResultSet.getString(PRIMARY_COLUMN_NAME);
          columns.put(columnName.toUpperCase(), sequenceNumber);
        }

        return columns.build();
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException("Error reading primary keys for table [" + tableName + "]", e);
    }
  }


  /**
   * Loads the columns for the given table name.
   */
  protected List<Column> loadTableColumns(String tableName, Map<String, Integer> primaryKey) {
    final Collection<ColumnBuilder> originalColumns = allColumns.get().get(tableName.toUpperCase()).values();
    return createColumnsFrom(originalColumns, primaryKey);
  }


  /**
   * Creates a list of table columns from given columns and map of primary key columns.
   * Also reorders the primary key columns between themselves to reflect the order of columns within the primary key.
   */
  protected static List<Column> createColumnsFrom(Collection<ColumnBuilder> originalColumns, Map<String, Integer> primaryKey) {
    final List<Column> primaryKeyColumns = new ArrayList<>(Collections.nCopies(primaryKey.size(), null));
    final List<Supplier<Column>> results = new ArrayList<>(originalColumns.size());

    // Reorder primary-key columns between themselves according to their ordering within provided reference
    // All non-primary-key columns simply keep their original positions
    Iterator<Integer> numberer = IntStream.rangeClosed(0, primaryKey.size()).iterator();
    for (ColumnBuilder column : originalColumns) {
      String columnName = column.getName().toUpperCase();
      if (primaryKey.containsKey(columnName)) {
        Integer primaryKeyPosition = primaryKey.get(columnName);
        primaryKeyColumns.set(primaryKeyPosition, column.primaryKey());
        results.add(() -> primaryKeyColumns.get(numberer.next()));
      }
      else {
        results.add(Suppliers.ofInstance(column));
      }
    }

    return results.stream().map(Supplier::get).collect(Collectors.toList());
  }


  /**
   * Loads the indexes for the given table name, except for the primary key index.
   */
  protected List<Index> loadTableIndexes(String tableName) {
    final Map<String, ImmutableList.Builder<String>> indexColumns = new HashMap<>();
    final Map<String, Boolean> indexUniqueness = new HashMap<>();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet indexResultSet = databaseMetaData.getIndexInfo(null, schemaName, tableName, false, false)) {
        while (indexResultSet.next()) {
          String indexName = readIndexName(indexResultSet);
          try {
            if (indexName == null) {
              continue;
            }
            if (isPrimaryKeyIndex(indexName)) {
              continue;
            }
            if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(indexName)) {
              continue;
            }

            String columnName = indexResultSet.getString(INDEX_COLUMN_NAME);
            String realColumnName = allColumns.get().get(tableName.toUpperCase()).get(columnName.toUpperCase()).getName();
            boolean unique = !indexResultSet.getBoolean(INDEX_NON_UNIQUE);

            indexUniqueness.put(indexName, unique);

            indexColumns.computeIfAbsent(indexName, k -> ImmutableList.builder())
                .add(realColumnName);
          }
          catch (SQLException e) {
            throw new RuntimeSqlException("Error reading metadata for index ["+indexName+"] on table ["+tableName+"]", e);
          }
        }

        return indexColumns.entrySet().stream()
            .map(e -> createIndexFrom(e.getKey(), indexUniqueness.get(e.getKey()), e.getValue().build()))
            .collect(Collectors.toList());
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException("Error reading metadata for table [" + tableName + "]", e);
    }
  }


  /**
   * Retrieves index name from a result set.
   */
  protected String readIndexName(ResultSet indexResultSet) throws SQLException {
    return indexResultSet.getString(INDEX_NAME);
  }


  /**
   * Identify whether this is the primary key for this table.
   */
  protected boolean isPrimaryKeyIndex(String indexName) {
    return "PRIMARY".equals(indexName);
  }


  /**
   * Creates an index from given info.
   */
  protected static Index createIndexFrom(String indexName, boolean isUnique, List<String> columnNames) {
    IndexBuilder index = SchemaUtils.index(indexName).columns(columnNames);
    return isUnique ? index.unique() : index;
  }


  /**
   * Creates a map of all view names,
   * indexed by their upper-case names.
   */
  protected Map<String, String> loadAllViewNames() {
    final ImmutableMap.Builder<String, String> viewNameMappings = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet viewResultSet = databaseMetaData.getTables(null, schemaName, null, tableTypesForViews())) {
        while (viewResultSet.next()) {
          final String viewName = readViewName(viewResultSet);

          viewNameMappings.put(viewName.toUpperCase(), viewName);
        }

        return viewNameMappings.build();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error reading metadata for views", e);
    }
  }


  /**
   * Types for {@link DatabaseMetaData#getTables(String, String, String, String[])}
   * used by {@link #loadAllViewNames()}.
   */
  protected String[] tableTypesForViews() {
    return new String[] { "VIEW" };
  }


  /**
   * Retrieves view name from a result set.
   */
  protected String readViewName(ResultSet viewResultSet) throws SQLException {
    return viewResultSet.getString(TABLE_NAME);
  }


  /**
   * Loads a view.
   */
  protected View loadView(String viewNameKey) {
    final String realViewName = viewNames.get().get(viewNameKey);
    if (realViewName == null) {
      throw new IllegalArgumentException("View [" + viewNameKey + "] not found.");
    }

    return new View() {
      @Override
      public String getName() {
        return realViewName;
      }

      @Override
      public boolean knowsSelectStatement() {
        return false;
      }

      @Override
      public boolean knowsDependencies() {
        return false;
      }

      @Override
      public SelectStatement getSelectStatement() {
        throw new UnsupportedOperationException("Cannot return SelectStatement as [" + realViewName + "] has been loaded from the database");
      }

      @Override
      public String[] getDependencies() {
        throw new UnsupportedOperationException("Cannot return dependencies as [" + realViewName + "] has been loaded from the database");
      }
    };
  }
}
