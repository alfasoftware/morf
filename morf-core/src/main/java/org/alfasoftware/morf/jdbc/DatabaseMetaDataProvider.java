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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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


  private final Supplier<Map<AName, Map<AName, ColumnBuilder>>> allColumns = Suppliers.memoize(this::loadAllColumns);

  private final Supplier<Map<AName, RealName>> tableNames = Suppliers.memoize(this::loadAllTableNames);
  private final LoadingCache<AName, Table> tableCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::loadTable));

  private final Supplier<Map<AName, RealName>> viewNames = Suppliers.memoize(this::loadAllViewNames);
  private final LoadingCache<AName, View> viewCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::loadView));


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
    return tableNames.get().containsKey(named(tableName));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String tableName) {
    return tableCache.getUnchecked(named(tableName));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    return tableNames.get().values().stream().map(RealName::getRealName).collect(Collectors.toList());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return tableNames.get().values().stream().map(RealName::getRealName).map(this::getTable).collect(Collectors.toList());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String viewName) {
    return viewNames.get().containsKey(named(viewName));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String viewName) {
    return viewCache.getUnchecked(named(viewName));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    return viewNames.get().values().stream().map(RealName::getRealName).collect(Collectors.toList());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return viewNames.get().values().stream().map(RealName::getRealName).map(this::getView).collect(Collectors.toList());
  }


  /**
   * Creates a map of all table names,
   * indexed by their case-agnostic names.
   *
   * @return Map of real table names.
   */
  protected Map<AName, RealName> loadAllTableNames() {
    final ImmutableMap.Builder<AName, RealName> tableNameMappings = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet tableResultSet = databaseMetaData.getTables(null, schemaName, null, tableTypesForTables())) {
        while (tableResultSet.next()) {
          RealName tableName = readTableName(tableResultSet);
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
              tableNameMappings.put(tableName, tableName);
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
   *
   * @return Array of relevant JDBC types.
   */
  protected String[] tableTypesForTables() {
    return new String[] { "TABLE" };
  }


  /**
   * Retrieves table name from a result set.
   *
   * @param tableResultSet Result set to be read.
   * @return Name of the table.
   * @throws SQLException Upon errors.
   */
  protected RealName readTableName(ResultSet tableResultSet) throws SQLException {
    String tableName = tableResultSet.getString(TABLE_NAME);
    return createRealName(tableName, tableName);
  }


  /**
   * Identify whether or not the table is one owned by the system, or owned by
   * our application. The default implementation assumes that all tables we can
   * access in the schema are under our control.
   *
   * @param tableName The table which we are accessing.
   * @return <var>true</var> if the table is owned by the system
   */
  protected boolean isSystemTable(@SuppressWarnings("unused") RealName tableName) {
    return false;
  }


  /**
   * Identify whether or not the specified table should be ignored in the metadata. This is
   * typically used to filter temporary tables.
   *
   * @param tableName The table which we are accessing.
   * @return <var>true</var> if the table should be ignored, false otherwise.
   */
  protected boolean isIgnoredTable(@SuppressWarnings("unused") RealName tableName) {
    return false;
  }


  /**
   * Creates a map of maps of all table columns,
   * first indexed by their case-agnostic table names,
   * and then indexed by their case-agnostic column names.
   *
   * @return Map of table columns by table names and column names.
   */
  protected Map<AName, Map<AName, ColumnBuilder>> loadAllColumns() {
    final Map<AName, ImmutableMap.Builder<AName, ColumnBuilder>> columnMappingBuilders = Maps.toMap(tableNames.get().keySet(), k -> ImmutableMap.builder());

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet columnResultSet = databaseMetaData.getColumns(null, schemaName, null, null)) {
        while (columnResultSet.next()) {
          String tableName = columnResultSet.getString(COLUMN_TABLE_NAME);
          RealName realTableName = tableNames.get().get(named(tableName));
          if (realTableName == null) {
            continue; // ignore columns of unknown tables
          }

          RealName columnName = readColumnName(columnResultSet);
          try {
            String typeName = columnResultSet.getString(COLUMN_TYPE_NAME);
            int typeCode = columnResultSet.getInt(COLUMN_DATA_TYPE);
            int width = columnResultSet.getInt(COLUMN_SIZE);
            int scale = columnResultSet.getInt(COLUMN_DECIMAL_DIGITS);

            try {
              DataType dataType = dataTypeFromSqlType(typeCode, typeName, width);

              ColumnBuilder column = SchemaUtils.column(columnName.getRealName(), dataType, width, scale);
              column = setColumnNullability(realTableName, column, columnResultSet);
              column = setColumnAutonumbered(realTableName, column, columnResultSet);
              column = setColumnDefaultValue(realTableName, column, columnResultSet);
              column = setAdditionalColumnMetadata(realTableName, column, columnResultSet);

              if (log.isDebugEnabled()) {
                log.debug("Found column [" + column + "] on table [" + tableName + "]: " + column);
              }

              columnMappingBuilders.get(realTableName).put(columnName, column);
            }
            catch (UnexpectedDataTypeException e) {
              ColumnBuilder column = new UnsupportedDataTypeColumn(columnName, typeName, typeCode, width, scale, columnResultSet);

              if (log.isDebugEnabled()) {
                log.debug("Found unsupported column [" + column + "] on table [" + tableName + "]: " + column);
              }

              columnMappingBuilders.get(realTableName).put(columnName, column);
            }
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
   *
   * @param columnResultSet Result set to be read.
   * @return Name of the column.
   * @throws SQLException Upon errors.
   */
  protected RealName readColumnName(ResultSet columnResultSet) throws SQLException {
    String columnName = columnResultSet.getString(COLUMN_NAME);
    return createRealName(columnName, columnName);
  }


  /**
   * Converts a given SQL data type to a {@link DataType}.
   *
   * @param typeCode JDBC data type.
   * @param typeName JDBC type name.
   * @param width JDBC column size.
   * @return Morf data type.
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
        throw new UnexpectedDataTypeException("Unsupported data type [" + typeName + "] (type " + typeCode + " width " + width + ")");
    }
  }


  /**
   * Sets column nullability from a result set.
   *
   * @param tableName Name of the table.
   * @param column Column builder to set to.
   * @param columnResultSet Result set to be read.
   * @return Resulting column builder.
   * @throws SQLException Upon errors.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnNullability(RealName tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    boolean nullable = "YES".equals(columnResultSet.getString(COLUMN_IS_NULLABLE));
    return nullable ? column.nullable() : column;
  }


  /**
   * Sets column being autonumbered from a result set.
   *
   * @param tableName Name of the table.
   * @param column Column builder to set to.
   * @param columnResultSet Result set to be read.
   * @return Resulting column builder.
   * @throws SQLException Upon errors.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnAutonumbered(RealName tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    boolean autoNumbered = "YES".equals(columnResultSet.getString(COLUMN_IS_AUTOINCREMENT));
    return autoNumbered ? column.autoNumbered(-1) : column;
  }


  /**
   * Sets column default value.
   *
   * Note: Uses an empty string for any column other than version.
   * Database-schema level default values are not supported by ALFA's domain model
   * hence we don't want to include a default value in the definition of tables.
   *
   * @param tableName Name of the table.
   * @param column Column builder to set to.
   * @param columnResultSet Result set to be read.
   * @return Resulting column builder.
   * @throws SQLException Upon errors.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setColumnDefaultValue(RealName tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    String defaultValue = "version".equalsIgnoreCase(column.getName()) ? "0" : "";
    return column.defaultValue(defaultValue);
  }


  /**
   * Sets additional column information.
   *
   * @param tableName Name of the table.
   * @param column Column builder to set to.
   * @param columnResultSet Result set to be read.
   * @return Resulting column builder.
   * @throws SQLException Upon errors.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setAdditionalColumnMetadata(RealName tableName, ColumnBuilder column, ResultSet columnResultSet) throws SQLException {
    return column;
  }


  /**
   * Loads a table.
   *
   * @param tableName Name of the table.
   * @return The table metadata.
   */
  protected Table loadTable(AName tableName) {
    final RealName realTableName = tableNames.get().get(tableName);

    if (realTableName == null) {
      throw new IllegalArgumentException("Table [" + tableName + "] not found.");
    }

    final Map<AName, Integer> primaryKey = loadTablePrimaryKey(realTableName);
    final Supplier<List<Column>> columns = Suppliers.memoize(() -> loadTableColumns(realTableName, primaryKey));
    final Supplier<List<Index>> indexes = Suppliers.memoize(() -> loadTableIndexes(realTableName));

    return new Table() {
      @Override
      public String getName() {
        return realTableName.getRealName();
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
   * as a map of case-agnostic names and respective positions within the key.
   *
   * @param tableName Name of the table.
   * @return Map of respective positions by column names.
   */
  protected Map<AName, Integer> loadTablePrimaryKey(RealName tableName) {
    final ImmutableMap.Builder<AName, Integer> columns = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet primaryKeyResultSet = databaseMetaData.getPrimaryKeys(null, schemaName, tableName.getDbName())) {
        while (primaryKeyResultSet.next()) {
          int sequenceNumber = primaryKeyResultSet.getShort(PRIMARY_KEY_SEQ) - 1;
          String columnName = primaryKeyResultSet.getString(PRIMARY_COLUMN_NAME);
          columns.put(named(columnName), sequenceNumber);
        }

        if (log.isDebugEnabled()) {
          log.debug("Found primary key [" + columns.build() + "] on table [" + tableName + "]");
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
   *
   * @param tableName Name of the table.
   * @param primaryKey Map of respective positions by column names.
   * @return List of table columns.
   */
  protected List<Column> loadTableColumns(RealName tableName, Map<AName, Integer> primaryKey) {
    final Collection<ColumnBuilder> originalColumns = allColumns.get().get(tableName).values();
    return createColumnsFrom(originalColumns, primaryKey);
  }


  /**
   * Creates a list of table columns from given columns and map of primary key columns.
   * Also reorders the primary key columns between themselves to reflect the order of columns within the primary key.
   *
   * @param originalColumns Collection of table columns to work with.
   * @param primaryKey Map of respective positions by column names.
   * @return List of table columns.
   */
  protected static List<Column> createColumnsFrom(Collection<ColumnBuilder> originalColumns, Map<AName, Integer> primaryKey) {
    final List<Column> primaryKeyColumns = new ArrayList<>(Collections.nCopies(primaryKey.size(), null));
    final List<Supplier<Column>> results = new ArrayList<>(originalColumns.size());

    // Reorder primary-key columns between themselves according to their ordering within provided reference
    // All non-primary-key columns simply keep their original positions
    Iterator<Integer> numberer = IntStream.rangeClosed(0, primaryKey.size()).iterator();
    for (ColumnBuilder column : originalColumns) {
      if (primaryKey.containsKey(named(column.getName()))) {
        Integer primaryKeyPosition = primaryKey.get(named(column.getName()));
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
   *
   * @param tableName Name of the table.
   * @return List of table indexes.
   */
  protected List<Index> loadTableIndexes(RealName tableName) {
    final Map<RealName, ImmutableList.Builder<RealName>> indexColumns = new HashMap<>();
    final Map<RealName, Boolean> indexUniqueness = new HashMap<>();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet indexResultSet = databaseMetaData.getIndexInfo(null, schemaName, tableName.getDbName(), false, false)) {
        while (indexResultSet.next()) {
          RealName indexName = readIndexName(indexResultSet);
          try {
            if (indexName == null) {
              continue;
            }
            if (isPrimaryKeyIndex(indexName)) {
              continue;
            }
            if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(indexName.getDbName())) {
              continue;
            }

            String dbColumnName = indexResultSet.getString(INDEX_COLUMN_NAME);
            String realColumnName = allColumns.get().get(tableName).get(named(dbColumnName)).getName();
            RealName columnName = createRealName(dbColumnName, realColumnName);
            boolean unique = !indexResultSet.getBoolean(INDEX_NON_UNIQUE);

            if (log.isDebugEnabled()) {
              log.debug("Found index column [" + columnName + "] for index [" + indexName  + ", unique: " + unique + "] on table [" + tableName + "]");
            }

            indexUniqueness.put(indexName, unique);

            indexColumns.computeIfAbsent(indexName, k -> ImmutableList.builder())
                .add(columnName);
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
   *
   * @param indexResultSet Result set to be read.
   * @return Name of the index.
   * @throws SQLException Upon errors.
   */
  protected RealName readIndexName(ResultSet indexResultSet) throws SQLException {
    String indexName = indexResultSet.getString(INDEX_NAME);
    return createRealName(indexName, indexName);
  }


  /**
   * Identify whether this is the primary key for this table.
   *
   * @param indexName Name of the index.
   * @return true for primary key, false otherwise.
   */
  protected boolean isPrimaryKeyIndex(RealName indexName) {
    return "PRIMARY".equals(indexName.getDbName());
  }


  /**
   * Creates an index from given info.
   *
   * @param indexName The name of the index.
   * @param isUnique Whether to mark this index as unique.
   * @param columnNames The column names for the index.
   * @return An {@link IndexBuilder} for the index.
   */
  protected static Index createIndexFrom(RealName indexName, boolean isUnique, List<RealName> columnNames) {
    List<String> realColumnNames = columnNames.stream().map(RealName::getRealName).collect(Collectors.toList());
    IndexBuilder index = SchemaUtils.index(indexName.getRealName()).columns(realColumnNames);
    return isUnique ? index.unique() : index;
  }


  /**
   * Creates a map of all view names,
   * indexed by their case-agnostic names.
   *
   * @return Map of real view names.
   */
  protected Map<AName, RealName> loadAllViewNames() {
    final ImmutableMap.Builder<AName, RealName> viewNameMappings = ImmutableMap.builder();

    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      try (ResultSet viewResultSet = databaseMetaData.getTables(null, schemaName, null, tableTypesForViews())) {
        while (viewResultSet.next()) {
          RealName viewName = readViewName(viewResultSet);

          if (log.isDebugEnabled()) {
            log.debug("Found view [" + viewName + "]");
          }

          viewNameMappings.put(viewName, viewName);
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
   *
   * @return Array of relevant JDBC types.
   */
  protected String[] tableTypesForViews() {
    return new String[] { "VIEW" };
  }


  /**
   * Retrieves view name from a result set.
   *
   * @param viewResultSet Result set to be read.
   * @return Name of the view.
   * @throws SQLException Upon errors.
   */
  protected RealName readViewName(ResultSet viewResultSet) throws SQLException {
    String viewName = viewResultSet.getString(TABLE_NAME);
    return createRealName(viewName, viewName);
  }


  /**
   * Loads a view.
   *
   * @param viewName Name of the view.
   * @return The view metadata.
   */
  protected View loadView(AName viewName) {
    final RealName realViewName = viewNames.get().get(viewName);

    if (realViewName == null) {
      throw new IllegalArgumentException("View [" + viewName + "] not found.");
    }

    return new View() {
      @Override
      public String getName() {
        return realViewName.getRealName();
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
        throw new UnsupportedOperationException("Cannot return SelectStatement as [" + realViewName.getRealName() + "] has been loaded from the database");
      }

      @Override
      public String[] getDependencies() {
        throw new UnsupportedOperationException("Cannot return dependencies as [" + realViewName.getRealName() + "] has been loaded from the database");
      }
    };
  }


  /**
   * Creates {@link AName} for searching the maps within this metadata provider.
   *
   * <p>
   * Metadata providers need to use case insensitive keys for lookup maps, since
   * database object name are considered case insensitive. While the same could
   * be achieved by simply upper-casing all database object names, such approach
   * can lead to mistakes.
   *
   * <p>
   * On top of that, using {@link AName} instead of upper-cased strings has the
   * advantage of strongly typed map keys, as opposed to maps of strings.
   *
   * @param name Case insensitive name of the object.
   * @return {@link AName} instance suitable for use as a key in the lookup maps.
   */
  protected static AName named(String name) {
    return new AName(name);
  }


  /**
   * Creates {@link RealName}, which contractually remembers two versions of a
   * database object name: the name as retrieved by the JDBC driver, and also
   * the user-friendly camel-case name of that same object, often derived by
   * looking at the comment of that object, or in schema descriptions.
   *
   * <p>
   * Note: Any {@link RealName} is also {@link AName}, and thus can be used as a
   * key in the lookup maps for convenience, just like any other {@link AName}.
   *
   * <p>
   * However,
   * the distinction beetween {@link RealName} and {@link AName} is important.
   * Strongly typed {@link RealName} is used in places where the two versions
   * of a database object name are known, as opposed to {@link AName} being used
   * in places where case insensitive map lookup keys are good enough. Method
   * signatures for example use {@link RealName} if they need a specific version
   * of the database object name, and use {@link AName} if they do not really
   * care (or cannot be expected to care) about the true letter case.
   *
   * <p>
   * Never create an instance of {@link RealName} without knowing true values of
   * the two versions of a database object name.
   *
   * @param dbName the name as retrieved by the JDBC driver
   * @param realName the user-friendly camel-case name of that same object,
   *                 often derived by looking at the comment of that object,
   *                 or in schema descriptions.
   * @return {@link RealName} instance holding the two name versions.
   *         Can also be used as a key in the lookup maps, like {@link AName}.
   */
  protected static RealName createRealName(String dbName, String realName) {
    return new RealName(dbName, realName);
  }


  /**
   * Case insensitive name of a database object.
   * Used as keys in the maps within this metadata provider.
   * Also used for referencing database objects without worrying about letter case.
   *
   * <p>For more info,
   * see {@link DatabaseMetaDataProvider#named(String)}
   * and {@link DatabaseMetaDataProvider#createRealName(String, String)}
   */
  protected static class AName {
    private final String aName;
    private final int hashCode;

    protected AName(String aName) {
      this.aName = aName;
      this.hashCode = aName.toLowerCase().hashCode();
    }

    protected String getAName() {
      return aName;
    }

    @Override
    public String toString() {
      return aName + "/*";
    }

    @Override
    public final int hashCode() { // final intentional!
      return hashCode;
    }

    @Override
    public final boolean equals(Object obj) { // final intentional!
      if (this == obj) return true;
      if (obj == null) return false;
      if (!(obj instanceof AName)) return false; // instanceof intentional!
      AName that = (AName) obj;
      return this.aName.equalsIgnoreCase(that.aName);
    }
  }


  /**
   * Two case sensitive names of a database object: the name as retrieved by the JDBC driver,
   * and also the user-friendly camel-case name of that same object, often derived by looking
   * at the comment of that object, or in schema descriptions.
   *
   * <p>Can also be used as {@link AName}, for convenience.
   *
   * <p>For more info,
   * see {@link DatabaseMetaDataProvider#named(String)}
   * and {@link DatabaseMetaDataProvider#createRealName(String, String)}
   */
  protected static final class RealName extends AName {

    private final String realName;

    private RealName(String dbName, String realName) {
      super(dbName);
      this.realName = realName;
    }

    public String getRealName() {
      return realName;
    }

    public String getDbName() {
      return getAName();
    }

    @Override
    public String toString() {
      return getDbName() + "/" + getRealName();
    }
  }


  @Override
  public String toString() {
    return "Schema[" + tables().size() + " tables, " + views().size() + " views]";
  }


  /**
   * Exception for unsupported data types.
   */
  public static final class UnexpectedDataTypeException extends RuntimeException {

    public UnexpectedDataTypeException(String string) {
      super(string);
    }
  }

  /**
   * This implementation of {@link Column} describing an unsupportable column.
   * Reading most of this column's data will result in exceptions being thrown.
   */
  protected static final class UnsupportedDataTypeColumn implements ColumnBuilder {

    private final RealName columnName;
    private final String typeName;
    private final int typeCode;
    private final int width;
    private final int scale;
    private final Map<String, Object> columnResultSet;

    UnsupportedDataTypeColumn(RealName columnName, String typeName, int typeCode, int width, int scale, ResultSet columnResultSet) throws SQLException {
      this.columnName = columnName;
      this.typeName = typeName;
      this.typeCode = typeCode;
      this.width = width;
      this.scale = scale;

      Map<String, Object> values = new LinkedHashMap<>();
      ResultSetMetaData metaData = columnResultSet.getMetaData();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        String label = metaData.getColumnLabel(i);
        Object value = columnResultSet.getObject(1);
        values.put(i+"-"+label, value);
      }
      this.columnResultSet = values;
    }

    @Override
    public String getName() {
      return columnName.getRealName();
    }


    @Override
    public DataType getType() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public int getWidth() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public int getScale() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public boolean isNullable() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public boolean isPrimaryKey() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public boolean isAutoNumbered() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public int getAutoNumberStart() {
      throw new UnexpectedDataTypeException(this.toString());
    }

    @Override
    public String getDefaultValue() {
      throw new UnexpectedDataTypeException(this.toString());
    }


    @Override
    public String toString() {
      return new StringBuilder()
          .append("Column-").append(columnName)
          .append("-").append("UNSUPPORTED")
          .append("-").append(typeName)
          .append("-").append(typeCode)
          .append("-").append(width)
          .append("-").append(scale)
          .append("-").append(columnResultSet)
          .toString();
    }

    @Override
    public ColumnBuilder nullable() {
      return this;
    }

    @Override
    public ColumnBuilder defaultValue(String value) {
      return this;
    }

    @Override
    public ColumnBuilder primaryKey() {
      return this;
    }

    @Override
    public ColumnBuilder notPrimaryKey() {
      return this;
    }

    @Override
    public ColumnBuilder autoNumbered(int from) {
      return this;
    }

    @Override
    public ColumnBuilder dataType(DataType dataType) {
      return this;
    }
  }
}
