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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Maps.uniqueIndex;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

/**
 * Provides meta data based on a database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class DatabaseMetaDataProvider implements Schema {

  // Column numbers for indexing the ResultSet returned by DatabaseMetaData.getColumns()
  // See http://docs.oracle.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getColumns%28java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String%29
  private static final int IS_AUTOINCREMENT = 23;
  protected static final int REMARKS = 12;
  private static final int IS_NULLABLE = 18;
  private static final int DECIMAL_DIGITS = 9;
  private static final int COLUMN_SIZE = 7;
  private static final int DATA_TYPE = 5;
  private static final int TYPE_NAME = 6;
  private static final int COLUMN_NAME = 4;


  /**
   * Log provider
   */
  private static final Log      log         = LogFactory.getLog(DatabaseMetaDataProvider.class);

  /**
   * Type names used to query database meta data.
   */
  private static final String[] TABLE_TYPES = new String[] { "TABLE" };

  /** Cache table names so we can remember what case the database is using. */
  private Map<String, String>   tableNameMappings;

  /**
   * View cache
   */
  private Map<String, View> viewMap;

  /**
   * The connection from which meta data is provided.
   */
  protected final Connection    connection;

  /**
   * The database schema name, used in meta data queries.
   */
  protected final String        schemaName;


  /**
   * @param connection The database connection from which meta data should be
   *          provided.
   * @param schemaName The name of the schema in which the data is stored. This
   *          might be null.
   */
  protected DatabaseMetaDataProvider(Connection connection, String schemaName) {
    super();
    this.connection = connection;
    this.schemaName = schemaName;
  }


  /**
   * @return a mapping of uppercase table names to database table names.
   */
  private Map<String, String> tableNameMappings() {
    if (tableNameMappings == null) {
      // Create a TreeMap instead of a HashMap so that the contents are sorted
      // alphabetically rather than being in a random order
      tableNameMappings = new TreeMap<String, String>();
      readTableNames();
    }
    return tableNameMappings;
  }


  /**
   * Use to access the metadata for the views in the specified connection.
   * Lazily initialises the metadata, and only loads it once.
   *
   * @return View metadata.
   */
  protected final Map<String, View> viewMap() {
    if (viewMap != null) {
      return viewMap;
    }

    viewMap = new TreeMap<String, View>(String.CASE_INSENSITIVE_ORDER);
    populateViewMap(viewMap);
    return viewMap;
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    return tableNameMappings().isEmpty();
  }


  /**
   * Return a {@link Table} implementation. Note the metadata is read lazily.
   *
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String name) {
    final String adjustedTableName = tableNameMappings().get(name.toUpperCase());

    final List<String> primaryKeys = getPrimaryKeys(adjustedTableName);

    if (adjustedTableName == null) {
      throw new IllegalArgumentException("Table [" + name + "] not found.");
    }

    return new Table() {

      private List<Column> columns;
      private List<Index>  indexes;


      /**
       * @see org.alfasoftware.morf.metadata.Table#getName()
       */
      @Override
      public String getName() {
        return adjustedTableName;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Table#columns()
       */
      @Override
      public List<Column> columns() {
        if (columns == null) {
          columns = readColumns(adjustedTableName, primaryKeys);
        }
        return columns;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Table#indexes()
       */
      @Override
      public List<Index> indexes() {
        if (indexes == null) {
          indexes = readIndexes(adjustedTableName);
        }
        return indexes;
      }


      @Override
      public boolean isTemporary() {
        return false;
      }

    };
  }


  /**
   * Finds the primary keys for the {@code tableName}.
   *
   * @param tableName the table to query for.
   * @return a collection of the primary keys.
   */
  private List<String> getPrimaryKeys(String tableName) {
    List<PrimaryKeyColumn> columns = newArrayList();
    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      ResultSet primaryKeyResults = databaseMetaData.getPrimaryKeys(null, schemaName, tableName);

      try {
        while (primaryKeyResults.next()) {
          columns.add(new PrimaryKeyColumn(primaryKeyResults.getShort(5), primaryKeyResults.getString(4)));
        }
      } finally {
        primaryKeyResults.close();
      }
    } catch (SQLException sqle) {
      throw new RuntimeSqlException("Error reading primary keys for table [" + tableName + "]", sqle);
    }

    List<PrimaryKeyColumn> sortedColumns = Ordering.from(new Comparator<PrimaryKeyColumn>() {
      @Override
      public int compare(PrimaryKeyColumn o1, PrimaryKeyColumn o2) {
        return o1.sequence.compareTo(o2.sequence);
      }
    }).sortedCopy(columns);

    // Convert to String before returning
    return transform(sortedColumns, new Function<PrimaryKeyColumn, String>() {
      @Override
      public String apply(PrimaryKeyColumn input) {
        return input.name;
      }
    });
  }


  /**
   * Read out the columns for a particular table.
   *
   * @param tableName The source table
   * @param primaryKeys the primary keys for the table.
   * @return a list of columns
   */
  protected List<Column> readColumns(String tableName, List<String> primaryKeys) {
    try {
      try {
        final DatabaseMetaData databaseMetaData = connection.getMetaData();

        ResultSet columnResults = databaseMetaData.getColumns(null, schemaName, tableName, null);
        try {

          List<Column> rawColumns = new LinkedList<Column>();

          while (columnResults.next()) {
            String columnName = columnResults.getString(COLUMN_NAME);

            try {
              String typeName = columnResults.getString(TYPE_NAME);
              int typeCode = columnResults.getInt(DATA_TYPE);
              int width = columnResults.getInt(COLUMN_SIZE);
              DataType dataType = dataTypeFromSqlType(typeCode, typeName, width);
              int scale = columnResults.getInt(DECIMAL_DIGITS);
              boolean nullable = columnResults.getString(IS_NULLABLE).equals("YES");
              String defaultValue = determineDefaultValue(columnName);

              ColumnBuilder column = column(columnName, dataType, width, scale).defaultValue(defaultValue);
              column = nullable ? column.nullable() : column;
              column = primaryKeys.contains(columnName) ? column.primaryKey() : column;
              column = setAdditionalColumnMetadata(tableName, column, columnResults);

              rawColumns.add(column);

            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Error reading metadata for column [%s] on table [%s]", columnName, tableName), e);
            }
          }

          return sortByPrimaryKey(primaryKeys, rawColumns);
        } finally {
          columnResults.close();
        }
      } catch (SQLException sqle) {
        throw new RuntimeSqlException(sqle);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Error reading metadata for table [" + tableName + "]", ex);
    }
  }


  /**
   * Sorts a list of columns so that the primary key columns appear first in key order.
   * @param primaryKeys The list of the primary key column names
   * @param rawColumns The list of columns to re-arrange
   * @return A list containing all the {@link Column} instances in the supplied list re-arranged so that the primary
   * key columns appear first.
   */
  protected List<Column> sortByPrimaryKey(List<String> primaryKeys, List<Column> rawColumns) {
    // Map allowing retrieval of columns by name
    ImmutableMap<String, Column> columnsMap = uniqueIndex(rawColumns, new Function<Column, String>() {
      @Override
      public String apply(Column col) {
        return col.getName();
      }
    });

    List<Column> results = newArrayList();
    List<Column> normalColumns = newArrayList(rawColumns);

    // Add the primary key columns to the results first
    for(String pk : primaryKeys) {
      Column pkCol = columnsMap.get(pk);
      if (pkCol == null) {
        throw new IllegalStateException("Could not find primary key column [" + pk + "] in columns [" + rawColumns + "]");
      }

      normalColumns.remove(pkCol);
      results.add(pkCol);
    }

    // Then add the remaining columns
    results.addAll(normalColumns);
    return results;
  }


  /**
   * Implements the default method for obtaining the information as to whether a column is autonumbered,
   * and if so, from what start value, from the database.  Optionally overridden in specific RDBMS implementations
   * where the information is available from different sources.
   *
   * @param tableName The name of the table.
   * @param columnBuilder The column under construction.
   * @param columnMetaData The JDBC column metdata, if required.
   * @return The modified column
   * @throws SQLException when a problem in retrieving information from the database is encountered.
   */
  @SuppressWarnings("unused")
  protected ColumnBuilder setAdditionalColumnMetadata(String tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData) throws SQLException {
    return "YES".equals(columnMetaData.getString(IS_AUTOINCREMENT)) ? columnBuilder.autoNumbered(-1) : columnBuilder;
  }


  /**
   * Read out the indexes for a particular table
   *
   * @param tableName The source table
   * @return The indexes for the table
   */
  protected List<Index> readIndexes(String tableName) {
    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      ResultSet indexResultSet = databaseMetaData.getIndexInfo(null, schemaName, tableName, false, false);
      try {
        List<Index> indexes = new LinkedList<Index>();

        SortedMap<String, List<String>> columnsByIndexName = new TreeMap<String, List<String>>();

        // there's one entry for each column in the index
        // the results are sorted by ordinal position already
        while (indexResultSet.next()) {
          String indexName = indexResultSet.getString(6);

          if (indexName == null) {
            continue;
          }

          // don't output the primary key as an index
          if (isPrimaryKeyIndex(indexName)) {
            continue;
          }

          // some indexes should be ignored anyway
          if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(indexName)) {
            continue;
          }

          String columnName = indexResultSet.getString(9);

          List<String> columnNames = columnsByIndexName.get(indexName);
          // maybe create a new one
          if (columnNames == null) {
            columnNames = new LinkedList<String>();
            boolean unique = !indexResultSet.getBoolean(4);

            indexes.add(new IndexImpl(indexName, unique, columnNames));
            columnsByIndexName.put(indexName, columnNames);
          }

          // add this column to the list
          columnNames.add(columnName);
        }

        return indexes;

      } finally {
        indexResultSet.close();
      }
    } catch (SQLException sqle) {
      throw new RuntimeSqlException("Error reading metadata for table [" + tableName + "]", sqle);
    }
  }


  /**
   * Populate the given map with information on the views in the database.
   *
   * @param viewMap Map to populate.
   */
  protected void populateViewMap(Map<String, View> viewMap) {
    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      ResultSet views = databaseMetaData.getTables(null, schemaName, null, new String[] { "VIEW" });
      try {
        while (views.next()) {
          final String viewName = views.getString(3);
          log.debug("Found view [" + viewName + "]");
          viewMap.put(viewName, new View() {
            @Override public String getName() { return viewName; }
            @Override public boolean knowsSelectStatement() { return false; }
            @Override public boolean knowsDependencies() { return false; }
            @Override public SelectStatement getSelectStatement() {
              throw new UnsupportedOperationException("Cannot return SelectStatement as [" + viewName + "] has been loaded from the database");
            }
            @Override public String[] getDependencies() {
              throw new UnsupportedOperationException("Cannot return dependencies as [" + viewName + "] has been loaded from the database");
            }
          });
        }
      } finally {
        views.close();
      }
    } catch (SQLException sqle) {
      throw new RuntimeSqlException("Error reading metadata for views", sqle);
    }
  }


  /**
   * @param indexName The index name
   * @return Whether this is the primary key for this table
   */
  protected boolean isPrimaryKeyIndex(String indexName) {
    return "PRIMARY".equals(indexName);
  }


  /**
   * Identify whether or not the table is one owned by the system, or owned by
   * our application. The default implementation assumes that all tables we can
   * access in the schema are under our control.
   *
   * @param tableName The table which we are accessing.
   * @return <var>true</var> if the table is owned by the system, and should not
   *         be managed by this code.
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
  protected boolean shouldIgnoreTable(@SuppressWarnings("unused") String tableName) {
    return false;
  }


  /**
   * Converts a given SQL data type to a {@link DataType}.
   *
   * @param sqlType The jdbc data type to convert.
   * @param typeName The (potentially database specific) type name
   * @param width The width of the column
   * @return The Cryo DataType that represents the sql connection data type
   *         given in <var>sqlType</var>.
   */
  protected DataType dataTypeFromSqlType(int sqlType, String typeName, @SuppressWarnings("unused") int width) {
    switch (sqlType) {
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
      case Types.BINARY:
        return DataType.BOOLEAN;
      case Types.DATE:
        return DataType.DATE;
      case Types.BLOB:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return DataType.BLOB;
      case Types.NCLOB:
      case Types.CLOB:
        return DataType.CLOB;

      default:
        throw new IllegalArgumentException("Unknown SQL data type [" + typeName + "] (" + sqlType + ")");
    }
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String name) {
    return tableNameMappings().containsKey(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    if (log.isDebugEnabled()) {
      log.debug("Find tables in schema [" + schemaName + "]");
    }

    return tableNameMappings().values();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    List<Table> result = new ArrayList<Table>();
    for (String tableName : tableNames()) {
      result.add(getTable(tableName));
    }
    return result;
  }


  /**
   * Reads the table names and makes sure we are case insensitive for future
   * requests.
   */
  protected void readTableNames() {
    try {
      final DatabaseMetaData databaseMetaData = connection.getMetaData();

      ResultSet tables = databaseMetaData.getTables(null, schemaName, null, TABLE_TYPES);
      try {
        while (tables.next()) {
          String tableName = tables.getString(3);
          String tableSchemaName = tables.getString(2);
          String tableType = tables.getString(4);

          foundTable(tableName, tableSchemaName, tableType);
        }
      } finally {
        tables.close();
      }
    } catch (SQLException sqle) {
      throw new RuntimeSqlException("SQLException in readTableNames()", sqle);
    }
  }


  /**
   * Declare a table that has been found in the metadata.
   *
   * @param tableName The name of the table
   * @param tableSchemaName The schema of the table
   * @param tableType The type of the table.
   */
  protected void foundTable(String tableName, String tableSchemaName, String tableType) {
    boolean tableIsSystemTable = isSystemTable(tableName);
    boolean tableShouldBeIgnored = shouldIgnoreTable(tableName);

    if (log.isDebugEnabled()) {
      log.debug("Found table [" + tableName + "] of type [" + tableType + "] in schema [" + tableSchemaName + "]"
          + (tableIsSystemTable ? " - SYSTEM TABLE" : "") + (tableShouldBeIgnored ? " - IGNORED" : ""));
    }

    // If this is not a system table and the schema name matches the requested
    // schema then add the table
    if (!tableIsSystemTable && !tableShouldBeIgnored) {
      tableNameMappings.put(tableName.toUpperCase(), tableName);
    }
  }


  /**
   * Sets the default value to an empty string for any column other than
   * version. Database-schema level default values are not supported by ALFA's
   * domain model hence we don't want to include a default value in the xml
   * definition of a table.
   *
   * @param columnName the name of the column
   * @return the default value
   */
  protected String determineDefaultValue(String columnName) {
    if (columnName.equals("version") || columnName.equals("version".toUpperCase())) {
      return "0";
    }

    return "";
  }

  /**
   * Implementation of {@link Index} for database meta data
   */
  protected static final class IndexImpl implements Index {

    /**
     * index name
     */
    private final String       name;
    /**
     * index is unique
     */
    private final boolean      isUnique;
    /**
     * index columns
     */
    private final List<String> columnNames;


    /**
     * @param name The name of the index
     * @param isUnique Whether the index is unique
     * @param columnNames The index column names
     */
    public IndexImpl(String name, boolean isUnique, List<String> columnNames) {
      super();
      this.name = name;
      this.isUnique = isUnique;
      this.columnNames = columnNames;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Index#columnNames()
     */
    @Override
    public List<String> columnNames() {
      return columnNames;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Index#getName()
     */
    @Override
    public String getName() {
      return name;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Index#isUnique()
     */
    @Override
    public boolean isUnique() {
      return isUnique;
    }


    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return "Index " + name + " on " + columnNames + (isUnique ? " (Unique)" : "");
    }
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String name) {
    return viewMap().containsKey(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String name) {
    return viewMap().get(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    return viewMap().keySet();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return viewMap().values();
  }

  private static class PrimaryKeyColumn {
    final Short sequence;
    final String name;
    PrimaryKeyColumn(Short sequence, String name) {
      this.sequence = sequence;
      this.name = name;
    }
  }
}
