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

package org.alfasoftware.morf.jdbc.oracle;

import static java.util.Collections.sort;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider.UnexpectedDataTypeException;
import org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.ColumnType;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;


/**
 * Oracle-specific meta data provision for databases.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class OracleMetaDataProvider implements AdditionalMetadata {

  /**
   * Standard log line.
   */
  private static final Log log = LogFactory.getLog(OracleMetaDataProvider.class);

  /**
   * Regex pattern matcher for the real name on column/table comments
   */
  private static final Pattern realnameCommentMatcher = Pattern.compile(".*"+OracleDialect.REAL_NAME_COMMENT_LABEL+":\\[([^\\]]*)\\](/TYPE:\\[([^\\]]*)\\])?.*");

  private Map<String, List<String>> keyMap;
  private Map<String, Table> tableMap;
  private Map<String, View> viewMap;
  private Map<String, Sequence> sequenceMap;

  private final Connection connection;
  private final String schemaName;
  private Map<String, String> primaryKeyIndexNames;

  /**
   * Construct a new meta data provider.
   *
   * <p>Converts the schema name to upper case, otherwise the provider cannot connect to it.</p>
   *
   * @param connection Connection details.
   * @param schemaName Schema name.
   */
  public OracleMetaDataProvider(Connection connection, String schemaName) {
    super();
    this.connection = connection;
    this.schemaName = schemaName.toUpperCase();
  }


  /**
   * Use to access the metadata for the tables in the specified connection.
   * Lazily initialises the metadata, and only loads it once.
   *
   * @return Table metadata.
   */
  private Map<String, Table> tableMap() {
    if (tableMap != null) {
      return tableMap;
    }

    tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    expensiveReadTableNames();
    return tableMap;
  }


  /**
   * Use to access the metadata for the views in the specified connection.
   * Lazily initialises the metadata, and only loads it once.
   *
   * @return View metadata.
   */
  private Map<String, View> viewMap() {
    if (viewMap != null) {
      return viewMap;
    }

    viewMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    readViewMap();
    return viewMap;
  }


  /**
   * Use to access the metadata for the views in the specified connection.
   * Lazily initialises the metadata, and only loads it once.
   *
   * @return Sequence metadata.
   */
  private Map<String, Sequence> sequenceMap() {
    if (sequenceMap != null) {
      return sequenceMap;
    }

    sequenceMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    readSequenceMap();
    return sequenceMap;
  }


  @Override
  public Map<String, String> primaryKeyIndexNames() {
    if (primaryKeyIndexNames != null) {
      return primaryKeyIndexNames;
    }
    tableMap();
    return primaryKeyIndexNames;
  }


  /**
   * A table name reading method which is more efficient than the Oracle driver meta-data version.
   *
   * @see <a href="http://download.oracle.com/docs/cd/B19306_01/server.102/b14237/statviews_2094.htm">ALL_TAB_COLUMNS specification</a>
   */
  private void expensiveReadTableNames() {
    log.info("Starting read of table definitions");

    long start = System.currentTimeMillis();

    // -- Stage 1: identify tables & keys...
    //
    final Map<String, List<String>> primaryKeys = keyMap();

    // -- Stage 2: get column data...
    //
    // Explicitly ignore the BIN$ tables as they are in the recycle bin (for flashback)
    final String getColumnsSql = "select cols.table_name, tabcomments.comments as table_comment, cols.column_name, colcomments.COMMENTS, cols.data_type, cols.char_length, cols.data_length, cols.data_precision, cols.data_scale, cols.nullable, cols.DATA_DEFAULT "
      +
      "from ALL_TAB_COLUMNS cols JOIN ALL_TAB_COMMENTS tabcomments ON cols.OWNER = tabcomments.OWNER AND cols.table_name = tabcomments.table_name " +
      "JOIN ALL_COL_COMMENTS colcomments ON cols.OWNER = colcomments.OWNER AND cols.table_name = colcomments.table_name AND cols.column_name = colcomments.column_name " +
      "JOIN ALL_TABLES tables on cols.OWNER = tables.OWNER and cols.table_name = tables.table_name " +
      "where cols.owner=? and cols.table_name not like 'BIN$%' AND tables.TEMPORARY = 'N' order by cols.table_name, cols.column_id";
    runSQL(getColumnsSql, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {
        while ( resultSet.next()) {
          String tableName = resultSet.getString(1);
          String tableComment = resultSet.getString(2);
          String columnName = resultSet.getString(3);
          String columnComment = resultSet.getString(4);
          String dataTypeName = resultSet.getString(5);

          if (isSystemTable(tableName))
            continue;

          try {
            Integer dataLength;
            if (dataTypeName.contains("CHAR")) {
              dataLength = resultSet.getInt(6);
            } else {
              dataLength = resultSet.getInt(7);
            }
            Integer dataPrecision;
            if (resultSet.getString(8) == null) {
              dataPrecision = null;
            } else {
              dataPrecision = resultSet.getInt(8);
            }
            Integer dataScale = resultSet.getInt(9);
            String nullableStr = resultSet.getString(10);
            String defaultValue = determineDefaultValue(columnName);
            String actualDefaultValue = determineActualDefaultValue(resultSet.getString(11));
            if (!defaultValue.equals(actualDefaultValue)) {
                log.warn("DEFAULT value for " + tableName + "." + columnName + " expected to be [" + defaultValue + "], but was [" + actualDefaultValue + "]");
            }

            handleTableColumnRow(primaryKeys, tableName, tableComment, columnName, columnComment, dataTypeName, dataLength,
              dataPrecision, dataScale, nullableStr, defaultValue);
          } catch (Exception e) {
            throw new RuntimeException("Exception while reading metadata for table [" + tableName + "] column [" + columnName + "] datatype [" + dataTypeName + "]", e);
          }
        }
      }


      /**
       * Handle the column read from the result set.
       */
      private void handleTableColumnRow(final Map<String, List<String>> primaryKeys,
                                        String tableName, String tableComment,
                                        String columnName, String columnComment,
                                        String dataTypeName, Integer dataLength,
                                        Integer dataPrecision, Integer dataScale,
                                        String nullableStr, String defaultValue) {
        String commentType = null;

        if (tableComment != null) {
          Matcher matcher = realnameCommentMatcher.matcher(tableComment);
          if (matcher.matches()) {
            String tableNameFromComment = matcher.group(1);
            if (tableNameFromComment.toUpperCase().equals(tableName)) {
              tableName = tableNameFromComment;
            } else {
              throw new RuntimeException("Table name [" + tableNameFromComment + "] in comment does not match oracle table name [" + tableName + "]");
            }
          }
        }

        if (columnComment != null) {
          Matcher matcher = realnameCommentMatcher.matcher(columnComment);
          if (matcher.matches()) {
            columnName = matcher.group(1);
            commentType = matcher.group(3);
          }
        }

        Table currentTable = tableMap.get(tableName);

        if (currentTable == null) {
          currentTable = table(tableName);
          tableMap.put(tableName, currentTable);
        }

        boolean primaryKey = false;
        List<String> primaryKeyColumns = primaryKeys.get(tableName.toUpperCase());
        if (primaryKeyColumns != null) {
          primaryKey = primaryKeyColumns.contains(columnName.toUpperCase());
        }

        int autoIncrementFrom = getAutoIncrementStartValue(columnComment);
        boolean isAutoIncrement = autoIncrementFrom != -1;
        autoIncrementFrom = autoIncrementFrom == -1 ? 0 : autoIncrementFrom;

        // Deferred type column required as tables not yet excluded will be processed at this stage.
        currentTable.columns().add(
          new DeferredTypeColumn(
            dataTypeName,
            dataLength,
            dataPrecision == null ? 0 : dataPrecision,
            dataScale == null ? 0 : dataScale,
            commentType,
            columnName,
            "Y".equals(nullableStr), // nullable
            primaryKey, isAutoIncrement, autoIncrementFrom, defaultValue
          )
        );
      }});

    //
    // -- Stage 2b: Re-order the columns as per the primary key order...
    //
    for( Entry<String, Table> entry : tableMap.entrySet()) {
      final List<String> primaryKeysForTable = primaryKeys.get(entry.getKey().toUpperCase());
      // Table which don't have a primary key return null here
      if (primaryKeysForTable != null) {
        sort(entry.getValue().columns(), new PrimaryKeyComparator(primaryKeysForTable));
      }
    }

    long pointTwo = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug(String.format("Loaded table column list in %dms", pointTwo - start));
      log.debug("Loading indexes: [" + tableMap.size() + "]");
    }

    Supplier<Map<String, Set<String>>> indexPartitions = Suppliers.memoize(() -> {
      Map<String, Set<String>> result = new HashMap<>();
      runSQL("select index_name, status from ALL_IND_PARTITIONS where index_owner=?",
        resultSet -> {
          while(resultSet.next()) {
            result.computeIfAbsent(resultSet.getString(1), k -> new HashSet<>()).add(resultSet.getString(2));
          }
        });
      return result;
      }
    );


    // -- Stage 3: find the index names...
    //
    primaryKeyIndexNames = Maps.newHashMap();
    final String getIndexNamesSql = "select table_name, index_name, uniqueness, status from ALL_INDEXES where owner=? order by table_name, index_name";
    runSQL(getIndexNamesSql, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {
        int indexCount = 0;
        while (resultSet.next()) {
          String tableName = resultSet.getString(1);
          String indexName = resultSet.getString(2);
          String uniqueness = resultSet.getString(3);
          String status = resultSet.getString(4);

          Table currentTable = tableMap.get(tableName);

          if (currentTable == null) {
            log.warn(String.format("Table [%s] was not in the table map - ignoring index [%s]", tableName, indexName));
            continue;
          }

          final boolean unique = "UNIQUE".equals(uniqueness);
          boolean isValid = isValid(status, indexName, indexPartitions);

          // don't output the primary key as an index
          if(isPrimaryKeyIndex(indexName) && isValid) {
            if (log.isDebugEnabled()) {
              log.debug(String.format("Ignoring index [%s] on table [%s] as it is a primary key index", indexName, tableName));
            }

            if (!unique) {
              log.warn("Primary Key on table [" + tableName + "] is backed by non-unique index [" + indexName + "]");
            }
            primaryKeyIndexNames.put(tableName.toUpperCase(), indexName);
            continue;
          }

          // Chop up the index name
          if (indexName.toUpperCase().startsWith(currentTable.getName().toUpperCase())) {
            indexName = currentTable.getName() + indexName.substring(currentTable.getName().length());
          }

          if (!isValid) {
            log.fatal("Index [" + indexName + "] is not in a valid state");
            indexName = indexName + "<UNUSABLE>"; // this will cause the schema checker to find a mismatch and also provide a good hint in the log messages what was wrong
          }

          final String indexNameFinal = indexName;

          if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(indexName)) {
            Index ignoredIndex = getAssembledIndex(unique, indexNameFinal);
            currentTable.ignoredIndexes().add(ignoredIndex);
            continue;
          }

          currentTable.indexes().add(getAssembledIndex(unique, indexNameFinal));
          indexCount++;
        }

        if (log.isDebugEnabled()) {
          log.debug(String.format("Loaded %d indexes", indexCount));
        }
      }

      private Index getAssembledIndex(boolean unique, String indexNameFinal) {
        return new Index() {
          private final List<String> columnNames = new ArrayList<>();

          @Override
          public boolean isUnique() {
            return unique;
          }


          @Override
          public String getName() {
            return indexNameFinal;
          }


          @Override
          public List<String> columnNames() {
            return columnNames;
          }


          @Override
          public String toString() {
            return this.toStringHelper();
          }
        };
      }


    });

    long pointThree = System.currentTimeMillis();
    if (log.isDebugEnabled()) {
      log.debug(String.format("Loaded index list in %dms", pointThree - pointTwo));
      log.debug("Loading index columns");
    }

    // -- Stage 4: find the index columns...
    //
    final String getIndexColumnsSql = "select table_name, INDEX_NAME, COLUMN_NAME from ALL_IND_COLUMNS where INDEX_OWNER=? order by table_name, index_name, column_position";
    runSQL(getIndexColumnsSql, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {

        while (resultSet.next()) {
          String tableName = resultSet.getString(1);

          Table currentTable = tableMap.get(tableName);

          if (currentTable == null) {
            continue;
          }

          String indexName = resultSet.getString(2);
          String columnName = resultSet.getString(3);

          // Skip this column if the index is a primary key index
          if (isPrimaryKeyIndex(indexName)) {
            if (log.isDebugEnabled()) {
              log.debug(String.format("Ignoring index [%s] on table [%s] as it is a primary key index", indexName, tableName));
            }
            continue;
          }

          if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(indexName)) {
            Index lastIndex = null;
            for (Index currentIndex : currentTable.ignoredIndexes()) {
              if (currentIndex.getName().equalsIgnoreCase(indexName)) {
                lastIndex = currentIndex;
                break;
              }
            }

            if (lastIndex == null) {
              log.warn(String.format("Ignoring index details for index [%s] on table [%s] as no index definition exists", indexName, tableName));
              continue;
            }

            // Correct the case on the column name
            columnName = getColumnCorrectCase(currentTable, columnName);

            lastIndex.columnNames().add(columnName);

            continue;
          }

          Index lastIndex = null;
          for (Index currentIndex : currentTable.indexes()) {
            if (currentIndex.getName().equalsIgnoreCase(indexName)) {
              lastIndex = currentIndex;
              break;
            }
          }

          if (lastIndex == null) {
            log.warn(String.format("Ignoring index details for index [%s] on table [%s] as no index definition exists", indexName, tableName));
            continue;
          }

          // Correct the case on the column name
          columnName = getColumnCorrectCase(currentTable, columnName);

          lastIndex.columnNames().add(columnName);
        }
      }

      private String getColumnCorrectCase(Table currentTable, String columnName) {
        for (Column currentColumn : currentTable.columns()) {
          if (currentColumn.getName().equalsIgnoreCase(columnName)) {
            columnName = currentColumn.getName();
            break;
          }
        }
        return columnName;
      }
    });

    long end = System.currentTimeMillis();
    if (log.isDebugEnabled()) log.debug(String.format("Loaded index column list in %dms", end - pointThree));

    log.info(String.format("Read table metadata in %dms; %d tables", end - start, tableMap.size()));
  }


  private boolean isValid(String status, String indexName, Supplier<Map<String, Set<String>>> indexPartitions) {
    if ("VALID".equals(status)
        || "true".equals(System.getProperty("org.alfasoftware.morf.jdbc.oracle.OracleMetaDataProvider.disableIndexValidation"))) {
      return true;
    }

    if ("UNUSABLE".equals(status)) {
      return false;
    }

    // if we have another status (usually 'N/A') it is likely because the index is partitioned, so we have to check the status on ALL_IND_PARTITIONS
    if (indexPartitions.get().containsKey(indexName)) {
      return !indexPartitions.get().get(indexName).contains("UNUSABLE");
    }

    log.warn("Unable to determine validity of index [" + indexName + "] based on status [" + status + "]");

    return false;
  }


  /**
   * Get our {@link DataType} from the Oracle type. This serves the same purpose
   * as {@link DatabaseMetaDataProvider#dataTypeFromSqlType(int, String, int)} but is
   * entirely Oracle specific.
   *
   * @param dataTypeName The Oracle type name.
   * @param commentType the type of the column stored in a comment.
   * @return The DataType.
   * @throws UnexpectedDataTypeException If data type cannot be parsed.
   */
  private static DataType dataTypeForColumn(String columnName, String dataTypeName, String commentType) {
    /*
     * Oracle stores all numeric types as 'NUMBER', so we have no easy way of
     * identifying fields such as 'int' or 'big int'. As such, the actual data
     * type of the column is stored in a comment against that column. Hence, if
     * we're given a type from a comment then try and use that, only falling
     * back to the matching below if we don't have/find one.
     *
     * It's not possible to reverse engineer the type from the database because
     * of things such as foreign keys: although the ID column is a 'big int', the
     * actual value on a column that links to this ID will be stored as a decimal
     * in most cases.
     */
    if (StringUtils.isNotEmpty(commentType)) {
      for (DataType dataType : DataType.values()) {
        if (dataType.toString().equals(commentType)) {
          return dataType;
        }
      }
    }

    if ("NVARCHAR2".equals(dataTypeName) || "VARCHAR2".equals(dataTypeName)) {
      return DataType.STRING;
    } else if ("NUMBER".equals(dataTypeName)) {
      return DataType.DECIMAL;
    } else if ("BLOB".equals(dataTypeName)) {
      return DataType.BLOB;
    } else if ("NCLOB".equals(dataTypeName)) {
      return DataType.CLOB;
    } else if ("DATE".equals(dataTypeName)) {
      return DataType.DATE;
    }
    else {
      throw new DatabaseMetaDataProvider.UnexpectedDataTypeException("Unsupported data type [" + dataTypeName + "]" + " in [" + columnName + "]");
    }
  }


  /**
   * Populate {@link #viewMap} with information from the database. Since JDBC metadata reading
   * is slow on Oracle, this uses an optimised query.
   *
   * @see <a href="http://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2117.htm">ALL_VIEWS specification</a>
   */
  private void readViewMap() {
    log.info("Starting read of view definitions");

    long start = System.currentTimeMillis();

    // Explicitly ignore the BIN$ tables as they are in the recycle bin (for flashback)
    final String viewsSql = "SELECT view_name FROM ALL_VIEWS WHERE owner=?";
    runSQL(viewsSql, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          final String viewName = resultSet.getString(1);
          if (isSystemTable(viewName))
            continue;

          viewMap.put(viewName.toUpperCase(), new View() {
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
      }
    });

    long end = System.currentTimeMillis();
    log.info(String.format("Read view metadata in %dms; %d views", end - start, viewMap.size()));
  }


  /**
   * Populate {@link #sequenceMap} with information from the database. Since JDBC metadata reading
   * is slow on Oracle, this uses an optimised query.
   *
   * @see <a href="http://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2117.htm">ALL_VIEWS specification</a>
   */
  private void readSequenceMap() {
    log.info("Starting read of sequence definitions");

    long start = System.currentTimeMillis();

    /**
     * Here we filter out any sequences which have been created to as part of a table autonumber column as we only want to consider
     * sequences that are effectively schema level as we do with other dialects.
     * @see OracleDialect#createNewSequence(Table, Column) for more context on how table level sequences are created.
     */
    final String sequencesSql = "SELECT sequence_name FROM ALL_SEQUENCES WHERE cache_size != 2000 AND sequence_owner=?";
    runSQL(sequencesSql, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          final String sequenceName = resultSet.getString(1);
          if (isSystemSequence(sequenceName))
            continue;

          sequenceMap.put(sequenceName.toUpperCase(), new Sequence() {
            @Override public String getName() { return sequenceName; }

            @Override
            public Integer getStartsWith() {
              return null;
            }

            @Override
            public boolean isTemporary() {
              return false;
            }
          });
        }
      }
    });

    long end = System.currentTimeMillis();
    log.info(String.format("Read sequence metadata in %dms; %d sequences", end - start, sequenceMap.size()));
  }


  /**
   * Reading all the table metadata is slow on Oracle, so we can optimise the empty
   * database check by just seeing if there are any tables.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    return keyMap().isEmpty();
  }


  /**
   * Use to access the metadata for the primary keys in the specified connection.
   * Lazily initialises the metadata, and only loads it once.
   *
   * @return Primary keys metadata.
   */
  private Map<String, List<String>> keyMap() {
    if (keyMap != null) {
      return keyMap;
    }

    keyMap = new HashMap<>();
    expensiveReadTableKeys();
    return keyMap;
  }


  /**
   * Read the tables, and the primary keys for the database.
   *
   * @return A map of table name to primary key(s).
   */
  private Map<String, List<String>> expensiveReadTableKeys() {
    log.info("Starting read of key definitions");
    long start = System.currentTimeMillis();

    final StringBuilder primaryKeysWithWrongIndex = new StringBuilder();

    final String getConstraintSql = "SELECT A.TABLE_NAME, A.COLUMN_NAME, C.INDEX_NAME FROM ALL_CONS_COLUMNS A "
        + "JOIN ALL_CONSTRAINTS C  ON A.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND A.OWNER = C.OWNER and A.TABLE_NAME = C.TABLE_NAME "
        + "WHERE C.TABLE_NAME not like 'BIN$%' AND C.OWNER=? AND C.CONSTRAINT_TYPE = 'P' ORDER BY A.TABLE_NAME, A.POSITION";

    runSQL(getConstraintSql, new ResultSetHandler() {
      @Override public void handle(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
          String tableName = resultSet.getString(1);
          String columnName = resultSet.getString(2);
          String pkIndexName = resultSet.getString(3);

          if (pkIndexName == null || !pkIndexName.endsWith("_PK")) {
            primaryKeysWithWrongIndex.append("Primary Key on table [").append(tableName)
                    .append("] column [").append(columnName).append("] backed with an index whose name does not end in _PK [")
                    .append(pkIndexName).append("]").append(System.lineSeparator());
          }

          List<String> columns = keyMap.get(tableName);
          if (columns == null) {
            columns = new ArrayList<>();
            keyMap.put(tableName, columns);
          }

          columns.add(columnName);
        }
      }
    });

    if (primaryKeysWithWrongIndex.length() > 0) {
      throw new RuntimeException(primaryKeysWithWrongIndex.toString());
    }

    long end = System.currentTimeMillis();
    log.info(String.format("Read key metadata in %dms; %d tables", end - start, keyMap.size()));

    return keyMap;
  }


  /**
   * Handler for {@link ResultSet}s from some SQL.
   */
  private interface ResultSetHandler {
    /**
     * handle the results.
     * @param resultSet The result set to handle
     * @throws SQLException If an db exception occurs.
     */
    void handle(ResultSet resultSet) throws SQLException;
  }


  /**
   * Run some SQL, and tidy up afterwards.
   *
   * Note this assumes a predicate on the schema name will be present with a single parameter in position "1".
   *
   * @param sql The SQL to run.
   * @param handler The handler to handle the result-set.
   */
  private void runSQL(String sql, ResultSetHandler handler) {
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      try {
        // We'll inevitably need a lot of meta data so may as well get it in big chunks.
        statement.setFetchSize(100);

        // pass through the schema name
        statement.setString(1, schemaName);

        ResultSet resultSet = statement.executeQuery();
        try {
          handler.handle(resultSet);
        } finally {
          resultSet.close();
        }
      } finally {
        statement.close();
      }
    } catch (SQLException sqle) {
      throw new RuntimeSqlException("Error running SQL: " + sql, sqle);
    }
  }


  /**
   * Oracle sometimes spits back some very odd table names, something to do with the system. We don't want those.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isSystemTable(DatabaseMetaDataProvider.RealName)
   */
  private boolean isSystemTable(String tableName) {
    return !tableName.matches("\\w+") || tableName.matches("DBMS_\\w+") || tableName.matches("SYS_\\w+");
  }


  /**
   * Oracle sometimes spits back some very odd sequence names, something to do with the system. We don't want those.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isSystemSequence(DatabaseMetaDataProvider.RealName)
   */
  private boolean isSystemSequence(String sequenceName) {
    return !sequenceName.matches("\\w+") || sequenceName.matches("DBMS_\\w+") || sequenceName.matches("SYS_\\w+");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(DatabaseMetaDataProvider.RealName)
   */
  private boolean isPrimaryKeyIndex(String indexName) {
    return indexName.endsWith("_PK");
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String name) {
    return tableMap().containsKey(name.toUpperCase());
  }


  /**
   * {@inheritDoc}
   *
   * <p>The {@link Table} implementation returned may contain {@link Column} implementations
   * which evaluate the metadata elements ({@link Column#getType()}, {@link Column#getWidth()}
   * etc.) lazily.  If the database column type is not supported, this may throw an
   * {@link UnexpectedDataTypeException} when evaluated.  This allows tables with unsupported
   * data types to be enumerated (and thus co-exist in the database schema) but not be supported
   * by the application.</p>
   *
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String name) {
    return tableMap().get(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    return tableMap().keySet();
  }


  /**
   * {@inheritDoc}
   *
   * <p>The {@link Table} implementation returned may contain {@link Column} implementations
   * which evaluate the metadata elements ({@link Column#getType()}, {@link Column#getWidth()}
   * etc.) lazily.  If the database column type is not supported, this may throw an
   * {@link UnexpectedDataTypeException} when evaluated.  This allows tables with unsupported
   * data types to be enumerated (and thus co-exist in the database schema) but not be supported
   * by the application.</p>
   *
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return tableMap().values();
  }


  /**
   * Sets the default value to an empty string for any column other than version. Database-schema level default values are
   * not supported by ALFA's domain model hence we don't want to include a default value in the xml definition of a table.
   *
   * @param columnName the name of the column
   * @return the default value
   */
  private String determineDefaultValue(String columnName) {
    if (columnName.equals("VERSION")) {
      return "0";
    }

    return "";
  }


  private String determineActualDefaultValue(final String actualDefaultValue) {
    // columns that never had DEFAULT
    if (actualDefaultValue == null)
      return "";

    final String trimedActualDefaultValue = actualDefaultValue.trim();

    // columns that previously had DEFAULT and were set to DEFAULT NULL
    if ("NULL".equalsIgnoreCase(trimedActualDefaultValue))
      return "";

    // columns that previously had DEFAULT and were set to DEFAULT ''
    if ("''".equals(trimedActualDefaultValue))
      return "";

    // other values returned with just a bit of trimming
    // - note that these are Oracle expressions, not actual default values
    // - simple decimals come back as decimals,
    // - strings come back wrapped in single quotes,
    // - functions come back as expressions,
    // - as specified in the last alter statement
    return trimedActualDefaultValue;
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


  /**
   * @see org.alfasoftware.morf.metadata.Schema#sequenceExists(java.lang.String)
   */
  @Override
  public boolean sequenceExists(String name) {
    return sequenceMap().containsKey(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getSequence(String)
   */
  @Override
  public Sequence getSequence(String name) {
    return sequenceMap().get(name.toUpperCase());
  }


  /**
   * @see Schema#sequenceNames()
   */
  @Override
  public Collection<String> sequenceNames() {
    return sequenceMap().keySet();
  }


  /**
   * @see Schema#sequences()
   */
  @Override
  public Collection<Sequence> sequences() {
    return sequenceMap().values();
  }


  private static final class PrimaryKeyComparator implements Comparator<Column> {

    private final List<String> primaryKeysForTable;

    PrimaryKeyComparator(final List<String> primaryKeysForTable) {
      this.primaryKeysForTable = primaryKeysForTable;
    }


    /**
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(Column o1, Column o2) {
      String col1 = o1.getName().toUpperCase();
      String col2 = o2.getName().toUpperCase();
      if (primaryKeysForTable.contains(col1) &&
          primaryKeysForTable.contains(col2)) {
        return primaryKeysForTable.indexOf(col1) < primaryKeysForTable.indexOf(col2) ? -1 : 1; // Indexes can't be equal
      } else if (primaryKeysForTable.contains(col1)) {
        return -1;
      } else if (primaryKeysForTable.contains(col2)) {
        return 1;
      } else {
        return 0; // Neither column a primary key; no re-ordering
      }
    }
  }


  /**
   * This implementation of {@link Column} defers determining the data type of
   * the column to allow for tables which may use data types not supported by
   * Morf to be included in a schema. Exceptions regarding the incompatibility
   * will only be thrown if the data type is queried.
   */
  private static final class DeferredTypeColumn implements Column {

    private final String columnName;
    private final Supplier<String> upperCaseName;
    private final boolean nullable;
    private final boolean primaryKey;
    private final boolean autoIncrement;
    private final int autoIncrementFrom;
    private final String defaultValue;

    private final com.google.common.base.Supplier<ColumnType> columnType;

    DeferredTypeColumn(String dataTypeName, int dataLength, int precision, int scale, String commentType, String columnName,
        boolean nullable, boolean primaryKey, boolean autoIncrement, int autoIncrementFrom, String defaultValue) {
      super();
      this.columnName = columnName;
      this.upperCaseName = Suppliers.memoize(columnName::toUpperCase);
      this.nullable = nullable;
      this.primaryKey = primaryKey;
      this.autoIncrement = autoIncrement;
      this.autoIncrementFrom = autoIncrementFrom;
      this.defaultValue = defaultValue;
      this.columnType = Suppliers.memoize(() -> {
        DataType dataType = dataTypeForColumn(columnName, dataTypeName, commentType);

        if (commentType == null && dataType == DataType.DECIMAL) {
          // Oracle doesn't store the precision for integer columns - so it's the version
          if (precision == 0) {
            dataType = DataType.INTEGER;
          }
          // Only the ID column can be the BIG_INTEGER primary key
          else if (precision == 19 && columnName.equalsIgnoreCase("id")) {
            dataType = DataType.BIG_INTEGER;
          }
        }

        return SchemaUtils.type(dataType, DataType.STRING == dataType ? dataLength : precision, scale);
      });
    }


    @Override
    public boolean isNullable() {
      return nullable;
    }

    @Override
    public boolean isPrimaryKey() {
      return primaryKey;
    }

    @Override
    public boolean isAutoNumbered() {
      return autoIncrement;
    }

    @Override
    public String getName() {
      return columnName;
    }

    @Override
    public String getUpperCaseName() {
      return upperCaseName.get();
    }

    @Override
    public String getDefaultValue() {
      return defaultValue;
    }

    @Override
    public int getAutoNumberStart() {
      return autoIncrementFrom;
    }

    @Override
    public DataType getType() {
      return columnType.get().getType();
    }

    @Override
    public int getWidth() {
      return columnType.get().getWidth();
    }

    @Override
    public int getScale() {
      return columnType.get().getScale();
    }

    @Override
    public String toString() {
      return this.toStringHelper();
    }
  }
}