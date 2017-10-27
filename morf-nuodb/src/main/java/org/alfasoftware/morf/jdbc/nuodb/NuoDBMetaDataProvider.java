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

package org.alfasoftware.morf.jdbc.nuodb;

import static java.util.Arrays.stream;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;

/**
 * Database meta-data layer for NUODB.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class NuoDBMetaDataProvider extends DatabaseMetaDataProvider {

  private static final Logger log = LoggerFactory.getLogger(NuoDBMetaDataProvider.class);

  private Multimap<String, ColumnBuilder> columnMetaData;

  private ArrayListMultimap<String, Index> indexMetaData;

  private ArrayListMultimap<String, String> primaryKeyMetaData;


  /**
   * Converts a given SQL data type to a {@link DataType}.
   * @param typeName The database specific type name
   * @return The Cryo DataType that represents the sql connection data type
   *         given in <var>sqlType</var>.
   */
  private static DataType dataTypeFromDeclaredType(String typeName) {
    switch (typeName.toUpperCase()) {
      case "INTEGER":
        return DataType.INTEGER;
      case "BIGINT":
        return DataType.BIG_INTEGER;
      case "NUMERIC":
      case "DECIMAL":
        return DataType.DECIMAL;
      case "CHARACTER VARYING":
      case "CHAR":
      case "VARCHAR":
        return DataType.STRING;
      case "SMALLINT":  //In NuoDB, we reserve smallint for storing boolean values
      case "BOOLEAN":
        return DataType.BOOLEAN;
      case "DATE":
        return DataType.DATE;
      case "BLOB":
        return DataType.BLOB;
      case "NCLOB":
        return DataType.CLOB;

      default:
        throw new IllegalArgumentException("Unknown SQL data type [" + typeName + "]");
    }
  }


  /**
   * @param connection DataSource to provide meta data for.
   */
  public NuoDBMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  @Override
  protected List<String> getPrimaryKeys(String tableName) {
    if (primaryKeyMetaData == null) {
      log.info("Initialising index metadata cache for schema [" + schemaName + "]");
      retrieveIndexMetaData(connection, schemaName);
    }
    return primaryKeyMetaData.get(tableName);
  }


  @Override
  protected List<Index> readIndexes(String tableName) {
    if (indexMetaData == null) {
      log.info("Initialising index metadata cache for schema [" + schemaName + "]");
      retrieveIndexMetaData(connection, schemaName);
    }

    return indexMetaData.get(tableName);
  }


  /**
   * Cache the column metadata on the first column read for the whole schema for efficiency in NuoDB.
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#readColumns(java.lang.String, java.util.List)
   */
  @Override
  protected List<Column> readColumns(String tableName, final List<String> primaryKeys) {
    if (columnMetaData == null) {
      log.info("Initialising column metadata cache for schema [" + schemaName + "]");
      columnMetaData = retrieveColumnMetaData(connection, schemaName);
    }

    List<Column> rawColumns = new LinkedList<>(Collections2.transform(columnMetaData.get(tableName), new Function<ColumnBuilder, Column>() {
      @Override
      public Column apply(ColumnBuilder input) {
        return primaryKeys.contains(input.getName()) ? input.primaryKey() : input;
      }
    }));
    return sortByPrimaryKey(primaryKeys, rawColumns);
  }



  /**
   * Retrieve the metadata for every field, on every table, in the schema.
   * <br> Reading from the System tables is rather slow so we read every table in one
   * go and cache the result for efficiency.
   * <br> NuoDB can inaccurately store data types and so we parse the declared type
   * for a column rather than the stored type.
   * <br> The database driven autonumbering is managed with generator sequences, and we
   * store the autonumber start value as a part of the generator sequence name.
   */
  private Multimap<String, ColumnBuilder> retrieveColumnMetaData(Connection connection, String schemaName) {
    String columnQuery = "SELECT F.TABLENAME, F.FIELD, F.DECLARED_TYPE, F.GENERATOR_SEQUENCE, F.SCALE, F.PRECISION, F.FLAGS "
        + "FROM SYSTEM.FIELDS AS F, SYSTEM.TABLES AS T "
        + "WHERE T.TABLENAME = F.TABLENAME AND T.TYPE = 'TABLE' AND T.SCHEMA = F.SCHEMA AND F.SCHEMA = ?";

    try (PreparedStatement createStatement = connection.prepareStatement(columnQuery)) {
      createStatement.setString(1, schemaName);
      try (ResultSet columnQueryResult = createStatement.executeQuery()) {
        Multimap<String, ColumnBuilder> multimap = ArrayListMultimap.create();
        while (columnQueryResult.next()) {
          String tablename = columnQueryResult.getString("TABLENAME");
          String fieldName = columnQueryResult.getString("FIELD");
          String declaredDataType = columnQueryResult.getString("DECLARED_TYPE");

          if (log.isDebugEnabled()) log.debug("Fetched metadata for [" + tablename + "." + fieldName + "] type [" + declaredDataType + "]");
          if (declaredDataType == null) throw new IllegalStateException("Null declared type for [" + tablename + "].[" + fieldName + "]");

          DataType dataType = dataTypeFromDeclaredType(declaredDataType.replaceFirst("^([a-z ]+).*$", "$1"));

          ColumnBuilder nuoDBFieldMetaData = column(fieldName, dataType, columnQueryResult.getInt("PRECISION"), columnQueryResult.getInt("SCALE"));
          nuoDBFieldMetaData = nuoDBFieldMetaData.defaultValue(determineDefaultValue(fieldName));
          nuoDBFieldMetaData = columnQueryResult.getInt("FLAGS") == 0 ? nuoDBFieldMetaData.nullable() : nuoDBFieldMetaData;

          if (StringUtils.isNotBlank(columnQueryResult.getString("GENERATOR_SEQUENCE"))) {
            nuoDBFieldMetaData = nuoDBFieldMetaData.autoNumbered(fetchAutoNumber(columnQueryResult.getString("GENERATOR_SEQUENCE")));
          }

          if (log.isDebugEnabled()) log.debug("Caching metadata for schema [" + schemaName + "], table [" + tablename + "], column [" + fieldName + "]");
          multimap.put(tablename, nuoDBFieldMetaData);
        }
        return multimap;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * NUODB reports its primary key indexes as ..PRIMARY_KEY or similar.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#isPrimaryKeyIndex(java.lang.String)
   */
  @Override
  protected boolean isPrimaryKeyIndex(String indexName) {
    return indexName.endsWith("..PRIMARY_KEY");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider#shouldIgnoreTable(java.lang.String)
   */
  @Override
  protected boolean shouldIgnoreTable(String tableName) {
    // Ignore temporary tables
    return tableName.toUpperCase().startsWith(NuoDBDialect.TEMPORARY_TABLE_PREFIX);
  }


  /**
   * Fetch the autonumber start from the name of the generator sequence in NuoDB
   */
  private int fetchAutoNumber(String generatorSequence) {
    try {
      return Integer.parseInt(generatorSequence.substring(generatorSequence.lastIndexOf('_') + 1));
    } catch (NumberFormatException e) {
      throw new RuntimeException("Cannot determine AutoNumber start from Generator_Sequence [" + generatorSequence + "]", e);
    }
  }


  /**
   * Creates a map of table name to index information (including primary keys).
   * <br> This is then cached as two multimaps, one holding index metadata, the other
   * primary key metadata.
   * <br> Reading from the System tables is rather slow so we read every table in one
   * go and cache the result for efficiency.
   */
  private void retrieveIndexMetaData(Connection connection, String schemaName) {
    String indexQuery = "SELECT I.INDEXNAME, I.TABLENAME, I.INDEXTYPE, FI.FIELD, FI.POSITION, I.FIELDCOUNT "
        + "FROM SYSTEM.INDEXES AS I, SYSTEM.INDEXFIELDS AS FI "
        + "WHERE I.INDEXNAME = FI.INDEXNAME AND I.TABLENAME = FI.TABLENAME AND I.SCHEMA = FI.SCHEMA AND I.SCHEMA = ?";

    //Create a nested map TableName to [sortedmultimap of IndexTuple to OrderedColumnsOnIndex]
    Map<String, Map<IndexTuple, String[]>> tableIndexMap =  new HashMap<>();
    try (PreparedStatement createStatement = connection.prepareStatement(indexQuery)) {
      createStatement.setString(1, schemaName);
      try (ResultSet columnQueryResult = createStatement.executeQuery()) {

        while (columnQueryResult.next()) {
          String indexName = columnQueryResult.getString("INDEXNAME");
          String tableName = columnQueryResult.getString("TABLENAME");
          String columnName = columnQueryResult.getString("FIELD");
          int indexType = columnQueryResult.getInt("INDEXTYPE");
          int fieldsOnIndex = columnQueryResult.getInt("FIELDCOUNT");
          //Note, NuoDB stores its column positions in the index, zero indexed.
          int positionInIndex = columnQueryResult.getInt("POSITION");

          if (log.isDebugEnabled()) log.debug("Fetched index metadata for [" + tableName + "], index name [" + indexName + "] and column ["+ columnName + "] in position [" + positionInIndex + "]");

          Map<IndexTuple, String[]> indexesOnTable = tableIndexMap.computeIfAbsent(tableName, k -> new HashMap<>());

          //Get the ordered list of columns in the index
          IndexTuple indexTuple = new IndexTuple(indexName, indexType);
          String[] orderedcolumnsForIndex = indexesOnTable.computeIfAbsent(indexTuple, k -> new String[fieldsOnIndex]);

          //Update the column list
          orderedcolumnsForIndex[positionInIndex] = columnName;
          indexesOnTable.put(indexTuple, orderedcolumnsForIndex);

          //Update the indexes for the table
          tableIndexMap.put(tableName, indexesOnTable);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }

    if (log.isDebugEnabled()) log.debug("Caching index metadata for schema [" + schemaName + "]");
    cacheIndexesAndPrimaryKeyColumns(tableIndexMap);
  }


  /**
   * Build, and cache, a multimap for the indexes and primary keys present on each table
   */
  private void cacheIndexesAndPrimaryKeyColumns(Map<String, Map<IndexTuple, String[]>> tableIndexMap) {
    ArrayListMultimap<String, Index> indexes = ArrayListMultimap.create();
    ArrayListMultimap<String, String> primaryKeys = ArrayListMultimap.create();

    //Build each multimap of primary keys and indexes
    tableIndexMap.entrySet().stream().forEach(
      e -> {
        primaryKeys.putAll(e.getKey(), primaryKeyBuilder(e));
        indexes.putAll(e.getKey(), indexBuilder(e));
      }
    );

    indexMetaData = indexes;
    primaryKeyMetaData = primaryKeys;
  }


  /**
   * For a given entry:
   * <br> * Exclude primary keys
   * <br> * Build the Index based on the column list and the IndexTuple
   */
  private List<Index> indexBuilder(Entry<String, Map<IndexTuple, String[]>> entry) {
    //An index type of 0 represents a primary key
    //and an index type of 1 is a unique key
    return entry.getValue()
      .entrySet()
      .stream()
      .filter(p -> p.getKey().indexType != 0)
      .map(p -> new IndexImpl(p.getKey().indexName, p.getKey().indexType == 1, Arrays.asList(p.getValue())))
      .collect(Collectors.toList());
  }


  /**
   * For a given entry:
   * <br> * Exclusively retain primary keys
   * <br> * Build the new primary key column list
   */
  private List<String> primaryKeyBuilder(Entry<String, Map<IndexTuple, String[]>> entry) {
    //An index type of 0 represents a primary key
    return entry.getValue()
      .entrySet()
      .stream()
      .filter(p -> p.getKey().indexType == 0)
      .flatMap(p -> stream(p.getValue()))
      .collect(Collectors.toList());
  }


  /**
   * Static class to hold information about index name
   * and its type.
   */
  private static final class IndexTuple {
    String indexName;
    int indexType;
    IndexTuple(String indexName, int indexType) {
      this.indexName = indexName;
      this.indexType = indexType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexName, indexType);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      IndexTuple other = (IndexTuple) obj;
      return Objects.equals(this.indexName, other.indexName)
          && Objects.equals(this.indexType, other.indexType);
    }
  }
}
