package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getDataTypeFromColumnComment;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.shouldIgnoreIndex;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Provides meta data from a PostgreSQL database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class PostgreSQLMetaDataProvider extends DatabaseMetaDataProvider implements AdditionalMetadata {

  private static final Log log = LogFactory.getLog(PostgreSQLMetaDataProvider.class);

  private static final Pattern REALNAME_COMMENT_MATCHER = Pattern.compile(".*"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":\\[([^\\]]*)\\](/TYPE:\\[([^\\]]*)\\])?.*");

  private static final Pattern NULL_PREDICATE_COLUMN_MATCHER = Pattern.compile("\\\"?([A-Za-z_][A-Za-z0-9_]*)\\\"?\\s+IS\\s+NULL", Pattern.CASE_INSENSITIVE);

  private static final Pattern INDEX_COLUMNS_COMMENT_MATCHER = Pattern.compile(".*"+PostgreSQLDialect.INDEX_COLUMNS_COMMENT_LABEL+":\\[([^\\]]*)\\].*");

  private final Supplier<Map<AName, RealName>> allIndexNames = Suppliers.memoize(this::loadAllIndexNames);
  private final Supplier<Map<RealName, List<RealName>>> allPartialIndexDefinitionColumnNames = Suppliers.memoize(this::loadAllPartialIndexDefinitionColumnNames);
  private final Supplier<Map<String, List<Index>>> allIgnoredIndexes = Suppliers.memoize(this::loadIgnoredIndexes);
  private final Set<RealName> allIgnoredIndexesTables = new HashSet<>();

  public PostgreSQLMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  @Override
  protected boolean isPrimaryKeyIndex(RealName indexName) {
    return indexName.getDbName().endsWith("_pk");
  }


  @Override
  protected DataType dataTypeFromSqlType(int sqlType, String typeName, int width) {

    if (sqlType == Types.VARCHAR) {
      if (typeName.equals("text")) {
        return DataType.CLOB;
      }
      return super.dataTypeFromSqlType(sqlType, typeName, width);
    }

    return super.dataTypeFromSqlType(sqlType, typeName, width);
  }


  @Override
  protected ColumnBuilder setAdditionalColumnMetadata(RealName tableName, ColumnBuilder columnBuilder, ResultSet columnMetaData) throws SQLException {
    columnBuilder = super.setAdditionalColumnMetadata(tableName, columnBuilder, columnMetaData);

    // read autonumber from comments
    if (columnBuilder.isAutoNumbered()) {
      int startValue = getAutoIncrementStartValue(columnMetaData.getString(COLUMN_REMARKS));
      columnBuilder = columnBuilder.autoNumbered(startValue == -1 ? 1 : startValue);
    }

    // read datatype from comments
    Optional<String> dataTypeComment = getDataTypeFromColumnComment(columnMetaData.getString(COLUMN_REMARKS));
    if(dataTypeComment.isPresent() && dataTypeComment.get().equals("BIG_INTEGER")){
      columnBuilder = columnBuilder.dataType(DataType.BIG_INTEGER);
    }

    return columnBuilder;
  }


  @Override
  protected RealName readColumnName(ResultSet columnResultSet) throws SQLException {
    String columnName = columnResultSet.getString(COLUMN_NAME);
    String comment = columnResultSet.getString(COLUMN_REMARKS);
    String realName = matchComment(comment);
    return StringUtils.isNotBlank(realName)
        ? createRealName(columnName, realName)
        : super.readColumnName(columnResultSet);
  }


  @Override
  protected RealName readTableName(ResultSet tableResultSet) throws SQLException {
    String tableName = tableResultSet.getString(TABLE_NAME);
    String comment = tableResultSet.getString(TABLE_REMARKS);
    String realName = matchComment(comment);
    return StringUtils.isNotBlank(realName)
        ? createRealName(tableName, realName)
        : super.readTableName(tableResultSet);
  }


  @Override
  protected RealName readViewName(ResultSet viewResultSet) throws SQLException {
    String viewName = viewResultSet.getString(TABLE_NAME);
    String comment = viewResultSet.getString(TABLE_REMARKS);
    String realName = matchComment(comment);
    return StringUtils.isNotBlank(realName)
        ? createRealName(viewName, realName)
        : super.readViewName(viewResultSet);
  }

  @Override
  public Map<String, List<Index>> ignoredIndexes() {
    return allIgnoredIndexes.get();
  }


  private Map<String, List<Index>> loadIgnoredIndexes() {
    ImmutableMap<String, List<Index>> ignoredIndexes = ImmutableMap.of();
    // make sure allIgnoredIndexesTables is loaded.
    allIndexNames.get();
    if (!allIgnoredIndexesTables.isEmpty()) {
      ignoredIndexes =  loadAllIgnoredIndexes();
    }
    return ignoredIndexes;
  }


  private ImmutableMap<String, List<Index>> loadAllIgnoredIndexes() {
    ImmutableMap.Builder<String, List<Index>> ignoredIndexes = ImmutableMap.builder();
    for (RealName realTableName : allIgnoredIndexesTables) {
      ignoredIndexes.put(realTableName.getDbName().toLowerCase(), loadTableIndexes(realTableName, true));
    }
    return ignoredIndexes.build();
  }


  protected Map<AName, RealName> loadAllIndexNames() {
    final ImmutableMap.Builder<AName, RealName> indexNames = ImmutableMap.builder();

    String schema = StringUtils.isNotBlank(schemaName)
        ? " JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace AND n.nspname = '" + schemaName + "'"
        : "";

    String sql = "SELECT ci.relname AS indexName, d.description AS indexRemark, t.relname as tableName, td.description as tableRemark"
                + " FROM pg_catalog.pg_index i"
                + " JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid"
                + " JOIN pg_catalog.pg_class t ON t.oid = i.indrelid"
                + schema
                + " JOIN pg_description d ON d.objoid = ci.oid"
                + " JOIN pg_description td ON td.objoid = t.oid and td.objsubid=0";

    allIgnoredIndexesTables.clear();

    try (Statement createStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      try (ResultSet indexResultSet = createStatement.executeQuery(sql)) {
        while (indexResultSet.next()) {
          String indexName = indexResultSet.getString(1);
          String comment = indexResultSet.getString(2);
          String tableName = indexResultSet.getString(3);
          String tableRemark = indexResultSet.getString(4);
          String realName = matchComment(comment);
          String realTable = matchComment(tableRemark);

          if (log.isDebugEnabled()) {
            log.debug("Found index [" + indexName + "] with remark [" + comment + "] parsed as [" + realName + "] in schema [" + schemaName + "]");
          }

          if (StringUtils.isNotBlank(realName)) {
            RealName realIndexName = createRealName(indexName, realName);
            indexNames.put(realIndexName, realIndexName);

            if (shouldIgnoreIndex(realName)) {
              RealName realTableName = createRealName(tableName, realTable);
              allIgnoredIndexesTables.add(realTableName);
            }
          }
        }

        return indexNames.build();
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }


  @Override
  protected Map<RealName, List<RealName>> loadPartialIndexColumnNames(RealName tableName) {
    String schema = StringUtils.isNotBlank(schemaName)
        ? " JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace AND n.nspname = '" + schemaName + "'"
        : "";

    String sql = "SELECT ci.relname AS indexName, pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate"
                + " FROM pg_catalog.pg_index i"
                + " JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid"
                + " JOIN pg_catalog.pg_class t ON t.oid = i.indrelid"
                + schema
                + " WHERE i.indpred IS NOT NULL"
                + " AND t.relname = '" + tableName.getDbName() + "'";

    ImmutableMap.Builder<RealName, List<RealName>> partialIndexColumnNames = ImmutableMap.builder();
    try (Statement createStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      try (ResultSet indexResultSet = createStatement.executeQuery(sql)) {
        while (indexResultSet.next()) {
          RealName indexName = readIndexNameFromDatabaseName(indexResultSet.getString(1));
          partialIndexColumnNames.put(indexName, partialIndexColumnNamesFromPredicate(indexResultSet.getString(2)));
        }
      }
      return partialIndexColumnNames.build();
    }
    catch (SQLException e) {
      throw new RuntimeSqlException("Error reading partial index metadata for table [" + tableName + "]", e);
    }
  }


  private RealName readIndexNameFromDatabaseName(String indexName) {
    RealName readIndexName = createRealName(indexName, indexName);
    return allIndexNames.get().getOrDefault(readIndexName, readIndexName);
  }


  private List<RealName> partialIndexColumnNamesFromPredicate(String predicate) {
    ImmutableList.Builder<RealName> partialIndexColumnNames = ImmutableList.builder();
    Matcher matcher = NULL_PREDICATE_COLUMN_MATCHER.matcher(StringUtils.defaultString(predicate));
    while (matcher.find()) {
      String columnName = matcher.group(1);
      partialIndexColumnNames.add(createRealName(columnName, columnName));
    }
    return partialIndexColumnNames.build();
  }


  @Override
  protected List<RealName> columnNamesForIndexDefinition(RealName indexName, List<RealName> physicalColumnNames, List<RealName> partialIndexColumnNames) {
    if (partialIndexColumnNames.isEmpty()) {
      return physicalColumnNames;
    }

    List<RealName> storedColumnNames = allPartialIndexDefinitionColumnNames.get().get(indexName);
    if (storedColumnNames != null) {
      return storedColumnNames;
    }

    ImmutableList.Builder<RealName> fallbackColumnNames = ImmutableList.builder();
    fallbackColumnNames.addAll(physicalColumnNames);
    for (RealName partialIndexColumnName : partialIndexColumnNames) {
      if (!physicalColumnNames.contains(partialIndexColumnName)) {
        fallbackColumnNames.add(partialIndexColumnName);
      }
    }
    return fallbackColumnNames.build();
  }


  private Map<RealName, List<RealName>> loadAllPartialIndexDefinitionColumnNames() {
    String schema = StringUtils.isNotBlank(schemaName)
        ? " JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace AND n.nspname = '" + schemaName + "'"
        : "";

    String sql = "SELECT ci.relname AS indexName, d.description AS indexRemark"
                + " FROM pg_catalog.pg_index i"
                + " JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid"
                + schema
                + " LEFT JOIN pg_description d ON d.objoid = ci.oid AND d.objsubid = 0"
                + " WHERE i.indpred IS NOT NULL";

    ImmutableMap.Builder<RealName, List<RealName>> indexDefinitionColumnNames = ImmutableMap.builder();
    try (Statement createStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      try (ResultSet indexResultSet = createStatement.executeQuery(sql)) {
        while (indexResultSet.next()) {
          String comment = indexResultSet.getString(2);
          List<RealName> columnNames = indexDefinitionColumnNamesFromComment(comment);
          if (!columnNames.isEmpty()) {
            indexDefinitionColumnNames.put(readIndexNameFromDatabaseName(indexResultSet.getString(1)), columnNames);
          }
        }
      }
      return indexDefinitionColumnNames.build();
    }
    catch (SQLException e) {
      throw new RuntimeSqlException("Error reading partial index column-definition comments", e);
    }
  }


  private List<RealName> indexDefinitionColumnNamesFromComment(String comment) {
    if (StringUtils.isBlank(comment)) {
      return ImmutableList.of();
    }

    Matcher matcher = INDEX_COLUMNS_COMMENT_MATCHER.matcher(comment);
    if (!matcher.matches()) {
      return ImmutableList.of();
    }

    String[] columnNames = StringUtils.split(matcher.group(1), ",");
    if (columnNames == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<RealName> indexDefinitionColumnNames = ImmutableList.builder();
    for (String columnName : columnNames) {
      String trimmedColumnName = columnName.trim();
      if (StringUtils.isNotBlank(trimmedColumnName)) {
        indexDefinitionColumnNames.add(createRealName(trimmedColumnName, trimmedColumnName));
      }
    }
    return indexDefinitionColumnNames.build();
  }


  @Override
  protected RealName readIndexName(ResultSet indexResultSet) throws SQLException {
    RealName readIndexName = super.readIndexName(indexResultSet);
    return allIndexNames.get().getOrDefault(readIndexName, readIndexName);
  }


  private String matchComment(String comment) {
    if (StringUtils.isNotBlank(comment)) {
      Matcher matcher = REALNAME_COMMENT_MATCHER.matcher(comment);
      if (matcher.matches()) {
        return matcher.group(1);
      }
    }
    return null;
  }


  /**
   * @see DatabaseMetaDataProvider#buildSequenceSql(String)
   */
  @Override
  protected String buildSequenceSql(String schemaName) {
    StringBuilder sequenceSqlBuilder = new StringBuilder("SELECT S.relname FROM pg_class S LEFT JOIN pg_depend D ON " +
      "(S.oid = D.objid AND D.deptype = 'a') LEFT JOIN pg_namespace N on (N.oid = S.relnamespace) WHERE S.relkind = " +
      "'S' AND D.objid IS NULL");

    if (schemaName != null && !schemaName.isBlank()) {
      sequenceSqlBuilder.append(" AND N.nspname=?");
    }

    return sequenceSqlBuilder.toString();
  }
}
