package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getDataTypeFromColumnComment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.postgresql.PostgreSQLUniqueIndexAdditionalDeploymentStatements.AdditionalIndexInfo;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Provides meta data from a PostgreSQL database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class PostgreSQLMetaDataProvider extends DatabaseMetaDataProvider {

  private static final Log log = LogFactory.getLog(PostgreSQLMetaDataProvider.class);

  private static final Pattern REALNAME_COMMENT_MATCHER = Pattern.compile(".*"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":\\[([^\\]]*)\\](/TYPE:\\[([^\\]]*)\\])?.*");

  private final Supplier<Map<AName, RealName>> allIndexNames = Suppliers.memoize(this::loadAllIndexNames);

  private final HashMultimap<String, AdditionalIndexInfo> additionalConstraintIndexes = HashMultimap.create();

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


  protected Map<AName, RealName> loadAllIndexNames() {
    final ImmutableMap.Builder<AName, RealName> indexNames = ImmutableMap.builder();

    String schema = StringUtils.isNotBlank(schemaName)
        ? " JOIN pg_catalog.pg_namespace n ON n.oid = ci.relnamespace AND n.nspname = '" + schemaName + "'"
        : "";

    String sql = "SELECT ci.relname AS indexName, d.description AS indexRemark"
                + " FROM pg_catalog.pg_index i"
                + " JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid"
                + schema
                + " JOIN pg_description d ON d.objoid = ci.oid";

    try (Statement createStatement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      try (ResultSet indexResultSet = createStatement.executeQuery(sql)) {
        while (indexResultSet.next()) {
          String indexName = indexResultSet.getString(1);
          String comment = indexResultSet.getString(2);
          String realName = matchComment(comment);

          if (log.isDebugEnabled()) {
            log.debug("Found index [" + indexName + "] with remark [" + comment + "] parsed as [" + realName + "] in schema [" + schemaName + "]");
          }

          if (StringUtils.isNotBlank(realName)) {
            RealName realIndexName = createRealName(indexName, realName);
            indexNames.put(realIndexName, realIndexName);
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


  @Override
  protected void ignoreIndexName(RealName indexName, ResultSet indexResultSet) throws SQLException {
    super.ignoreIndexName(indexName, indexResultSet);

    PostgreSQLUniqueIndexAdditionalDeploymentStatements.matchAdditionalIndex(indexName.getDbName(), indexName.getRealName())
      .ifPresent(additionalIndexInfo -> {
        if (log.isDebugEnabled()) {
          log.debug("Accepted index [" + indexName + "] as supporting constraint for index [" + additionalIndexInfo.getBaseName() + "]");
        }
        additionalConstraintIndexes.put(additionalIndexInfo.getBaseName().toLowerCase(), additionalIndexInfo);
      });
  }


  /**
   * For a given unique constraint index base name, returns a collection of {@link AdditionalIndexInfo},
   * a pre-parsed POJOs describing ignored technical indexes recorded for the given constraint base name.
   *
   * Important: The {@link AdditionalIndexInfo} are gathered when reading the metadata of the underlying
   * index. Therefore, this method might return an empty collection, if called too soon, before metadata
   * of the index has been read.
   *
   * @param baseIndexName Name of the underlying unique index.
   * @return Collection of additional technical indexes belonging to the underlying unique index.
   */
  public Collection<AdditionalIndexInfo> getAdditionalConstraintIndexes(String baseIndexName) {
    Set<AdditionalIndexInfo> infos = additionalConstraintIndexes.get(baseIndexName);
    return infos == null ? ImmutableList.of() : infos;
  }
}
