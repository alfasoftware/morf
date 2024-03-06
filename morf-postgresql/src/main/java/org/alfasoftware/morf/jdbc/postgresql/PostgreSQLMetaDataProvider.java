package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getDataTypeFromColumnComment;

import java.sql.*;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Suppliers;
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


  /**
   * @see DatabaseMetaDataProvider#getSequenceResultSet(String)
   */
  @Override
  public Map<AName, RealName> getSequenceResultSet(String schemaName) {
    final ImmutableMap.Builder<AName, RealName> sequenceNames = ImmutableMap.builder();

    log.info("Starting read of sequence definitions");

    long start = System.currentTimeMillis();

    StringBuilder sequenceSqlBuilder = new StringBuilder("SELECT S.relname FROM pg_class S LEFT JOIN pg_depend D ON " +
      "(S.oid = D.objid AND D.deptype = 'a') LEFT JOIN pg_namespace N on (N.oid = S.relnamespace) WHERE S.relkind = " +
      "'S' AND D.objid IS NULL");

    if (schemaName != null && !schemaName.isBlank()) {
      sequenceSqlBuilder.append(" AND N.nspname=?");
    }

    runSQL(sequenceSqlBuilder.toString(), schemaName, new ResultSetHandler() {
      @Override
      public void handle(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
         RealName realName = readSequenceName(resultSet);
         if (PostgreSQLMetaDataProvider.super.isSystemSequence(realName)) {
           continue;
         }
         sequenceNames.put(realName, realName);
        }
      }
    });

    long end = System.currentTimeMillis();

    Map<AName, RealName> sequenceNamesMap = sequenceNames.build();

    log.info(String.format("Read sequence metadata in %dms; %d sequences", end-start, sequenceNamesMap.size()));
    return sequenceNamesMap;
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
  private void runSQL(String sql, String schemaName, ResultSetHandler handler) {
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      try {

        // pass through the schema name
        if (schemaName != null && !schemaName.isBlank()) {
          statement.setString(1, schemaName);
        }

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

}
