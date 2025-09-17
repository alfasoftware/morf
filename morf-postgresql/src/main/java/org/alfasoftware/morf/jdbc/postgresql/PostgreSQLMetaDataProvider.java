package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getAutoIncrementStartValue;
import static org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils.getDataTypeFromColumnComment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Partition;
import org.alfasoftware.morf.metadata.PartitioningRuleType;
import org.alfasoftware.morf.metadata.Partitions;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.alfasoftware.morf.metadata.Table;
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
public class PostgreSQLMetaDataProvider extends DatabaseMetaDataProvider implements AdditionalMetadata {

  private static final Log log = LogFactory.getLog(PostgreSQLMetaDataProvider.class);

  private static final Pattern REALNAME_COMMENT_MATCHER = Pattern.compile(".*"+PostgreSQLDialect.REAL_NAME_COMMENT_LABEL+":\\[([^\\]]*)\\](/TYPE:\\[([^\\]]*)\\])?.*");

  private final Supplier<Map<AName, RealName>> allIndexNames = Suppliers.memoize(this::loadAllIndexNames);

  public PostgreSQLMetaDataProvider(Connection connection, String schemaName) {
    super(connection, schemaName);
  }


  @Override
  protected Set<String> getIgnoredTables() {
    Set<String> ignoredTables = new HashSet<>();
    try(Statement ignoredTablesStmt = connection.createStatement()) {
      try (ResultSet ignoredTablesRs = ignoredTablesStmt.executeQuery("select relname from pg_class where relispartition and relkind = 'r'")) {
        while (ignoredTablesRs.next()) {
          ignoredTables.add(ignoredTablesRs.getString(1).toLowerCase(Locale.ROOT));
        }
      }
    } catch (SQLException e) {
        // ignore exception, if it fails then incompatible Postgres version
    }
    return ignoredTables;
  }

  @Override
  protected boolean isIgnoredTable(@SuppressWarnings("unused") RealName tableName) {
    return ignoredTables.get().contains(tableName.getDbName().toLowerCase(Locale.ROOT));
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
  protected Partitions loadTablePartitions(RealName realTableName) {
    SchemaUtils.PartitionsBuilder partitions = null;
    final ImmutableMap.Builder<AName, List<RealName>> indexNames = ImmutableMap.builder();
    Table table = null;
    Column partitionColumn = null;
    List<Partition> partitionList = new ArrayList<>();

      String schema = StringUtils.isNotBlank(schemaName)
      ? " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = '" + schemaName + "'"
      : "";

    String sql = "SELECT t.relname AS tableName,"
      + " c.relname AS partitionTable, pg_get_expr(c.relpartbound, i.inhrelid) as partitionClause"
      + " FROM pg_catalog.pg_inherits i"
      + " JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid"
      + " JOIN pg_catalog.pg_class t ON t.oid = i.inhparent"
      + schema
      + " WHERE t.relname = ?"
      + " ORDER BY t.relname";

    String sqlForColumnName = "select par.relname as tableName, d.description, pt.partnatts as numColumns, pt.partstrat, col.column_name"
      + " from (select partrelid, partnatts, partstrat, unnest(partattrs) column_index"
      + "  from pg_partitioned_table) pt"
      + " join pg_class par on par.oid = pt.partrelid"
      + " join pg_namespace n on n.oid = par.relnamespace"
      + " JOIN pg_description d ON d.objoid = par.oid"
      + " join information_schema.columns col on col.table_schema = n.nspname"
      + " and col.table_name = par.relname and ordinal_position = pt.column_index"
      + " WHERE par.relname = ?";


    try (PreparedStatement preparedStatementColumn = connection.prepareStatement(sqlForColumnName, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      preparedStatementColumn.setString(1, realTableName.getDbName());
      try (ResultSet partitionsResultSet = preparedStatementColumn.executeQuery()) {
        if (partitionsResultSet.next()) {
          String tableName = partitionsResultSet.getString(1);
          int numColumns = partitionsResultSet.getInt(3);
          String partitionStrategy = partitionsResultSet.getString(4);
          String column = partitionsResultSet.getString(5);
          String comment = partitionsResultSet.getString(2);
          String realName = matchComment(comment);

          if (log.isDebugEnabled()) {
              log.debug("Found partitioned table [" + tableName + "] with remark [" + comment + "] parsed as [" + realName + "] in schema [" + schemaName + "]");
          }

          if (numColumns > 1) {
            log.info("morf doesn't support multiple columns on partition yet");
          } else {
            table = getTable(tableName);
            partitionColumn = table.columns().stream().filter(column1 ->
              column1.getName().equals(column)).findFirst().orElse(null);

            if (partitionColumn != null) {
              partitions = SchemaUtils.partitions().column(partitionColumn);
              switch(partitionStrategy) {
                case "r":
                  partitions = partitions.ruleType(PartitioningRuleType.rangePartitioning);
                break;
                case "h": partitions = partitions.ruleType(PartitioningRuleType.hashPartitioning);
                break;
                case "l": partitions = partitions.ruleType(PartitioningRuleType.listPartitioning);
                break;
              }
            }
          }
        }
      }
    }
    catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }

    if (partitions != null) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        preparedStatement.setString(1, realTableName.getDbName());

        try (ResultSet partitionsResultSet = preparedStatement.executeQuery()) {
          while (partitionsResultSet.next()) {
            String tableName = partitionsResultSet.getString(1);
            String partitionName = partitionsResultSet.getString(3);
            String partitionClause = partitionsResultSet.getString(4);
            String comment = partitionsResultSet.getString(2);
            String realName = matchComment(comment);

            if (StringUtils.isNotBlank(partitionName)) {
              RealName partitionRealName = createRealName(partitionName, partitionName);
              Partition partition = null;

              switch (partitions.partitioningType()) {
                case rangePartitioning:
                  String[] asValuesRange = partitionClause.split("'");
                  String start = asValuesRange[1];
                  String end = asValuesRange[3];
                  partition = SchemaUtils.partitionByRange(partitionName)
                    .start(start).end(end);
                  break;
                case hashPartitioning:
                  String[] asValuesHash = partitionClause.split(Pattern.quote("("));
                  if (!asValuesHash[1].startsWith("modulus")) {
                    log.info("morf doesn't support a function other than modulus on partition yet");
                  } else {
                    String[] values = asValuesHash[1].replace("modulus ", "").split(",");
                    String divider = values[0];
                    String remainder = values[1].replace(" ", "").replace(")", "");
                    partition = SchemaUtils.partitionByHash(partitionName)
                      .divider(divider).remainder(remainder);
                  }
                  break;
                case listPartitioning:
                  break;
                default:
                  break;
              }

              if (partition != null) {
                partitionList.add(partition);
              }
            }
          }

          /*
          if (partitionColumn.getType().equals(Types.DATE) && partitions.partitioningType().equals(PartitioningRuleType.rangePartitioning)) {
            List<Pair<LocalDate, LocalDate>> partitionRanges = new ArrayList<>();
            for (Partition partition : partitionList) {
              PartitionByRange rangePartition = (PartitionByRange)partition;
              partitionRanges.add(Pair.of(LocalDate.parse(rangePartition.start()), LocalDate.parse(rangePartition.end())));
            }

            partitions = partitions.partitioningRule(new DatePartitionedByPeriodRule(partitionColumn.getName(), partitionRanges));
          }*/

          return partitions.partitions(partitionList);
        }
      } catch (SQLException e) {
        throw new RuntimeSqlException(e);
      }
    }

    return partitions;
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
