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

package org.alfasoftware.morf.jdbc.mysql;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.tryFind;
import static org.alfasoftware.morf.metadata.DataType.INTEGER;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.AbstractSelectStatement;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements database specific statement generation for MySQL.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class MySqlDialect extends SqlDialect {

  private static final long AUTONUMBER_LIMIT = 1000;

  /**
   * Default constructor.
   */
  public MySqlDialect() {
    super(""); // no schema name needed for MySQL.
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> internalTableDeploymentStatements(Table table) {
    List<String> statements = new ArrayList<>();

    // Create the table deployment statement
    StringBuilder createTableStatement = new StringBuilder();
    createTableStatement.append("CREATE ");

    if (table.isTemporary()) {
      createTableStatement.append("TEMPORARY ");
    }

    createTableStatement.append("TABLE `");
    createTableStatement.append(table.getName());
    createTableStatement.append("` (");

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;
    Column autoIncrementColumn = null;
    int autoNumberStart = -1;

    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }

      createTableStatement.append("`");
      createTableStatement.append(column.getName());
      createTableStatement.append("` ");
      createTableStatement.append(sqlRepresentationOfColumnType(column));
      if (column.isAutoNumbered()) {
        autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        createTableStatement.append(" AUTO_INCREMENT COMMENT 'AUTONUMSTART:[" + autoNumberStart + "]'");
        autoIncrementColumn = column;
      }

      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      first = false;
    }
    // Put on the primary key constraint
    if (!primaryKeys.isEmpty()) {
      createTableStatement
        .append(", ")
        .append(buildPrimaryKeyConstraint(table));
    }
    createTableStatement.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");

    if (autoIncrementColumn != null && autoIncrementColumn.getAutoNumberStart() != 0) {
      createTableStatement.append(" AUTO_INCREMENT=" + autoNumberStart);
    }

    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * CONSTRAINT TABLENAME_PK PRIMARY KEY (`X`, `Y`, `Z`)
   */
  private String buildPrimaryKeyConstraint(Table table) {
    return buildPrimaryKeyConstraint(table.getName(), namesOfColumns(primaryKeysForTable(table)));
  }

  /**
   * CONSTRAINT TABLENAME_PK PRIMARY KEY (`X`, `Y`, `Z`)
   */
  private String buildPrimaryKeyConstraint(String tableName, List<String> primaryKeyColumns) {
    return new StringBuilder()
    .append("CONSTRAINT `")
    .append(tableName)
    .append("_PK` ")
    .append("PRIMARY KEY (`")
    .append(Joiner.on("`, `").join(primaryKeyColumns))
    .append("`)").toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    // We use CHANGE, not ALTER on MySQL
    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE `")
      .append(table.getName())
      .append("` ")
      .append("ADD ")
      .append('`')
      .append(column.getName())
      .append('`')
      .append(' ')
      .append(sqlRepresentationOfColumnType(column));

    result.add(statement.toString());

    if (column.isPrimaryKey()) {
      StringBuilder primaryKeyStatement = new StringBuilder()
        .append("ALTER TABLE `")
        .append(table.getName())
        .append("` ADD ")
        .append(buildPrimaryKeyConstraint(table));
      result.add(primaryKeyStatement.toString());
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();

    // build the old version of the table
    Table oldTable = oldTableForChangeColumn(table, oldColumn, newColumn);

    // recreate the PK if there's any change in the PK
    boolean recreatePrimaryKey = oldColumn.isPrimaryKey() != newColumn.isPrimaryKey();

    // drop the existing PK if there is one, and we're changing it
    if (recreatePrimaryKey && !primaryKeysForTable(oldTable).isEmpty()) {
      result.add(dropPrimaryKey(oldTable));
    }

    result.add(
      "ALTER TABLE `" + table.getName() + "` CHANGE `" + oldColumn.getName() + "` `" + newColumn.getName() + "` " + sqlRepresentationOfColumnType(newColumn)
    );

    // Put the PK back if there is one, and we're changing it
    if (recreatePrimaryKey && !primaryKeysForTable(table).isEmpty()) {
      result.add(new StringBuilder()
        .append("ALTER TABLE `")
        .append(table.getName())
        .append("` ADD ")
        .append(buildPrimaryKeyConstraint(table)).toString());
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    StringBuilder statement = new StringBuilder().append("ALTER TABLE `").append(table.getName()).append("` ")
        .append("DROP").append(' ');

    statement.append('`').append(column.getName()).append('`');

    result.add(statement.toString());

    return result;
  }


  /**
   * ALTER TABLE `XYZ` DROP PRIMARY KEY
   */
  private String dropPrimaryKey(Table table) {
    return dropPrimaryKey(table.getName());
  }


  /**
   * ALTER TABLE `XYZ` DROP PRIMARY KEY
   */
  private String dropPrimaryKey(String tableName) {
    return "ALTER TABLE `" + tableName + "` DROP PRIMARY KEY";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    return Arrays.asList(
      "FLUSH TABLES " + table.getName(),
      "DROP TABLE " + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.View)
   */
  @Override
  public Collection<String> dropStatements(View view) {
    return Arrays.asList("DROP VIEW IF EXISTS " + view.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#truncateTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> truncateTableStatements(Table table) {
    return Arrays.asList("TRUNCATE " + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#deleteAllFromTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> deleteAllFromTableStatements(Table table) {
    return Arrays.asList("delete from " + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#postInsertWithPresetAutonumStatements(org.alfasoftware.morf.metadata.Table, boolean)
   */
  @Override
  public void postInsertWithPresetAutonumStatements(Table table, SqlScriptExecutor executor,Connection connection, boolean insertingUnderAutonumLimit) {
    repairAutoNumberStartPosition(table,executor,connection);
  }


 /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#repairAutoNumberStartPosition(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public void repairAutoNumberStartPosition(Table table, SqlScriptExecutor executor,Connection connection) {
    Column autoIncrementColumn = getAutoIncrementColumnForTable(table);

    if (autoIncrementColumn == null) {
      executor.execute(updateStatisticsStatement(table), connection);
      return;
    }

    long maxId = executor
                .executeQuery(checkMaxIdAutonumberStatement(table,autoIncrementColumn))
                .withConnection(connection)
                .processWith(new ResultSetProcessor<Long>() {
                  @Override
                  public Long process(ResultSet resultSet) throws SQLException {
                    if (!resultSet.next()) {
                      throw new UnsupportedOperationException("Nothing returned by results set");
                    }

                    return resultSet.getLong(1);
                  }
                });

    // We reset the auto increment seed to our start value every time we bulk insert data.  If the max value
    // on the table is greater, mySQL will just use that instead
    Collection<String> repairStatements = maxId < AUTONUMBER_LIMIT ?
                                                    ImmutableList.of(alterAutoincrementStatement(table,autoIncrementColumn),updateStatisticsStatement(table)) :
                                                    ImmutableList.of(updateStatisticsStatement(table));

    executor.execute(repairStatements,connection);
  }


  /**
   * Returns a statement which will update the statistics for a specific table.
   */
  private String alterAutoincrementStatement(Table table,Column autoIncrementColumn) {
    return "ALTER TABLE " + table.getName() + " AUTO_INCREMENT = " + autoIncrementColumn.getAutoNumberStart();
  }


  /**
   * Returns a statement which will update the statistics for a specific table.
   */
  private String updateStatisticsStatement(Table table) {
    return "ANALYZE TABLE " + table.getName();
  }


  /**
   * Returns a statement which will check that the max id value on the table is less than the autonumber start value
   */
  private String checkMaxIdAutonumberStatement(Table table,Column autoIncrementColumn) {
    return "SELECT MAX(" + autoIncrementColumn.getName()+") FROM "+table.getName();
  }


  /**
   * MySQL defaults to <a href="http://stackoverflow.com/questions/20496616/fetchsize-in-resultset-set-to-0-by-default">fetching
   * <em>all</em> records</a> into memory when a JDBC query is executed, which causes OOM
   * errors when used with large data sets (Cryo and ETLs being prime offenders). Ideally
   * we would use a nice big paging size here (like 200 as used in {@link OracleDialect})
   * but as noted in the link above, MySQL only supports one record at a time or all at
   * once, with nothing in between.  As a result, we default to one record for bulk loads
   * as the only safe choice.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#fetchSizeForBulkSelects()
   */
  @Override
  public int fetchSizeForBulkSelects() {
    return Integer.MIN_VALUE;
  }


  /**
   * MySQL doesn't permit a open connection to be used for anything else while using a streaming
   * {@link ResultSet}, so if we know it will be, we disable streaming entirely. This has obvious
   * memory implications for large data sets, so bulk loads should generally open new transactions
   * inside the loop iterating the result set, which implicitly opens separate connections, allowing
   * {@link Integer#MIN_VALUE} to be used instead.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()
   */
  @Override
  public int fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming() {
    return Integer.MAX_VALUE;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getColumnRepresentation(org.alfasoftware.morf.metadata.DataType,
   *      int, int)
   */
  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        return String.format("VARCHAR(%d)", width);

      case DECIMAL:
        return String.format("DECIMAL(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        // See http://www.xaprb.com/blog/2006/04/11/bit-values-in-mysql/
        return "TINYINT(1)";

      case INTEGER:
        return "INTEGER";

      case BIG_INTEGER:
        return "BIGINT";

      case BLOB:
        return "LONGBLOB";

      case CLOB:
        return "LONGTEXT";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#prepareBooleanParameter(org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement, java.lang.Boolean, org.alfasoftware.morf.sql.element.SqlParameter)
   */
  @Override
  protected void prepareBooleanParameter(NamedParameterPreparedStatement statement, Boolean boolVal, SqlParameter parameter) throws SQLException {
    Integer intValue = boolVal == null ? null : boolVal ? 1 : 0;
    super.prepareIntegerParameter(statement, intValue, parameter(parameter.getImpliedName()).type(INTEGER));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#connectionTestStatement()
   */
  @Override
  public String connectionTestStatement() {
    return "select 1";
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getDatabaseType()
   */
  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(MySql.IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(ConcatenatedField)
   */
  @Override
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      sql.add(getSqlFrom(field));
    }
    // Using "_WithSeparator" but not passing a separator because MySQL vanilla CONCAT doesn't support null arguments properly
    return "CONCAT_WS('', " + StringUtils.join(sql, ", ") + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForIsNull(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForIsNull(Function function) {
    return "COALESCE(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ") ";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForOrderByField(org.alfasoftware.morf.sql.element.FieldReference)
   */
  @Override
  protected String getSqlForOrderByField(FieldReference orderByField) {
    StringBuilder result = new StringBuilder();
    String sqlFromField = getSqlFrom(orderByField);

    if (orderByField.getNullValueHandling().isPresent()) {
      switch (orderByField.getNullValueHandling().get()) {
        case FIRST:
          result.append("-ISNULL(").append(sqlFromField).append("), ");
          break;
        case LAST:
          result.append("ISNULL(").append(sqlFromField).append("), ");
          break;
        case NONE:
        default:
          break;
      }
    }

    result.append(sqlFromField);

    switch (orderByField.getDirection()) {
      case DESCENDING:
        result.append(" DESC");
        break;
      case ASCENDING:
      case NONE:
      default:
        break;
    }

    return result.toString().trim();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#buildAutonumberUpdate(org.alfasoftware.morf.sql.element.TableReference, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public List<String> buildAutonumberUpdate(TableReference dataTable, String fieldName, String idTableName, String nameColumn,
      String valueColumn) {
    List<String> sql = new LinkedList<>();

    String existingSelect = getExistingMaxAutoNumberValue(dataTable, fieldName);
    String tableName = getAutoNumberName(dataTable.getName());

    if (tableName.equals("autonumber")) {
      return sql;
    }

    sql.add(String.format("INSERT IGNORE INTO %s%s (%s, %s) VALUES('%s', 1)", schemaNamePrefix(), idTableName, nameColumn, valueColumn, tableName));
    sql.add(String.format("UPDATE %s%s set %s = (%s) where %s = '%s' and %s < (%s) and (%s) <> 1", schemaNamePrefix(), idTableName, valueColumn, existingSelect, nameColumn, tableName, valueColumn, existingSelect, existingSelect));

    return sql;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    StringBuilder statement = new StringBuilder();

    statement.append("ALTER TABLE `")
             .append(table.getName())
             .append("` DROP INDEX `")
             .append(indexToBeRemoved.getName())
             .append("`");

    return Arrays.asList(statement.toString());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public String indexDeploymentStatement(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("ALTER TABLE `");
    statement.append(table.getName());
    statement.append("` ADD ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX `")
             .append(index.getName())
             .append("` (`")
             .append(Joiner.on("`, `").join(index.columnNames()))
             .append("`)");

    return statement.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(java.lang.String, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    ArrayList<String> result = Lists.newArrayList();

    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add(dropPrimaryKey(table.getName()));
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(new StringBuilder()
        .append("ALTER TABLE `")
        .append(table.getName())
        .append("` ADD ")
        .append(buildPrimaryKeyConstraint(table.getName(), newPrimaryKeyColumns)).toString());
    }

    return result;
  }


  /**
   * Casting to BIGINT is not supported by MySQL at the moment as per
   * <a href="http://bugs.mysql.com/bug.php?id=26130"> this bug</a>. This method
   * skips the casting if the field type is BIGINT otherwise, it proceeds as
   * normal. See WEB-15027 for details.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.Cast)
   */
  @Override
  protected String getSqlFrom(Cast cast) {
    if (cast.getDataType() == DataType.BIG_INTEGER) {
      return getSqlFrom(cast.getExpression());
    } else if (cast.getDataType() == DataType.STRING) {
      // MySQL doesn't permit cast as VARCHAR - http://dev.mysql.com/doc/refman/5.0/en/cast-functions.html
      return String.format("CAST(%s AS CHAR(%d))", getSqlFrom(cast.getExpression()), cast.getWidth());
    } else if (cast.getDataType() == DataType.INTEGER) {
      // MySQL doesn't permit cast as INTEGER - http://dev.mysql.com/doc/refman/5.0/en/cast-functions.html
      return String.format("CAST(%s AS SIGNED)", getSqlFrom(cast.getExpression()));
    } else {
     return super.getSqlFrom(cast);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    return "DATE(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }



  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    return String.format("DATE_FORMAT(%s, '%%Y%%m%%d')",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    return String.format("DATE_FORMAT(%s, '%%Y%%m%%d%%H%%i%%s')",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "NOW()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return "TO_DAYS("  + getSqlFrom(toDate) + ") - TO_DAYS("+ getSqlFrom(fromDate) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForMonthsBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate) {
    String toDateStr = getSqlFrom(toDate);
    String fromDateStr = getSqlFrom(fromDate);
    return String.format(
       "CASE " +
        "WHEN %s = %s THEN 0 " +
        "ELSE " +
         "PERIOD_DIFF(EXTRACT(YEAR_MONTH FROM %s), EXTRACT(YEAR_MONTH FROM %s)) + " +
         "CASE " +
          "WHEN %s > %s THEN " +
            "CASE " +
             "WHEN DAY(%s) <= DAY(%s) OR %s = LAST_DAY(%s) THEN 0 " +
             "ELSE -1 " +
            "END " +
          "ELSE " +
            "CASE " +
             "WHEN DAY(%s) <= DAY(%s) OR %s = LAST_DAY(%s) THEN 0 " +
             "ELSE 1 " +
            "END " +
         "END " +
       "END ",
       fromDateStr, toDateStr,
       toDateStr, fromDateStr,
       toDateStr, fromDateStr,
       fromDateStr, toDateStr, toDateStr, toDateStr,
       toDateStr, fromDateStr, fromDateStr, fromDateStr
    );
  }


  /**
   * For MySQL, we need to alter the way we render a date literal in a default clause: We need to suppress the "DATE" prefix.
   */
  @Override
  protected String sqlForDefaultClauseLiteral(Column column) {
    if (column.getType() != DataType.DATE) {
      return super.sqlForDefaultClauseLiteral(column);
    }

    // suppress the "DATE" prefix for MySQL, just output the date part directly
    return String.format("'%s'", column.getDefaultValue());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#leftTrim(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String leftTrim(Function function) {
    return "LTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#rightTrim(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String rightTrim(Function function) {
    return "RTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddDays(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddDays(Function function) {
    return String.format(
      "DATE_ADD(%s, INTERVAL %s DAY)",
      getSqlFrom(function.getArguments().get(0)),
      getSqlFrom(function.getArguments().get(1))
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddMonths(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddMonths(Function function) {
    return String.format(
      "DATE_ADD(%s, INTERVAL %s MONTH)",
      getSqlFrom(function.getArguments().get(0)),
      getSqlFrom(function.getArguments().get(1))
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameTableStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {
    return Collections.singletonList("RENAME TABLE " + from.getName() + " TO " + to.getName());
  }


  /**
   * Backslashes in MySQL denote escape sequences and have to themselves be escaped.
   *
   * @see http://dev.mysql.com/doc/refman/5.0/en/string-literals.html
   * @see org.alfasoftware.morf.jdbc.SqlDialect#makeStringLiteral(java.lang.String)
   */
  @Override
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }
    return String.format("'%s'", StringUtils.replace(StringEscapeUtils.escapeSql(literalValue), "\\", "\\\\"));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.MergeStatement)
   */
  @Override
  protected String getSqlFrom(final MergeStatement statement) {

    if (StringUtils.isBlank(statement.getTable().getName())) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    checkSelectStatementHasNoHints(statement.getSelectStatement(), "MERGE may not be used with SELECT statement hints");

    final String destinationTableName = statement.getTable().getName();

    // Add the preamble
    StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");

    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);
    sqlBuilder.append("(");
    Iterable<String> intoFields = Iterables.transform(statement.getSelectStatement().getFields(), new com.google.common.base.Function<AliasedField, String>() {
      @Override
      public String apply(AliasedField field) {
        return field.getImpliedName();
      }
    });
    sqlBuilder.append(Joiner.on(", ").join(intoFields));
    sqlBuilder.append(") ");

    // Add select statement
    sqlBuilder.append(getSqlFrom(statement.getSelectStatement()));

    // Note that we use the source select statement's fields here as we assume that they are appropriately
    // aliased to match the target table as part of the API contract (it's needed for other dialects)
    sqlBuilder.append(" ON DUPLICATE KEY UPDATE ");
    Iterable<String> setStatements = Iterables.transform(statement.getSelectStatement().getFields(), new com.google.common.base.Function<AliasedField, String>() {
      @Override
      public String apply(AliasedField field) {
      return String.format("%s = values(%s)", field.getImpliedName(), field.getImpliedName());
      }
    });
    sqlBuilder.append(Joiner.on(", ").join(setStatements));
    return sqlBuilder.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameIndexStatements(org.alfasoftware.morf.metadata.Table,
   *      java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameIndexStatements(final Table table, final String fromIndexName, final String toIndexName) {
    Index newIndex, existingIndex;

    try {
      newIndex = Iterables.find(table.indexes(), new Predicate<Index>() {
        @Override public boolean apply(Index input) {
          return input.getName().equals(toIndexName);
        }
      });

      existingIndex = newIndex.isUnique()
        ? index(fromIndexName).columns(newIndex.columnNames()).unique()
        : index(fromIndexName).columns(newIndex.columnNames());
    } catch (NoSuchElementException nsee) {
      // If the index wasn't found, we must have the old schema instead of the
      // new one so try the other way round
      existingIndex = Iterables.find(table.indexes(), new Predicate<Index>() {
        @Override public boolean apply(Index input) {
          return input.getName().equals(fromIndexName);
        }
      });

      newIndex = existingIndex.isUnique()
        ? index(toIndexName).columns(existingIndex.columnNames()).unique()
        : index(toIndexName).columns(existingIndex.columnNames());
    }

    return ImmutableList.<String>builder()
      .addAll(indexDropStatements(table, existingIndex))
      .add(indexDeploymentStatement(table, newIndex))
      .build();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return  String.format("SUBSTRING(MD5(RAND()), 1, %s)",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#rebuildTriggers(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> rebuildTriggers(Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#supportsWindowFunctions()
   */
  @Override
  public boolean supportsWindowFunctions() {
    return false;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.WindowFunction)
   */
  @Override
  protected String getSqlFrom(final WindowFunction windowFunctionField) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName()+" does not support window functions.");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#likeEscapeSuffix()
   */
  @Override
  protected String likeEscapeSuffix() {
    return ""; // On MySql the escape character is \ by default. We don't need to set it, and setting it appears to be challenging anyway as it is a general escape char.
  }




  /**
   * If using {@link SelectStatement#useImplicitJoinOrder()}, we switch inner joins to STRAIGHT_JOINs.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#innerJoinKeyword(org.alfasoftware.morf.sql.AbstractSelectStatement)
   */
  @Override
  protected String innerJoinKeyword(AbstractSelectStatement<?> stmt) {
    if (stmt instanceof SelectStatement) {
      List<Hint> hints = ((SelectStatement)stmt).getHints();
      if (tryFind(hints, instanceOf(UseImplicitJoinOrder.class)).isPresent()) {
        return "STRAIGHT_JOIN";
      }
    }
    return super.innerJoinKeyword(stmt);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "LAST_DAY(" + getSqlFrom(date) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect.getSqlForAnalyseTable(Table)
   */
  @Override
  public Collection<String> getSqlForAnalyseTable(Table table) {
    return SqlDialect.NO_STATEMENTS;
  }
}
