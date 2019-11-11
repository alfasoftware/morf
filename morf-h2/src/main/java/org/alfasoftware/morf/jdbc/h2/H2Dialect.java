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

package org.alfasoftware.morf.jdbc.h2;

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Implements database specific statement generation for MySQL version 5.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class H2Dialect extends SqlDialect {

  /**
   * The prefix to add to all temporary tables.
   */
  public static final String TEMPORARY_TABLE_PREFIX = "TEMP_";

  private static final Pattern MERGE_SOURCE_REPLACE_PATTERN = Pattern.compile("USING (.*?\\) " + MERGE_SOURCE_ALIAS + ")");

  /**
   * @param h2
   *
   */
  public H2Dialect() {
    super(null);
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

    createTableStatement.append("TABLE ");
    createTableStatement.append(table.getName());
    createTableStatement.append(" (");

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }
      createTableStatement.append(column.getName() + " ");
      createTableStatement.append(sqlRepresentationOfColumnType(column));
      if (column.isAutoNumbered()) {
        int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        createTableStatement.append(" AUTO_INCREMENT(" + autoNumberStart + ") COMMENT 'AUTONUMSTART:[" + autoNumberStart + "]'");
      }

      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      first = false;
    }

    if (!primaryKeys.isEmpty()) {
      createTableStatement.append(", CONSTRAINT ");
      createTableStatement.append(table.getName());
      createTableStatement.append("_PK PRIMARY KEY (");
      createTableStatement.append(Joiner.on(", ").join(primaryKeys));
      createTableStatement.append(")");
    }

    createTableStatement.append(")");

    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    return Arrays.asList("drop table " + table.getName() + " cascade");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#truncateTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> truncateTableStatements(Table table) {
    return Arrays.asList("truncate table " + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#deleteAllFromTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> deleteAllFromTableStatements(Table table) {
    return Arrays.asList("delete from " + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getColumnRepresentation(org.alfasoftware.morf.metadata.DataType,
   *      int, int)
   */
  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        return width == 0 ? "VARCHAR" : String.format("VARCHAR(%d)", width);

      case DECIMAL:
        return width == 0 ? "DECIMAL" : String.format("DECIMAL(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        return "BIT";

      case BIG_INTEGER:
        return "BIGINT";

      case INTEGER:
        return "INTEGER";

      case BLOB:
        return "LONGVARBINARY";

      case CLOB:
        return "NCLOB";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#useInsertBatching()
   */
  @Override
  public boolean useInsertBatching() {
    return true;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual";
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
    return DatabaseType.Registry.findByIdentifier(H2.IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(ConcatenatedField)
   */
  @Override
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      // Interpret null values as empty strings
      sql.add("COALESCE(" + getSqlFrom(field) + ",'')");
    }
    return StringUtils.join(sql, " || ");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForIsNull(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForIsNull(Function function) {
    return "COALESCE(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ") ";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    StringBuilder statement = new StringBuilder().append("ALTER TABLE ").append(table.getName()).append(" ADD COLUMN ")
        .append(column.getName()).append(' ').append(sqlRepresentationOfColumnType(column, true));

    return Collections.singletonList(statement.toString());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();

    if (oldColumn.isPrimaryKey() && !newColumn.isPrimaryKey()) {
      result.add(dropPrimaryKeyConstraintStatement(table));
    }

    // Rename has to happen BEFORE any operations on the newly renamed column
    if (!newColumn.getName().equals(oldColumn.getName())) {
      result.add("ALTER TABLE " + schemaNamePrefix() + table.getName() + " ALTER COLUMN " + oldColumn.getName() + " RENAME TO "
          + newColumn.getName());
    }

    // Now do column operations on the new
    if (StringUtils.isNotEmpty(newColumn.getDefaultValue())) {
      String escape = newColumn.getType() == DataType.STRING ? "'" : "";
      result.add("ALTER TABLE " + table.getName() + " ALTER COLUMN " + newColumn.getName() + " SET DEFAULT " + escape
          + newColumn.getDefaultValue() + escape);
    }

    if (oldColumn.isNullable() != newColumn.isNullable()) {
      result.add("ALTER TABLE " + table.getName() + " ALTER COLUMN " + newColumn.getName() + " SET "
          + (newColumn.isNullable() ? "NULL" : "NOT NULL"));
    }

    if (oldColumn.getType() != newColumn.getType() ||
        oldColumn.getScale() != newColumn.getScale() ||
        oldColumn.getWidth() != newColumn.getWidth() ||
        !StringUtils.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue()) ||
        oldColumn.isAutoNumbered() != newColumn.isAutoNumbered()) {
      result.add("ALTER TABLE " + table.getName() + " ALTER COLUMN " + newColumn.getName() + " " +
        sqlRepresentationOfColumnType(newColumn, false, false, true));
    }

    // rebuild the PK if required
    List<Column> primaryKeys = primaryKeysForTable(table);
    if (oldColumn.isPrimaryKey() != newColumn.isPrimaryKey() && !primaryKeys.isEmpty()) {
      result.add(addPrimaryKeyConstraintStatement(table, namesOfColumns(primaryKeys)));
    }

    return result;
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ").append(table.getName())
      .append(" DROP COLUMN ").append(column.getName());

    return Collections.singletonList(statement.toString());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(org.alfasoftware.morf.metadata.Table, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> result = new ArrayList<>();

    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add("ALTER TABLE " + table.getName() + " DROP PRIMARY KEY");
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(
        addPrimaryKeyConstraintStatement(table, newPrimaryKeyColumns));
    }

    return result;
  }


  /**
   * @param table The table to add the constraint for
   * @param primaryKeyColumnNames
   * @return The statement
   */
  private String addPrimaryKeyConstraintStatement(Table table, List<String> primaryKeyColumnNames) {
    return "ALTER TABLE " + table.getName() + " ADD CONSTRAINT " + table.getName() + "_PK PRIMARY KEY (" + Joiner.on(", ").join(primaryKeyColumnNames) + ")";
  }


  /**
   * @param table The table whose primary key should be dropped
   * @return The statement
   */
  private String dropPrimaryKeyConstraintStatement(Table table) {
    return "ALTER TABLE " + table.getName() + " DROP CONSTRAINT " + table.getName() + "_PK";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public String indexDeploymentStatement(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ").append(index.getName()).append(" ON ").append(table.getName()).append(" (")
        .append(Joiner.on(',').join(index.columnNames())).append(")");

    return statement.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    return Arrays.asList("DROP INDEX " + indexToBeRemoved.getName());
  }


  /**
   * It does explicit VARCHAR casting to avoid a HSQLDB 'feature' in which
   * string literal values are effectively returned as CHAR (fixed width) data
   * types rather than VARCHARs, where the length of the CHAR to hold the value
   * is given by the maximum string length of any of the values that can be
   * returned by the CASE statement.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#makeStringLiteral(java.lang.String)
   */
  @Override
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }

    return String.format("CAST(%s AS VARCHAR(%d))", super.makeStringLiteral(literalValue), literalValue.length());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#decorateTemporaryTableName(java.lang.String)
   */
  @Override
  public String decorateTemporaryTableName(String undecoratedName) {
    return TEMPORARY_TABLE_PREFIX + undecoratedName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.View)
   */
  @Override
  public Collection<String> dropStatements(View view) {
    return Arrays.asList("DROP VIEW " + view.getName() + " IF EXISTS CASCADE");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "CAST(SUBSTRING(" + getSqlFrom(field) + ", 1, 4)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 5, 2)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 7, 2) AS DATE)";
  }



  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    return String.format("CAST(SUBSTRING(%1$s, 1, 4)||SUBSTRING(%1$s, 6, 2)||SUBSTRING(%1$s, 9, 2) AS INT)",sqlExpression);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    // Example for CURRENT_TIMESTAMP() -> 2015-06-23 11:25:08.11
    return String.format("CAST(SUBSTRING(%1$s, 1, 4)||SUBSTRING(%1$s, 6, 2)||SUBSTRING(%1$s, 9, 2)||SUBSTRING(%1$s, 12, 2)||SUBSTRING(%1$s, 15, 2)||SUBSTRING(%1$s, 18, 2) AS BIGINT)", sqlExpression);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "CURRENT_TIMESTAMP()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField,
   *      org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return "DATEDIFF('DAY'," + getSqlFrom(fromDate) + ", " + getSqlFrom(toDate) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForMonthsBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate) {
    return String.format(
       "CASE " +
        "WHEN %1$s = %2$s THEN 0 " +
        "ELSE " +
         "DATEDIFF(MONTH, %1$s, %2$s) + " +
         "CASE " +
          "WHEN %2$s > %1$s THEN " +
            "CASE " +
             "WHEN DAY(%1$s) <= DAY(%2$s) OR MONTH(%2$s) <> MONTH(DATEADD(DAY, 1, %2$s)) THEN 0 " +
             "ELSE -1 " +
            "END " +
          "ELSE " +
            "CASE " +
             "WHEN DAY(%2$s) <= DAY(%1$s) OR MONTH(%1$s) <> MONTH(DATEADD(DAY, 1, %1$s)) THEN 0 " +
             "ELSE 1 " +
            "END " +
         "END " +
       "END ",
       getSqlFrom(fromDate), getSqlFrom(toDate)
    );
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
      "DATEADD('DAY', %s, %s)",
      getSqlFrom(function.getArguments().get(1)),
      getSqlFrom(function.getArguments().get(0))
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddMonths(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddMonths(Function function) {
    return String.format(
      "DATEADD('MONTH', %s, %s)",
      getSqlFrom(function.getArguments().get(1)),
      getSqlFrom(function.getArguments().get(0))
        );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameTableStatements(java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {

    Builder<String> builder = ImmutableList.<String>builder();

    if (!primaryKeysForTable(from).isEmpty()) {
      builder.add(dropPrimaryKeyConstraintStatement(from));
    }

    builder.add("ALTER TABLE " + from.getName() + " RENAME TO " + to.getName());

    if (!primaryKeysForTable(to).isEmpty()) {
      builder.add(addPrimaryKeyConstraintStatement(to, namesOfColumns(primaryKeysForTable(to))));
    }

    return builder.build();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.MergeStatement)
   */
  @Override
  protected String getSqlFrom(MergeStatement statement) {

    String sqlMerge = super.getSqlFrom(statement);

    // TODO
    // The following is a workaround to a bug in H2 version 1.4.200 whereby the MERGE...USING statement does not release the source select statement
    // Please remove this once https://github.com/h2database/h2database/issues/2196 has been fixed and H2 upgraded to the fixed version
    String workaroundSqlMerge = "WITH " + MERGE_SOURCE_ALIAS + " AS (" + getSqlFrom(statement.getSelectStatement()) + ") " + MERGE_SOURCE_REPLACE_PATTERN.matcher(sqlMerge).replaceAll("USING " + MERGE_SOURCE_ALIAS);

    return workaroundSqlMerge;
  }


  @Override
  protected String getSqlFrom(SqlParameter sqlParameter) {
    return String.format("CAST(:%s AS %s)", sqlParameter.getMetadata().getName(), sqlRepresentationOfColumnType(sqlParameter.getMetadata(), false));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(int)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return String.format("SUBSTRING(REPLACE(RANDOM_UUID(),'-'), 1, %s)", getSqlFrom(function.getArguments().get(0)));
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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameIndexStatements(org.alfasoftware.morf.metadata.Table, java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.of(String.format("ALTER INDEX %s RENAME TO %s", fromIndexName, toIndexName));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#rebuildTriggers(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> rebuildTriggers(Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "DATEADD(dd, -DAY(DATEADD(m,1," + getSqlFrom(date) + ")), DATEADD(m,1," + getSqlFrom(date) + "))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect.getSqlForAnalyseTable(Table)
   */
  @Override
  public Collection<String> getSqlForAnalyseTable(Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect.getDeleteLimitSuffixSql(int)
   */
  @Override
  protected Optional<String> getDeleteLimitSuffix(int limit) {
    return Optional.of("LIMIT " + limit);
  }
}
