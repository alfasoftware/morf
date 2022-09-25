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

import static org.alfasoftware.morf.metadata.DataType.INTEGER;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.UseIndex;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.NullValueHandling;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements database specific statement generation for NuoDB.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class NuoDBDialect extends SqlDialect {

  /**
   * The prefix to add to all temporary tables.
   */
  static final String TEMPORARY_TABLE_PREFIX = "TEMP_";
  static final Set<String> TEMPORARY_TABLES = new HashSet<>();


  /**
   *
   */
  public NuoDBDialect(String schemaName) {
    super(schemaName);
  }


  @Override
  protected String schemaNamePrefix(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getSchemaName())) {
      if (TEMPORARY_TABLES.contains(tableRef.getName())) {
        return "";
      }
      return schemaNamePrefix();
    } else {
      return tableRef.getSchemaName().toUpperCase() + ".";
    }
  }


  @Override
  protected String schemaNamePrefix(Table table) {
    if (table.isTemporary()) {
      return "";
    }
    return schemaNamePrefix().toUpperCase();
  }


  /**
   * When deploying a table need to ensure that an index doesn't already exist when creating it.
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> tableDeploymentStatements(Table table) {
    Builder<String> statements = ImmutableList.<String>builder();

    statements.addAll(internalTableDeploymentStatements(table));

    for (Index index : table.indexes()) {
      statements.add(optionalDropIndexStatement(table, index));
      statements.addAll(indexDeploymentStatements(table, index));
    }

    return statements.build();
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
      TEMPORARY_TABLES.add(table.getName());
    }

    createTableStatement.append("TABLE ");
    createTableStatement.append(qualifiedTableName(table));
    createTableStatement.append(" (");

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;
    Column autoNumbered = null;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }
      createTableStatement.append(column.getName() + " ");
      createTableStatement.append(sqlRepresentationOfColumnType(column));
      if (column.isAutoNumbered()) {
        autoNumbered = column;
        int autoNumberStart = autoNumbered.getAutoNumberStart() == -1 ? 1 : autoNumbered.getAutoNumberStart();
        statements.add("DROP SEQUENCE IF EXISTS " + schemaNamePrefix() + createNuoDBGeneratorSequenceName(table, column));
        statements.add("CREATE SEQUENCE " + schemaNamePrefix() + createNuoDBGeneratorSequenceName(table, autoNumbered) + " START WITH " + autoNumberStart);
        createTableStatement.append(" GENERATED BY DEFAULT AS IDENTITY(" + createNuoDBGeneratorSequenceName(table, autoNumbered) + ")");
      }

      if (column.isPrimaryKey()) {
        primaryKeys.add(column.getName());
      }

      first = false;
    }

    if (!primaryKeys.isEmpty()) {
      createTableStatement.append(", PRIMARY KEY (");
      createTableStatement.append(Joiner.on(", ").join(primaryKeys));
      createTableStatement.append(")");
    }

    createTableStatement.append(")");
    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * Create a standard name for the NuoDB generator sequence, controlling the autonumbering
   */
  private String createNuoDBGeneratorSequenceName(Table table, Column column) {
    return  table.getName() + "_IDS_" + column.getAutoNumberStart();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    if (table.isTemporary()) {
      TEMPORARY_TABLES.remove(table.getName());
    }

    List<String> dropList = new ArrayList<>();
    dropList.add("drop table " + qualifiedTableName(table));

    for (Column column : table.columns()) {
      if(column.isAutoNumbered()) {
        dropList.add("DROP SEQUENCE IF EXISTS " + schemaNamePrefix() +  createNuoDBGeneratorSequenceName(table, column));
      }
    }

    // TODO Alfa internal ref WEB-57648 NuoDB doesn't seem to always drop the indexes when dropping the tables.
    // We need to explicitly have these statements to prevent index clashes.
    for (Index index : table.indexes()) {
      dropList.add(optionalDropIndexStatement(table, index));
    }

    return dropList;
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
        //NuoDB has a BOOLEAN data type but for consistency with the other dialects, we store a smallint
        return "SMALLINT";

      case BIG_INTEGER:
        return "BIGINT";

      case INTEGER:
        return "INTEGER";

      case BLOB:
        return "BLOB";

      case CLOB:
        return "CLOB";

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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#fetchSizeForBulkSelects()
   */
  @Override
  public int fetchSizeForBulkSelects() {
    return 1000;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual ";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#connectionTestStatement()
   */
  @Override
  public String connectionTestStatement() {
    return "select 1 from dual";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getDatabaseType()
   */
  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(NuoDB.IDENTIFIER);
  }


  @Override
  protected String getSqlFrom(LocalDate literalValue) {
    return String.format("DATE('%s')", literalValue.toString("yyyy-MM-dd"));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.FieldLiteral)
   */
  @Override
  protected String getSqlFrom(FieldLiteral field) {
    switch (field.getDataType()) {
      case DATE:
        return String.format("DATE('%s')", field.getValue());
      default:
        return super.getSqlFrom(field);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    ImmutableList.Builder<String> statements = ImmutableList.builder();

    statements.add(
      new StringBuilder().append("ALTER TABLE ").append(qualifiedTableName(table)).append(" ADD COLUMN ")
        .append(column.getName()).append(' ').append(sqlRepresentationOfColumnType(column, true))
        .toString()
    );

    if (StringUtils.isNotBlank(column.getDefaultValue()) && column.isNullable()) {
      statements.add("UPDATE " + table.getName() + " SET " + column.getName() + " = " + getSqlFrom(new FieldLiteral(column.getDefaultValue(), column.getType())));
    }

    return statements.build();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   * To modify the column we need to drop any indexes it's used for, change the column, and then recreate the index.
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();
    List<Index> indexesToRebuild = indexesToDropWhenModifyingColumn(table, oldColumn);

    for(Index index : indexesToRebuild) {
      result.addAll(indexDropStatements(table, index));
    }

    if (oldColumn.isPrimaryKey()) {
      result.add(dropPrimaryKeyConstraintStatement(table));
    }

    // Rename has to happen BEFORE any operations on the newly renamed column
    if (!newColumn.getName().equals(oldColumn.getName())) {
      result.add("ALTER TABLE " + qualifiedTableName(table)
        + " RENAME COLUMN " + oldColumn.getName()
        + " TO " + newColumn.getName()
      );
    }

    if (oldColumn.getType() != newColumn.getType()
        ||oldColumn.getScale() != newColumn.getScale()
        ||oldColumn.getWidth() != newColumn.getWidth()
    ) {
        result.add(
          "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
          + " TYPE " + sqlRepresentationOfColumnType(newColumn, false, false, true)
        );
    }

    result.addAll(changeColumnNullability(table, oldColumn, newColumn));

    if (oldColumn.isAutoNumbered() != newColumn.isAutoNumbered()) {
      // FIXME this is obviously wrong since we're losing the data in the column. Nuo support
      // is under construction. To be resolved.
      result.addAll(alterTableDropColumnStatements(table, oldColumn));
      result.addAll(alterTableAddColumnStatements(table, newColumn));
    }

    // rebuild the PK
    List<Column> primaryKeys = primaryKeysForTable(table);
    if ((newColumn.isPrimaryKey() || oldColumn.isPrimaryKey()) && !primaryKeys.isEmpty()) {
      result.add(addPrimaryKeyConstraintStatement(table, namesOfColumns(primaryKeys)));
    }

    for(Index index : indexesToRebuild) {
      result.addAll(indexDeploymentStatements(table, index));
    }

    return result;
  }


  /**
   * Change the nullability and default value of the column
   */
  private List<String> changeColumnNullability(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();
    if (StringUtils.isNotEmpty(newColumn.getDefaultValue())) {
      result.add(
        "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
        + " NOT NULL" // required by the DEFAULT to update existing rows
        + " DEFAULT " + sqlForDefaultClauseLiteral(newColumn)
      );
    }
    else if (!StringUtils.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue())) {
      result.add(
        "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
        + " DROP DEFAULT"
      );
      result.add(
        "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
        + " " + (newColumn.isNullable() ? "NULL" : "NOT NULL")
      );
    }
    else if (oldColumn.isNullable() != newColumn.isNullable()) {
      result.add(
        "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
        + " " + (newColumn.isNullable() ? "NULL" : "NOT NULL DEFAULT 0")
      );
      result.add(
        "ALTER TABLE " + qualifiedTableName(table) + " ALTER COLUMN " + newColumn.getName()
        + " DROP DEFAULT"
      );
    }
    return result;
  }


  private List<Index> indexesToDropWhenModifyingColumn(Table table, final Column column) {
    return FluentIterable.from(table.indexes()).filter(new Predicate<Index>() {
      @Override
      public boolean apply(Index input) {
        return input.columnNames().contains(column.getName());
      }
    }).toList();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    if (column.isPrimaryKey()) {
      result.add(dropPrimaryKeyConstraintStatement(table));
    }

    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ").append(qualifiedTableName(table))
      .append(" DROP COLUMN ").append(column.getName());

    result.add(statement.toString());
    result.add("DROP SEQUENCE IF EXISTS " + schemaNamePrefix() + createNuoDBGeneratorSequenceName(table, column));

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(org.alfasoftware.morf.metadata.Table, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> result = new ArrayList<>();

    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add(dropPrimaryKeyConstraintStatement(table));
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(addPrimaryKeyConstraintStatement(table, newPrimaryKeyColumns));
    }

    return result;
  }


  /**
   * @param table The table to add the constraint for
   * @param primaryKeyColumnNames
   * @return The statement
   */
  private String addPrimaryKeyConstraintStatement(Table table, List<String> primaryKeyColumnNames) {
    return "ALTER TABLE " + qualifiedTableName(table) + " ADD PRIMARY KEY (" + Joiner.on(", ").join(primaryKeyColumnNames) + ")";
  }


  /**
   * @param table The table whose primary key should be dropped
   * @return The statement
   */
  private String dropPrimaryKeyConstraintStatement(Table table) {
    return "DROP INDEX IF EXISTS "+schemaNamePrefix(table)+"\"" + table.getName().toUpperCase() + "..PRIMARY_KEY\"";
  }


  /**
   * For each index to add, we need to ensure the old index has been dropped in NuoDB
   * @see org.alfasoftware.morf.jdbc.SqlDialect#addIndexStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> addIndexStatements(Table table, Index index) {
    return ImmutableList.<String>builder()
        .add(optionalDropIndexStatement(table, index))
        .addAll(indexDeploymentStatements(table, index))
        .build();
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    return Collections.singletonList(
      statement
      .append("INDEX ")
      .append(index.getName()) // we don't specify the schema - it's implicit
      .append(" ON ")
      .append(qualifiedTableName(table))
      .append(" (")
      .append(Joiner.on(',').join(index.columnNames()))
      .append(")")
      .toString());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    return Arrays.asList("DROP INDEX " + schemaNamePrefix(table) + indexToBeRemoved.getName());
  }


  /**
   * Creates an SQL statement which attempts to drop an index if it exists
   */
  private String optionalDropIndexStatement(Table table, Index indexToBeRemoved) {
    return "DROP INDEX IF EXISTS " + schemaNamePrefix(table) + indexToBeRemoved.getName();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameIndexStatements(org.alfasoftware.morf.metadata.Table, java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameIndexStatements(Table table, final String fromIndexName, final String toIndexName) {
    Index newIndex, existingIndex;

    try {
      newIndex = Iterables.find(table.indexes(), new Predicate<Index>() {
        @Override public boolean apply(Index input) {
          return input.getName().equals(toIndexName);
        }
      });

      existingIndex = newIndex.isUnique() ? index(fromIndexName).columns(newIndex.columnNames()).unique() :
        index(fromIndexName).columns(newIndex.columnNames());
    } catch (NoSuchElementException nsee) {
      // If the index wasn't found, we must have the old schema instead of the
      // new one so try the other way round
      existingIndex = Iterables.find(table.indexes(), new Predicate<Index>() {
        @Override public boolean apply(Index input) {
          return input.getName().equals(fromIndexName);
        }
      });

      newIndex = existingIndex.isUnique() ? index(toIndexName).columns(existingIndex.columnNames()).unique() :
        index(toIndexName).columns(existingIndex.columnNames());
    }

    return ImmutableList.<String>builder()
      .addAll(indexDropStatements(table, existingIndex))
      .addAll(indexDeploymentStatements(table, newIndex))
      .build();
  }


  @Override
  protected String getSqlForOrderByField(FieldReference orderByField) {
    switch (orderByField.getNullValueHandling().isPresent()
              ? orderByField.getNullValueHandling().get()
              : NullValueHandling.NONE) {
      case FIRST:
        return getSqlFrom(orderByField) + " IS NOT NULL, " + super.getSqlForOrderByField(orderByField);
      case LAST:
        return getSqlFrom(orderByField) + " IS NULL, " + super.getSqlForOrderByField(orderByField);
      case NONE:
      default:
        return super.getSqlForOrderByField(orderByField);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForOrderByFieldNullValueHandling(org.alfasoftware.morf.sql.element.FieldReference, java.lang.StringBuilder)
   */
  @Override
  protected String getSqlForOrderByFieldNullValueHandling(FieldReference orderByField) {
    return "";
  }


  @Override
  protected String escapeSql(String literalValue) {
    String escaped = super.escapeSql(literalValue);
    // we need to deal with a strange design with the \' escape but no \\ escape
    return StringUtils.replace(escaped, "\\'", "'||TRIM('\\ ')||''");
  }

  //   \''\'
  //   \\''''\\''

  // <ello wo\''\'>
  // <ello wo\\''\\'>


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#decorateTemporaryTableName(java.lang.String)
   */
  @Override
  public String decorateTemporaryTableName(String undecoratedName) {
    return TEMPORARY_TABLE_PREFIX + undecoratedName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "DATE_FROM_STR(" + getSqlFrom(field) + ", 'yyyyMMdd')";

//    return "DATE(SUBSTRING(" + getSqlFrom(field) + ", 1, 4)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 5, 2)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 7, 2))";
  }



  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    return String.format("CAST(DATE_TO_STR(%1$s, 'yyyyMMdd') AS DECIMAL(8))", sqlExpression);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    // Example for CURRENT_TIMESTAMP() -> 2015-06-23 11:25:08.11
    return String.format("CAST(DATE_TO_STR(%1$s, 'yyyyMMddHHmmss') AS DECIMAL(14))", sqlExpression);
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
    //To avoid potential TIMESTAMP/DATE type mismatches, we need to ensure DATEDIFF
    //is passed a correctly formatted string of 'yyyy-MM-dd'. The following approach will
    //ensure that the source date/string is always correctly formatted.
    String fromDateSql = String.format("DATE_TO_STR(%1$s, 'yyyy-MM-dd')", getSqlFrom(fromDate));
    String toDateSql = String.format("DATE_TO_STR(%1$s, 'yyyy-MM-dd')", getSqlFrom(toDate));
    return "DATEDIFF(DAY," + fromDateSql + ", " + toDateSql + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForMonthsBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate) {
    String toDateStr = getSqlFrom(toDate);
    String fromDateStr = getSqlFrom(fromDate);
    return
       "(EXTRACT(YEAR FROM "+toDateStr+") - EXTRACT(YEAR FROM "+fromDateStr+")) * 12"
       + "+ (EXTRACT(MONTH FROM "+toDateStr+") - EXTRACT(MONTH FROM "+fromDateStr+"))"
       + "+ CASE WHEN "+toDateStr+" > "+fromDateStr
             + " THEN CASE WHEN EXTRACT(DAY FROM "+toDateStr+") >= EXTRACT(DAY FROM "+fromDateStr+") THEN 0"
                       + " WHEN EXTRACT(MONTH FROM "+toDateStr+") <> EXTRACT(MONTH FROM "+toDateStr+" + 1) THEN 0"
                       + " ELSE -1 END"
             + " ELSE CASE WHEN EXTRACT(MONTH FROM "+fromDateStr+") <> EXTRACT(MONTH FROM "+fromDateStr+" + 1) THEN 0"
                       + " WHEN EXTRACT(DAY FROM "+fromDateStr+") >= EXTRACT(DAY FROM "+toDateStr+") THEN 0"
                       + " ELSE 1 END"
             + " END"
       + "\n"
    ;
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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameTableStatements(java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {

    Builder<String> builder = ImmutableList.<String>builder();

    if (!primaryKeysForTable(from).isEmpty()) {
      builder.add(dropPrimaryKeyConstraintStatement(from));
    }

    builder.add("ALTER TABLE " + qualifiedTableName(from) + " RENAME TO " + qualifiedTableName(to));

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
    Iterable<String> intoFields = Iterables.transform(statement.getSelectStatement().getFields(), AliasedField::getImpliedName);
    sqlBuilder.append(Joiner.on(", ").join(intoFields));
    sqlBuilder.append(") ");

    // Add select statement
    sqlBuilder.append(getSqlFrom(statement.getSelectStatement()));

    // Add the update expressions
    if (getNonKeyFieldsFromMergeStatement(statement).iterator().hasNext()) {
      sqlBuilder.append(" ON DUPLICATE KEY UPDATE ");
      Iterable<AliasedField> updateExpressions = getMergeStatementUpdateExpressions(statement);
      String updateExpressionsSql = getMergeStatementAssignmentsSql(updateExpressions);
      sqlBuilder.append(updateExpressionsSql);
    } else {
      sqlBuilder.append(" ON DUPLICATE KEY SKIP");
    }
    return sqlBuilder.toString();
  }


  @Override
  protected String getSqlFrom(MergeStatement.InputField field) {
    return "values(" + field.getName() + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(int)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return String.format("SUBSTRING(CAST(RAND() AS STRING), 3, %s)", getSqlFrom(function.getArguments().get(0)));
  }


  @Override
  protected String getSqlForLeftPad(AliasedField field, AliasedField length, AliasedField character) {
    // this would be much nicer if written as DSL
    String paddingSql = "REPLACE('                    ', ' ', " + getSqlFrom(character) + ")";
    String padlengthSql = getSqlFrom(length) + " - LENGTH(CAST(" + getSqlFrom(field) + " AS STRING))";
    String padjoinSql = "SUBSTRING(" + paddingSql + ", 1, " + padlengthSql + ") || " + getSqlFrom(field);
    String padtrimSql = "SUBSTRING(" + getSqlFrom(field) + ", 1, " + getSqlFrom(length) + ")";
    return "CASE WHEN " + padlengthSql + " > 0 THEN " + padjoinSql + " ELSE " + padtrimSql + " END";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#likeEscapeSuffix()
   */
  @Override
  protected String likeEscapeSuffix() {
    return ""; // the escape character is \ by default. We don't need to set it, and setting it appears to be challenging anyway as it is a general escape char.
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#supportsWindowFunctions()
   */
  @Override
  public boolean supportsWindowFunctions() {
    return true;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth(org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "DATE_SUB(DATE_ADD(DATE_SUB("+getSqlFrom(date)+", INTERVAL DAY("+getSqlFrom(date)+")-1 DAY), INTERVAL 1 MONTH), INTERVAL 1 DAY)";
  }


  /**
   * Creates a qualified (with schema prefix) table name string, from a table object.
   *
   * @param table The table metadata.
   * @return The table's qualified name.
   */
  private String qualifiedTableName(Table table) {
    if (table.isTemporary()) {
      return table.getName(); // temporary tables do not exist in a schema
    }
    return schemaNamePrefix() + table.getName();
  }


  /**
   * Creates a qualified (with schema prefix) table name string, from a table reference.
   *
   * <p>If the reference has a schema specified, that schema is used. Otherwise, the default schema is used.</p>
   *
   * @param table The table metadata.
   * @return The table's qualified name.
   */
  private String qualifiedTableName(TableReference table) {
    if (StringUtils.isBlank(table.getSchemaName())) {
      return schemaNamePrefix() + table.getName();
    } else {
      return table.getSchemaName() + "." + table.getName();
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#selectStatementPreFieldDirectives(org.alfasoftware.morf.sql.SelectStatement)
   */
  @Override
  protected String selectStatementPreFieldDirectives(SelectStatement selectStatement) {
    if (selectStatement.getHints().isEmpty()) {
      return super.selectStatementPreFieldDirectives(selectStatement);
    }

    // http://doc.nuodb.com/Latest/Content/Using-Optimizer-Hints.htm

    List<String> hintTexts = Lists.newArrayList();
    for (Hint hint : selectStatement.getHints()) {
      if (hint instanceof UseIndex) {
        UseIndex useIndex = (UseIndex)hint;
        TableReference table = useIndex.getTable();
        hintTexts.add(
          String.format("USE_INDEX(%s, %s)",
            StringUtils.isEmpty(table.getAlias()) ? qualifiedTableName(table) : table.getAlias(),
            useIndex.getIndexName()));

      }
      if (hint instanceof UseImplicitJoinOrder) {
        hintTexts.add("ORDERED");
      }
    }

    return hintTexts.isEmpty() ? "" : "/*+ " + Joiner.on(", ").join(hintTexts) + " */ ";
  };


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect.getDeleteLimitSuffixSql(int)
   */
  @Override
  protected Optional<String> getDeleteLimitSuffix(int limit) {
    return Optional.of("LIMIT " + limit);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dbLinkPrefix(org.alfasoftware.morf.sql.element.TableReference)
   */
  @Override
  protected String dbLinkPrefix(TableReference table) {
    throw new IllegalStateException("DB Links are not supported in the Nuo dialect");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dbLinkSuffix(org.alfasoftware.morf.sql.element.TableReference)
   */
  @Override
  protected String dbLinkSuffix(TableReference table) {
    return "";
  }

}
