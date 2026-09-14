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

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.FunctionType;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements database specific statement generation for H2.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class H2Dialect extends SqlDialect {

  /**
   * The prefix to add to all temporary tables.
   */
  public static final String TEMPORARY_TABLE_PREFIX = "TEMP_";
  public static final String SYSTEM_SEQUENCE_PREFIX = "SYSTEM_SEQUENCE_";



  /**
   * @param schemaName Name of the schema to connect to
   *
   */
  public H2Dialect(String schemaName) {
    super(schemaName);
  }


  @Override
  protected String databaseTypeIdentifier() {
    return H2.IDENTIFIER;
  }


  /**
   * @see SqlDialect#tableDeploymentStatements(Table)
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
    createTableStatement.append(schemaNamePrefix());
    createTableStatement.append(table.getName());
    createTableStatement.append(" (");

    List<String> primaryKeys = new ArrayList<>();
    boolean first = true;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }
      createTableStatement.append(column.getName()).append(" ");
      createTableStatement.append(sqlRepresentationOfColumnType(column));
      if (column.isAutoNumbered()) {
        int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        createTableStatement.append(" AUTO_INCREMENT(").append(autoNumberStart)
                .append(") COMMENT 'AUTONUMSTART:[").append(autoNumberStart).append("]'");
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
   * @see SqlDialect#internalSequenceDeploymentStatements(Sequence)
   */
  @Override
  public Collection<String> internalSequenceDeploymentStatements(Sequence sequence) {
    List<String> statements = new ArrayList<>();

    // Create the sequence deployment statement
    StringBuilder createSequenceStatement = new StringBuilder();
    createSequenceStatement.append("CREATE ");

    createSequenceStatement.append("SEQUENCE ");
    createSequenceStatement.append(schemaNamePrefix());
    createSequenceStatement.append(sequence.getName());

    if (sequence.getStartsWith() != null) {
      createSequenceStatement.append(" START WITH ");
      createSequenceStatement.append(sequence.getStartsWith());
    }

    statements.add(createSequenceStatement.toString());

    return statements;
  }


  /**
   * @see SqlDialect#dropStatements(Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    return dropTables(Lists.newArrayList(table), false, true);
  }


  /**
   * @see SqlDialect#getColumnRepresentation(DataType,
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
   * @see SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual";
  }


  /**
   * @see SqlDialect#connectionTestStatement()
   */
  @Override
  public String connectionTestStatement() {
    return "select 1";
  }


  /**
   * @see SqlDialect#getDatabaseType()
   */
  @Override
  public DatabaseType getDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(H2.IDENTIFIER);
  }


  /**
   * @see SqlDialect#getSelectLimitSuffix(int)
   */
  @Override
  protected Optional<String> getSelectLimitSuffix(int limit) {
    return Optional.of("LIMIT " + limit);
  }


  /**
   * @see SqlDialect#alterTableAddColumnStatements(Table, Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    String statement = "ALTER TABLE " + schemaNamePrefix() + table.getName() + " ADD COLUMN " +
            column.getName() + ' ' + sqlRepresentationOfColumnType(column, true);

    return Collections.singletonList(statement);
  }


  /**
   * @see SqlDialect#alterTableChangeColumnStatements(Table, Column, Column)
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

      result.add("ALTER TABLE " + schemaNamePrefix() + table.getName() + " ALTER COLUMN " + newColumn.getName() + " SET DEFAULT "
          + sqlForDefaultClauseLiteral(newColumn));
    }

    if (oldColumn.isNullable() != newColumn.isNullable()) {
      result.add("ALTER TABLE " + schemaNamePrefix() + table.getName() + " ALTER COLUMN " + newColumn.getName() + " SET "
          + (newColumn.isNullable() ? "NULL" : "NOT NULL"));
    }

    if (oldColumn.getType() != newColumn.getType() ||
        oldColumn.getScale() != newColumn.getScale() ||
        oldColumn.getWidth() != newColumn.getWidth() ||
        !StringUtils.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue()) ||
        oldColumn.isAutoNumbered() != newColumn.isAutoNumbered()) {
      result.add("ALTER TABLE " + schemaNamePrefix() + table.getName() + " ALTER COLUMN " + newColumn.getName() + " " +
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
   * @see SqlDialect#alterTableDropColumnStatements(Table, Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    String statement = "ALTER TABLE " + schemaNamePrefix() + table.getName() +
            " DROP COLUMN " + column.getName();

    return Collections.singletonList(statement);
  }


  /**
   * @see SqlDialect#changePrimaryKeyColumns(Table, List, List)
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
   * @param primaryKeyColumnNames List of the column names of the primary key
   * @return The statement
   */
  private String addPrimaryKeyConstraintStatement(Table table, List<String> primaryKeyColumnNames) {
    return "ALTER TABLE " + schemaNamePrefix() + table.getName() + " ADD CONSTRAINT " + table.getName() + "_PK PRIMARY KEY (" + Joiner.on(", ").join(primaryKeyColumnNames) + ")";
  }


  /**
   * @param table The table whose primary key should be dropped
   * @return The statement
   */
  private String dropPrimaryKeyConstraintStatement(Table table) {
    return "ALTER TABLE " + schemaNamePrefix() + table.getName() + " DROP PRIMARY KEY";
  }


  /**
   * @see SqlDialect#indexDeploymentStatements(Table,
   *      Index)
   */
  @Override
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ").append(index.getName()).append(" ON ").append(schemaNamePrefix()).append(table.getName()).append(" (")
        .append(Joiner.on(',').join(index.columnNames())).append(")");

    return Collections.singletonList(statement.toString());
  }


  /**
   * @see SqlDialect#indexDropStatements(Table,
   *      Index)
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
   * @see SqlDialect#makeStringLiteral(String)
   */
  @Override
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }

    return String.format("CAST(%s AS VARCHAR(%d))", super.makeStringLiteral(literalValue), literalValue.length());
  }


  /**
   * @see SqlDialect#decorateTemporaryTableName(String)
   */
  @Override
  public String decorateTemporaryTableName(String undecoratedName) {
    return TEMPORARY_TABLE_PREFIX + undecoratedName;
  }


  /**
   * @see SqlDialect#getSqlForYYYYMMDDToDate(Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    AliasedField field = function.getArguments().get(0);
    return "CAST(SUBSTRING(" + getSqlFrom(field) + ", 1, 4)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 5, 2)||'-'||SUBSTRING(" + getSqlFrom(field) + ", 7, 2) AS DATE)";
  }



  /**
   * @see SqlDialect#getSqlForDateToYyyymmdd(Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    return String.format("CAST(SUBSTRING(%1$s, 1, 4)||SUBSTRING(%1$s, 6, 2)||SUBSTRING(%1$s, 9, 2) AS DECIMAL(8))",sqlExpression);
  }


  /**
   * @see SqlDialect#getSqlForDateToYyyymmddHHmmss(Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    String sqlExpression = getSqlFrom(function.getArguments().get(0));
    // Example for CURRENT_TIMESTAMP() -> 2015-06-23 11:25:08.11
    return String.format("CAST(SUBSTRING(%1$s, 1, 4)||SUBSTRING(%1$s, 6, 2)||SUBSTRING(%1$s, 9, 2)||SUBSTRING(%1$s, 12, 2)||SUBSTRING(%1$s, 15, 2)||SUBSTRING(%1$s, 18, 2) AS DECIMAL(14))", sqlExpression);
  }


  /**
   * @see SqlDialect#getSqlForNow(Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "CURRENT_TIMESTAMP()";
  }


  @Override
  protected String getSqlForHash(AliasedField field, AliasedField salt) {
    if (salt instanceof FieldLiteral && StringUtils.isBlank(((FieldLiteral) salt).getValue())) {
      return String.format("RAWTOHEX(HASH('SHA256', %s))",
          getSqlFrom(field));
    } else {
      return String.format("RAWTOHEX(HASH('SHA256', CONCAT(%s, %s)))",
          getSqlFrom(field), getSqlFrom(salt));
    }
  }


  /**
   * @see SqlDialect#getSqlForDaysBetween(AliasedField,
   *      AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return "DATEDIFF('DAY'," + getSqlFrom(fromDate) + ", " + getSqlFrom(toDate) + ")";
  }


  /**
   * @see SqlDialect#getSqlForMonthsBetween(AliasedField, AliasedField)
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
   * @see SqlDialect#getSqlForAddDays(Function)
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
   * @see SqlDialect#getSqlForAddMonths(Function)
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
   * @see SqlDialect#renameTableStatements(Table, Table)
   */
  @Override
  public Collection<String> renameTableStatements(Table from, Table to) {

    Builder<String> builder = ImmutableList.builder();

    // H2 special: PK gets dropped upon rename!
    if (!primaryKeysForTable(from).isEmpty()) {
      builder.add(dropPrimaryKeyConstraintStatement(from));
    }

    builder.add("ALTER TABLE " + schemaNamePrefix() + from.getName() + " RENAME TO " + to.getName());

    if (!primaryKeysForTable(to).isEmpty()) {
      builder.add(addPrimaryKeyConstraintStatement(to, namesOfColumns(primaryKeysForTable(to))));
    }

    return builder.build();
  }


  /**
   *  TODO
   * The following is a workaround to a bug in H2 version 1.4.200 whereby the MERGE...USING statement does not release the source select statement
   * Please remove this method once <a href="https://github.com/h2database/h2database/issues/2196">issue 2196</a> has been fixed and H2 upgraded to the fixed version
   * This workaround uses the following alternative syntax, which fortunately does not lead to the same bug:
   *
   * <pre>
   *   WITH xmergesource AS (SELECT ...)
   *   MERGE INTO Table
   *     USING xmergesource
   *     ON (Table.id = xmergesource.id)
   *     WHEN MATCHED THEN UPDATE ...
   *     WHEN NOT MATCHED THEN INSERT ...
   * </pre>
   *
   * @see SqlDialect#getSqlFrom(MergeStatement)
   */
  @Override
  protected String getSqlFrom(MergeStatement statement) {

    // --- TODO
    // call the original implementation which performs various consistency checks
    super.getSqlFrom(statement);

    // --- TODO
    // but ignore whatever it produces, and create a slightly different variant
    final StringBuilder sqlBuilder = new StringBuilder();

    // WITH xmergesource AS (SELECT ...)
    sqlBuilder.append("WITH ")
              .append(MERGE_SOURCE_ALIAS)
              .append(" AS (")
              .append(getSqlFrom(statement.getSelectStatement()))
              .append(") ");

    // MERGE INTO Table USING xmergesource
    sqlBuilder.append("MERGE INTO ")
              .append(schemaNamePrefix())
              .append(statement.getTable().getName())
              .append(" USING ")
              .append(MERGE_SOURCE_ALIAS);

    // ON (Table.id = xmergesource.id)
    sqlBuilder.append(" ON (")
              .append(matchConditionSqlForMergeFields(statement, MERGE_SOURCE_ALIAS, statement.getTable().getName()))
              .append(")");

    // WHEN MATCHED THEN UPDATE ...
    sqlBuilder.append(mergeStatementWhenMatchedUpdateClause(statement));

    // WHEN NOT MATCHED THEN INSERT ...
    Iterable<String> insertField = Iterables.transform(statement.getSelectStatement().getFields(), AliasedField::getImpliedName);
    Iterable<String> valueFields = Iterables.transform(statement.getSelectStatement().getFields(), field -> MERGE_SOURCE_ALIAS + "." + field.getImpliedName());

    sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (")
              .append(Joiner.on(", ").join(insertField))
              .append(") VALUES (")
              .append(Joiner.on(", ").join(valueFields))
              .append(")");

    return sqlBuilder.toString();
  }


  @Override
  protected String getSqlFrom(SqlParameter sqlParameter) {
    return String.format("CAST(:%s AS %s)", sqlParameter.getMetadata().getName(), sqlRepresentationOfColumnType(sqlParameter.getMetadata(), false));
  }


  /**
   * @see SqlDialect#getSqlFrom(SequenceReference)
   */
  @Override
  protected String getSqlFrom(SequenceReference sequenceReference) {
    StringBuilder result = new StringBuilder();

    result.append(sequenceReference.getName());

    switch (sequenceReference.getTypeOfOperation()) {
      case NEXT_VALUE:
        result.append(".NEXTVAL");
        break;
      case CURRENT_VALUE:
        result.append(".CURRVAL");
        break;
    }

    return result.toString();
  }

  /**
   * @see SqlDialect#getSqlForRandomString(Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return String.format("SUBSTRING(REPLACE(RANDOM_UUID(),'-'), 1, %s)", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see SqlDialect#getSqlForLastDayOfMonth
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "DATEADD(dd, -DAY(DATEADD(m,1," + getSqlFrom(date) + ")), DATEADD(m,1," + getSqlFrom(date) + "))";
  }


  /**
   * @see SqlDialect#getSqlForRowNumber()
   */
  @Override
  protected String getSqlForRowNumber() {
    return "ROW_NUMBER() OVER()";
  }


  /**
   * @see SqlDialect#getSqlForWindowFunction(Function)
   */
  @Override
  protected String getSqlForWindowFunction(Function function) {
    FunctionType functionType = function.getType();
    switch (functionType) {
      case ROW_NUMBER:
        return "ROW_NUMBER()";

      default:
        return super.getSqlForWindowFunction(function);
    }
  }


  /**
   * @see SqlDialect#getDeleteLimitSuffix(int)
   */
  @Override
  protected Optional<String> getDeleteLimitSuffix(int limit) {
    return Optional.of("LIMIT " + limit);
  }


  /**
   * @see SqlDialect#tableNameWithSchemaName(TableReference)
   */
  @Override
  protected String tableNameWithSchemaName(TableReference tableRef) {
    if (!StringUtils.isEmpty(tableRef.getDblink())) throw new IllegalStateException("DB Links are not supported in the H2 dialect. Found dbLink=" + tableRef.getDblink() + " for tableNameWithSchemaName=" + super.tableNameWithSchemaName(tableRef));
    return super.tableNameWithSchemaName(tableRef);
  }


  @Override
  public boolean useForcedSerialImport() {
    return true;
  }
}