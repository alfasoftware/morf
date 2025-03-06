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

package org.alfasoftware.morf.jdbc;

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.ExceptSetOperator;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.PortableSqlFunction;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;

/**
 * Mock {@link SqlDialect} to give tests something vaguely realistic to work with.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MockDialect extends SqlDialect {

  private final DatabaseType databaseType = mock(DatabaseType.class);

  public MockDialect() {
    super(null);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getDatabaseType()
   */
  @Override
  public DatabaseType getDatabaseType() {
    return databaseType;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#connectionTestStatement()
   */
  @Override
  public String connectionTestStatement() {
    return "select 1";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(SequenceReference)
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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#internalTableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
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
      createTableStatement.append(column.getName()).append(" ");
      createTableStatement.append(sqlRepresentationOfColumnType(column));
      if (column.isAutoNumbered()) {
        int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        createTableStatement.append(" AUTO_INCREMENT(").append(autoNumberStart).append(") COMMENT 'AUTONUMSTART:[").append(autoNumberStart).append("]'");
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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#internalSequenceDeploymentStatements(Sequence) 
   */
  @Override
  protected Collection<String> internalSequenceDeploymentStatements(Sequence sequence) {

    List<String> statements = new ArrayList<>();

    StringBuilder createSequenceStatement = new StringBuilder();
    createSequenceStatement.append("CREATE ");

    if (sequence.isTemporary()) {
      createSequenceStatement.append("TEMPORARY ");
    }

    createSequenceStatement.append("SEQUENCE ");
    createSequenceStatement.append(schemaNamePrefix());
    createSequenceStatement.append(sequence.getName());

    if (sequence.isTemporary()) {
      createSequenceStatement.append(" SESSION");
    }

    if (sequence.getStartsWith() != null) {
      createSequenceStatement.append(" START WITH ");
      createSequenceStatement.append(sequence.getStartsWith());
    }

    statements.add(createSequenceStatement.toString());

    return statements;
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

      result.add("ALTER TABLE " + table.getName() + " ALTER COLUMN " + newColumn.getName() + " SET DEFAULT "
          + sqlForDefaultClauseLiteral(newColumn));
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
      result.add(dropPrimaryKeyConstraintStatement(table));
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(addPrimaryKeyConstraintStatement(table, newPrimaryKeyColumns));
    }

    return result;
  }


  private String addPrimaryKeyConstraintStatement(Table table, List<String> primaryKeyColumnNames) {
    return "ALTER TABLE " + schemaNamePrefix() + table.getName() + " ADD CONSTRAINT " + table.getName() + "_PK PRIMARY KEY (" + Joiner.on(", ").join(primaryKeyColumnNames) + ")";
  }


  private String dropPrimaryKeyConstraintStatement(Table table) {
    return "ALTER TABLE " + schemaNamePrefix() + table.getName() + " DROP PRIMARY KEY";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return "SUBSTRING(MD5(RAND()), 1, " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "NOW()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    return "TO_DATE(" + getSqlFrom(function.getArguments().get(0)) + ", 'yyyymmdd')";
  }



  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    return "TO_NUMBER(TO_CHAR(" + getSqlFrom(function.getArguments().get(0)) + ", 'yyyymmdd'))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    return "TO_NUMBER(TO_CHAR(" + getSqlFrom(function.getArguments().get(0)) + ", 'yyyymmddHH24MISS'))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
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
    return "DATEDIFF('MONTH'," + getSqlFrom(fromDate) + ", " + getSqlFrom(toDate) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddDays(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddDays(Function function) {
    return "DATEADD('DAY', " + getSqlFrom(function.getArguments().get(1)) + ", " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddMonths(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddMonths(Function function) {
    return "DATEADD('MONTH', " + getSqlFrom(function.getArguments().get(1)) + ", " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth(org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "DATEADD(dd, -DAY(DATEADD(m,1," + getSqlFrom(date) + ")), DATEADD(m,1," + getSqlFrom(date) + "))";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableNameWithSchemaName(org.alfasoftware.morf.sql.element.TableReference)
   */
  @Override
  protected String tableNameWithSchemaName(TableReference tableRef) {
    if (!StringUtils.isEmpty(tableRef.getDblink())) throw new IllegalStateException("DB Links are not supported in the Mock dialect. Found dbLink=" + tableRef.getDblink() + " for tableNameWithSchemaName=" + super.tableNameWithSchemaName(tableRef));
    return super.tableNameWithSchemaName(tableRef);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.ExceptSetOperator)
   */
  @Override
  protected String getSqlFrom(ExceptSetOperator operator) {
    throw new IllegalStateException("EXCEPT set operator is not supported in the Mock dialect");
  }



  @Override
  protected String getSqlFrom(PortableSqlFunction function) {
    throw new IllegalStateException("Portable functions are not supported in the Mock dialect");
  }


  @Override
  public boolean useForcedSerialImport() {
    return false;
  }
}