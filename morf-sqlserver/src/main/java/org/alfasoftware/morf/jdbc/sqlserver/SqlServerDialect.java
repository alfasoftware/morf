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

package org.alfasoftware.morf.jdbc.sqlserver;

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.OptimiseForRowCount;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.UseIndex;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

/**
 * Provides SQL Server specific SQL statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class SqlServerDialect extends SqlDialect {

 /*
  * This SQL came from http://stackoverflow.com/questions/8641954/how-to-drop-column-with-constraint
  */
 @VisibleForTesting
 static final String dropDefaultForColumnSql = "DECLARE @sql NVARCHAR(MAX) \n" +
     "WHILE 1=1\n" +
     "BEGIN\n" +
     "    SELECT TOP 1 @sql = N'alter table {table} drop constraint ['+dc.NAME+N']'\n" +
     "    from sys.default_constraints dc\n" +
     "    JOIN sys.columns c\n" +
     "        ON c.default_object_id = dc.object_id\n" +
     "    WHERE\n" +
     "        dc.parent_object_id = OBJECT_ID('{table}')\n" +
     "    AND c.name = N'{column}'\n" +
     "    IF @@ROWCOUNT = 0 BREAK\n" +
     "    EXEC (@sql)\n" +
     "END";

  /**
   * Used to force collation to be case-sensitive.
   */
  private static final String COLLATE = "COLLATE SQL_Latin1_General_CP1_CS_AS";

  /**
   * The hint types supported.
   */
  private static final Set<Class<? extends Hint>> SUPPORTED_HINTS = ImmutableSet.of(UseIndex.class, OptimiseForRowCount.class, UseImplicitJoinOrder.class);


  /**
   * Creates an instance of MS SQL Server dialect.
   *
   * @param schemaName The database schema name.
   */
  public SqlServerDialect(String schemaName) {
    super(schemaName);
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
    createTableStatement.append("TABLE ");
    createTableStatement.append(schemaNamePrefix());
    createTableStatement.append(table.getName());
    createTableStatement.append(" (");

    boolean first = true;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }
      createTableStatement.append(String.format("[%s] ", column.getName()));
      createTableStatement.append(sqlRepresentationOfColumnType(table, column, false));
      if (column.isAutoNumbered()) {
        int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
        createTableStatement.append(" IDENTITY(" + autoNumberStart + ", 1)");
      }

      first = false;
    }

    List<Column> primaryKeys = primaryKeysForTable(table);

    if (!primaryKeys.isEmpty()) {
      createTableStatement.append(", ");
      createTableStatement.append(buildPrimaryKeyConstraint(table.getName(), namesOfColumns(primaryKeys)));
    }

    createTableStatement.append(")");

    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return StringUtils.EMPTY; // SQLServer doesn't have a "DUAL" table like oracle etc.
  }


  /**
   * @param tableName Name of the table.
   * @param primaryKeys List of the primary keys on the table.
   */
  private String buildPrimaryKeyConstraint(String tableName, List<String> primaryKeys) {

    StringBuilder pkConstraint = new StringBuilder();
    pkConstraint.append("CONSTRAINT [");
    pkConstraint.append(undecorateName(tableName));
    pkConstraint.append("_PK] PRIMARY KEY ([");
    pkConstraint.append(Joiner.on("], [").join(primaryKeys));
    pkConstraint.append("])");
    return pkConstraint.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder createIndexStatement = new StringBuilder();
    createIndexStatement.append("CREATE ");
    if (index.isUnique()) {
      createIndexStatement.append("UNIQUE NONCLUSTERED ");
    }
    createIndexStatement.append("INDEX ");
    createIndexStatement.append(index.getName());
    createIndexStatement.append(" ON ");
    createIndexStatement.append(schemaNamePrefix());
    createIndexStatement.append(table.getName());
    createIndexStatement.append(" (");
    boolean firstColumn = true;
    for (String columnName : index.columnNames()) {
      if (firstColumn) {
        firstColumn = false;
      } else {
        createIndexStatement.append(", ");
      }
      createIndexStatement.append(String.format("[%s]", columnName));
    }
    createIndexStatement.append(")");

    return Collections.singletonList(createIndexStatement.toString());
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table,
   *      org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    return Arrays.asList("DROP INDEX " + indexToBeRemoved.getName() + " ON " + schemaNamePrefix() + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    return Arrays.asList("DROP TABLE " + schemaNamePrefix() + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.View)
   */
  @Override
  public Collection<String> dropStatements(View view) {
    List<String> statements = new ArrayList<>();

    StringBuilder createTableStatement = new StringBuilder();
    createTableStatement.append(String.format("IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'%s%s'))",
      schemaNamePrefix(), view.getName()));
    createTableStatement.append(String.format(" DROP VIEW %s%s", schemaNamePrefix(), view.getName()));

    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#preInsertWithPresetAutonumStatements(org.alfasoftware.morf.metadata.Table, boolean)
   */
  @Override
  public Collection<String> preInsertWithPresetAutonumStatements(Table table, boolean insertingUnderAutonumLimit) {
    if (getAutoIncrementColumnForTable(table) != null) {
      return Arrays.asList("SET IDENTITY_INSERT " + schemaNamePrefix() + table.getName() + " ON");
    } else {
      return SqlDialect.NO_STATEMENTS;
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#postInsertWithPresetAutonumStatements(org.alfasoftware.morf.metadata.Table, boolean)
   */
  @Override
  public void postInsertWithPresetAutonumStatements(Table table, SqlScriptExecutor executor,Connection connection, boolean insertingUnderAutonumLimit) {
    Column autonumber = getAutoIncrementColumnForTable(table);
    if (autonumber == null) {
      return;
    }

    // See http://social.msdn.microsoft.com/Forums/en-US/transactsql/thread/4443e023-b6e9-4d71-9b53-06245b2e95ef
    // After an insert of values lower than the autonumber seed value, the "current" autonumber value is set to the
    // autonumber seed value or the highest value inserted, whichever is higher.  However, this differs from the
    // behaviour when the table is first created, when the first record will be created with the autonumber
    // seed value (not the autonumber seed value plus one).  This sequence resolves the difference by first forcing
    // the current (next - 1) value to the start value minus one, then lets SQL server correct it to the highest
    // value in the table if it is too low.

    // TODO Alfa internal ref WEB-23969 if we're running on SQL Server 2012, this bug no longer exists so we can
    // just reseed as for an empty table (i.e. autonumber.getAutoNumberStart()).  Need to implement this.
    executor.execute(ImmutableList.of(
      "SET IDENTITY_INSERT " + schemaNamePrefix() + table.getName() + " OFF",

      "IF EXISTS (SELECT 1 FROM " + schemaNamePrefix() + table.getName() + ")\n" +
      "BEGIN\n" +
      "  DBCC CHECKIDENT (\"" + schemaNamePrefix() + table.getName() + "\", RESEED, " + (autonumber.getAutoNumberStart() - 1) + ")\n" +
      "  DBCC CHECKIDENT (\"" + schemaNamePrefix() + table.getName() + "\", RESEED)\n" +
      "END\n" +
      "ELSE\n" +
      "BEGIN\n" +
      "  DBCC CHECKIDENT (\"" + schemaNamePrefix() + table.getName() + "\", RESEED, " + autonumber.getAutoNumberStart() + ")\n" +
      "END"
    ),connection);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getColumnRepresentation(org.alfasoftware.morf.metadata.DataType,
   *      int, int)
   */
  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    if (needsCollation(dataType)) {
      return String.format("%s %s", getInternalColumnRepresentation(dataType, width, scale), COLLATE);
    }
    return getInternalColumnRepresentation(dataType, width, scale);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.Cast)
   */
  @Override
  protected String getSqlFrom(Cast cast) {
    DataType dataType = cast.getDataType();
    StringBuilder output = new StringBuilder();
    output.append("CAST(")
          .append(getSqlFrom(cast.getExpression()))
          .append(" AS ")
          .append(getInternalColumnRepresentation(dataType, cast.getWidth(), cast.getScale()))
          .append(")");

    if (needsCollation(dataType)) {
      output.append(" ").append(COLLATE);
    }
    return output.toString();
  }


  /**
   * @param dataType a data type to examine
   * @return true if this data type should have COLLATE set.
   */
  private static boolean needsCollation(DataType dataType) {
    return dataType == DataType.STRING || dataType == DataType.CLOB;
  }


  /**
   * Gets the underlying column representation (e.g. without any COLLATE statements).
   *
   * @param dataType the column datatype.
   * @param width the column width.
   * @param scale the column scale.
   * @return a string representation of the column definition.
   */
  private String getInternalColumnRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        return String.format("NVARCHAR(%d)", width);

      case DECIMAL:
        return String.format("NUMERIC(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        return "BIT";

      case BIG_INTEGER:
        return "BIGINT";

      case INTEGER:
        return "INTEGER";

      case BLOB:
        return "IMAGE";

      case CLOB:
        return "NVARCHAR(MAX)";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
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
    return DatabaseType.Registry.findByIdentifier(SqlServer.IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(ConcatenatedField)
   */
  @Override
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      sql.add("COALESCE("+getSqlFrom(field)+",'')");
    }
    return StringUtils.join(sql, " + ");
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#decorateTemporaryTableName(java.lang.String)
   */
  @Override
  public String decorateTemporaryTableName(String undecoratedName) {
    return "#" + undecoratedName;
  }


  /**
   * Removes any decoration characters from the name. (# for temp table).
   *
   * @param name name of table
   * @return version of name with any decoration removed.
   */
  public String undecorateName(String name) {
    if (name.startsWith("#")) {
      return name.substring(1);
    } else {
      return name;
    }
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.UpdateStatement)
   */
  @Override
  protected String getSqlFrom(UpdateStatement statement) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException(String.format("Cannot create SQL for a blank table [%s]", destinationTableName));
    }

    StringBuilder sqlBuilder = new StringBuilder();

    // Add the preamble
    sqlBuilder.append("UPDATE ");

    // Now add the table to update
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);

    // Put in the standard fields
    sqlBuilder.append(getUpdateStatementSetFieldSql(statement.getFields()));

    // Add a FROM clause if the table is aliased
    if (!statement.getTable().getAlias().equals("")) {
      sqlBuilder.append(" FROM ");
      sqlBuilder.append(schemaNamePrefix(statement.getTable()));
      sqlBuilder.append(destinationTableName);
      sqlBuilder.append(String.format(" %s", statement.getTable().getAlias()));
    }

    // Now put the where clause in
    if (statement.getWhereCriterion() != null) {
      sqlBuilder.append(" WHERE ");
      sqlBuilder.append(getSqlFrom(statement.getWhereCriterion()));
    }

    return sqlBuilder.toString();
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    List<String> statements = new ArrayList<>();

    // TODO looks like if we're adding to an existing PK we should drop the PK first here. SQL
    // server is currently hard to test so need to investigate further.

    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ")
      .append(schemaNamePrefix())
      .append(table.getName())
      .append(" ADD ") // We don't say COLUMN here for some reason
      .append(column.getName())
      .append(' ')
      .append(sqlRepresentationOfColumnType(table, column, true));

    statements.add(statement.toString());

    // Recreate the primary key if the column is in it
    if (column.isPrimaryKey()) {
      // Add the new column if this is a change and it wasn't part of they key
      // before. Remove it if it now isn't part of the key and it was before
      StringBuilder primaryKeyStatement = new StringBuilder()
        .append("ALTER TABLE ")
        .append(schemaNamePrefix())
        .append(table.getName())
        .append(" ADD ")
        .append(buildPrimaryKeyConstraint(table.getName(), namesOfColumns(primaryKeysForTable(table))));

      statements.add(primaryKeyStatement.toString());
    }

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, final Column oldColumn, Column newColumn) {
    List<String> statements = new ArrayList<>();

    // If we are removing the autonumber then we must completely rebuild the table
    // without the autonumber (identity) property before we do anything else
    // PLEASE NOTE - THIS DOES NOT COPY VIEWS OR INDEXES -- See WEB-23759
    if (oldColumn.isAutoNumbered() && !newColumn.isAutoNumbered()) {
      // Create clone of table
      Table clone = table(table.getName() + "Clone")
                    .columns(table.columns().toArray(new Column[table.columns().size()]));
      Collection<String> cloneTableStatements = tableDeploymentStatements(clone);
      statements.addAll(tableDeploymentStatements(clone));

      // Meta data switch of the data from the original table to the cloned table
      statements.add("ALTER TABLE " + schemaNamePrefix() + table.getName() + " SWITCH TO " + schemaNamePrefix() + clone.getName());

      // Drop original table
      statements.add("DROP TABLE " + schemaNamePrefix() + table.getName());

      // Rename clone to make it look like the original table
      statements.add(String.format("EXECUTE sp_rename '%s%s', '%s%s'",
        schemaNamePrefix(),
        clone.getName(),
        schemaNamePrefix(),
        table.getName()
      ));
      if (containsPrimaryKeyConstraint(cloneTableStatements, clone.getName())) {
        statements.add(String.format("EXECUTE sp_rename '%s%s_PK', '%s%s_PK', 'OBJECT'",
          schemaNamePrefix(),
          clone.getName(),
          schemaNamePrefix(),
          table.getName()
        ));
      }
    }

    // build the old version of the table
    Table oldTable = oldTableForChangeColumn(table, oldColumn, newColumn);

    // If we are dropping or changing a column, drop indexes containing that column
    for (Index index : oldTable.indexes()) {
      for (String column : index.columnNames()) {
        if (column.equalsIgnoreCase(oldColumn.getName())) {
          statements.addAll(indexDropStatements(oldTable, index));
        }
      }
    }

    // Drop any defaults for the old column
    if (StringUtils.isNotBlank(oldColumn.getDefaultValue()))
      statements.add(dropDefaultForColumn(table, oldColumn));

    // -- Rename the column if we need to
    //
    if (!oldColumn.getName().equals(newColumn.getName())) {
      statements.add(String.format("EXEC sp_rename '%s%s.%s', '%s', 'COLUMN'",
        schemaNamePrefix(),
        table.getName(),
        oldColumn.getName(),
        newColumn.getName()
      ));
    }

    // Drop and re-create the primary key if either new or old columns are part of the PK
    boolean recreatePrimaryKey = oldColumn.isPrimaryKey() || newColumn.isPrimaryKey();

    // only drop if there actually was a PK though...
    if (recreatePrimaryKey && !primaryKeysForTable(oldTable).isEmpty()) {
      statements.add(dropPrimaryKey(table));
    }

    statements.add(new StringBuilder()
      .append("ALTER TABLE ")
      .append(schemaNamePrefix())
      .append(table.getName())
      .append(" ALTER COLUMN ")
      .append(newColumn.getName())
      .append(' ')
      .append(sqlRepresentationOfColumnType(table, newColumn, true))
      .toString());

    // Create the indexes we dropped previously
    for (Index index : table.indexes()) {
      for (String column : index.columnNames()) {
        if (column.equalsIgnoreCase(newColumn.getName())) {
          statements.addAll(addIndexStatements(table, index));
        }
      }
    }

    List<Column> primaryKeyColumns = primaryKeysForTable(table);

    // Recreate the primary key if necessary
    if (recreatePrimaryKey && !primaryKeyColumns.isEmpty()) {
      statements.add(new StringBuilder()
        .append("ALTER TABLE ").append(schemaNamePrefix()).append(table.getName()).append(" ADD ")
        .append(buildPrimaryKeyConstraint(table.getName(), namesOfColumns(primaryKeyColumns)))
        .toString()
      );
    }

    return statements;
  }


  /**
   * @param statements the statements to check for the primary key constraint
   * @param tableName the table name which is expected to make up the pk constraint name
   * @return true if the statements contain tableName_PK
   */
  private boolean containsPrimaryKeyConstraint(Collection<String> statements, String tableName) {
    for (String s : statements) {
      if (s.contains(tableName + "_PK")) {
        return true;
      }
    }

    return false;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(org.alfasoftware.morf.metadata.Table, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> statements = new ArrayList<>();

    if (!oldPrimaryKeyColumns.isEmpty()) {
      statements.add(dropPrimaryKey(table));
    }

    if (!newPrimaryKeyColumns.isEmpty()) {
      statements.add(new StringBuilder()
        .append("ALTER TABLE ").append(schemaNamePrefix()).append(table.getName()).append(" ADD ")
        .append(buildPrimaryKeyConstraint(table.getName(), newPrimaryKeyColumns))
        .toString()
      );
    }

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, final Column column) {
    List<String> statements = new ArrayList<>();

    // Drop any defaults for the old column
    if (StringUtils.isNotBlank(column.getDefaultValue()))
      statements.add(dropDefaultForColumn(table, column));

    // Drop the primary key if the column is part of the primary key and we are dropping the column
    boolean recreatePrimaryKey = column.isPrimaryKey();
    if (recreatePrimaryKey) {
      statements.add(dropPrimaryKey(table));
    }

    // We can't use the superclass method as we need to make sure we
    // modify the correct schema in the database
    StringBuilder statement = new StringBuilder()
      .append("ALTER TABLE ")
      .append(schemaNamePrefix())
      .append(table.getName())
      .append(" DROP COLUMN ")
      .append(column.getName());

    statements.add(statement.toString());

    List<Column> primaryKeyColumns = primaryKeysForTable(table);

    // Recreate the primary key if necessary
    if (recreatePrimaryKey && !primaryKeyColumns.isEmpty()) {
      statements.add(new StringBuilder()
        .append("ALTER TABLE ").append(schemaNamePrefix()).append(table.getName()).append(" ADD ")
        .append(buildPrimaryKeyConstraint(table.getName(), namesOfColumns(primaryKeyColumns)))
        .toString()
      );
    }

    return statements;
  }


  /**
   * @param table
   * @param statements
   */
  private String dropPrimaryKey(Table table) {
    StringBuilder dropPkStatement = new StringBuilder();
    dropPkStatement.append("ALTER TABLE ").append(schemaNamePrefix()).append(table.getName()).append(" DROP ");
    dropPkStatement.append("CONSTRAINT [");
    dropPkStatement.append(undecorateName(table.getName()));
    dropPkStatement.append("_PK]");

    return dropPkStatement.toString();
  }


  /**
   * Return the SQL representation for the column on the table.
   *
   * @see #sqlRepresentationOfColumnType(Table, Column)
   * @param table The table
   * @param column The column
   * @param includeDefaultWithValues Whether to include the WITH VALUES clause.
   *          This is only applicable on ALTER statements.
   */
  private String sqlRepresentationOfColumnType(Table table, Column column, boolean includeDefaultWithValues) {
    StringBuilder suffix = new StringBuilder(column.isNullable() ? "" : " NOT NULL");

    if (StringUtils.isNotEmpty(column.getDefaultValue())) {
      suffix.append(" CONSTRAINT " + getColumnDefaultConstraintName(table, column) + " DEFAULT " + getSqlFrom(new FieldLiteral(column.getDefaultValue(), column.getType()))) ;
      suffix.append(includeDefaultWithValues ? " WITH VALUES" : "");
    }

    return getColumnRepresentation(column.getType(), column.getWidth(), column.getScale()) + suffix;
  }


  /**
   * Returns SQL to drop the DEFAULT constraint for a particular column on a
   * particular table.
   *
   * @param table The name of the table on which the column resides.
   * @param column The name of the column.
   * @return SQL to drop the DEFAULT constraint for the specified column on the
   *         specified table.
   */
  private String dropDefaultForColumn(final Table table, final Column column) {

    // This SQL came from http://stackoverflow.com/questions/8641954/how-to-drop-column-with-constraint
    return dropDefaultForColumnSql
        .replace("{table}", table.getName())
        .replace("{column}", column.getName());
  }


  /**
   * Get the name of the DEFAULT constraint for a column.
   *
   * @param table The table on which the column exists.
   * @param column The column to get the name for.
   * @return The name of the DEFAULT constraint for the column on the table.
   */
  private String getColumnDefaultConstraintName(final Table table, final Column column) {
    return table.getName() + "_" + column.getName() + "_DF";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForYYYYMMDDToDate(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForYYYYMMDDToDate(Function function) {
    return "CONVERT(date, " + getSqlFrom(function.getArguments().get(0)) + ", 112)";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmdd(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmdd(Function function) {
    return String.format("CONVERT(VARCHAR(8),%s, 112)", getSqlFrom(function.getArguments().get(0)));
  };


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    return String.format("REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(19),%s, 120),'-',''), ':', ''), ' ', '')", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "GETUTCDATE()";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return String.format("DATEDIFF(DAY, %s, %s)", getSqlFrom(fromDate), getSqlFrom(toDate));
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
         "DATEDIFF(MONTH, %s, %s) + " +
         "CASE " +
          "WHEN %s > %s THEN " +
            "CASE " +
             "WHEN DATEPART(day, %s) <= DATEPART(day, %s) OR MONTH(%s) <> MONTH(DATEADD(DAY, 1, %s)) THEN 0 " +
             "ELSE -1 " +
            "END " +
          "ELSE " +
            "CASE " +
             "WHEN DATEPART(day, %s) <= DATEPART(day, %s) OR MONTH(%s) <> MONTH(DATEADD(DAY, 1, %s)) THEN 0 " +
             "ELSE 1 " +
            "END " +
         "END " +
       "END ",
       fromDateStr, toDateStr,
       fromDateStr, toDateStr,
       toDateStr, fromDateStr,
       fromDateStr, toDateStr, toDateStr, toDateStr,
       toDateStr, fromDateStr, fromDateStr, fromDateStr
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLeftPad(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForLeftPad(AliasedField field, AliasedField length, AliasedField character) {
    String strField = getSqlFrom(field);
    String strLength = getSqlFrom(length);
    String strCharacter = getSqlFrom(character);

    return String.format("CASE " +
                           "WHEN LEN(%s) > %s THEN " +
                             "LEFT(%s, %s) " +
                           "ELSE " +
                             "RIGHT(REPLICATE(%s, %s) + %s, %s) " +
                         "END",
                         strField, strLength,
                         strField, strLength,
                         strCharacter, strLength, strField, strLength);
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
          result.append("(CASE WHEN ").append(sqlFromField).append(" IS NULL THEN 0 ELSE 1 END), ");
          break;
        case LAST:
          result.append("(CASE WHEN ").append(sqlFromField).append(" IS NULL THEN 1 ELSE 0 END), ");
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
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlforLength(Function)
   */
  @Override
  protected String getSqlforLength(Function function){
    return String.format("LEN(%s)", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddDays(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddDays(Function function) {
    return String.format(
      "DATEADD(dd, %s, %s)",
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
      "DATEADD(month, %s, %s)",
      getSqlFrom(function.getArguments().get(1)),
      getSqlFrom(function.getArguments().get(0))
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForMod(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForMod(Function function) {
    return String.format("%s %% %s", getSqlFrom(function.getArguments().get(0)), getSqlFrom(function.getArguments().get(1)));
  }


  @Override
  public Collection<String> renameTableStatements(Table fromTable, Table toTable) {
    String from = fromTable.getName();
    String to = toTable.getName();
    Builder<String> builder = ImmutableList.<String>builder();

    builder.add("IF EXISTS (SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'" + from + "_version_DF') AND type = (N'D')) exec sp_rename N'" + from + "_version_DF', N'" + to + "_version_DF'");

    if (!primaryKeysForTable(fromTable).isEmpty()) {
      builder.add("sp_rename N'" + from + "." + from + "_PK', N'" + to + "_PK', N'INDEX'");
    }

    builder.add("sp_rename N'" + from + "', N'" + to + "'");

    return builder.build();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.FieldLiteral)
   */
  @Override
  protected String getSqlFrom(FieldLiteral field) {
    switch (field.getDataType()) {
      case DATE:
        // SQL server does not support ISO standard date literals.
        return String.format("'%s'", field.getValue());
      default:
        return super.getSqlFrom(field);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.joda.time.LocalDate)
   */
  @Override
  protected String getSqlFrom(LocalDate literalValue) {
    // SQL server does not support ISO standard date literals.
    return String.format("'%s'", literalValue.toString("yyyy-MM-dd"));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameIndexStatements(org.alfasoftware.morf.metadata.Table, java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.of(String.format("sp_rename N'%s%s.%s', N'%s', N'INDEX'", schemaNamePrefix(), table.getName(), fromIndexName, toIndexName));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return  String.format("SUBSTRING(REPLACE(CONVERT(varchar(255),NEWID()),'-',''), 1, %s)",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.SelectFirstStatement)
   */
  @Override
  protected String getSqlFrom(SelectFirstStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT TOP 1 ");

    // Start by adding the field
    result.append(getSqlFrom(stmt.getFields().get(0)));

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);
    appendOrderBy(result, stmt);

    return result.toString().trim();
  }


  @Override
  protected String selectStatementPostStatementDirectives(SelectStatement selectStatement) {
    if (selectStatement.getHints().isEmpty()) {
      return super.selectStatementPreFieldDirectives(selectStatement);
    }

    StringBuilder builder = new StringBuilder().append(" OPTION(");

    boolean comma = false;

    for (Hint hint : selectStatement.getHints()) {
      if (SUPPORTED_HINTS.contains(hint.getClass())) {
        if (comma) {
          builder.append(", ");
        } else {
          comma = true;
        }
      }
      if (hint instanceof OptimiseForRowCount) {
        builder.append("FAST " + ((OptimiseForRowCount)hint).getRowCount());
      }
      if (hint instanceof UseIndex) {
        UseIndex useIndex = (UseIndex) hint;
        builder.append("TABLE HINT(")
          // Includes schema name - see https://msdn.microsoft.com/en-us/library/ms181714.aspx
          .append(StringUtils.isEmpty(useIndex.getTable().getAlias()) ? schemaNamePrefix(useIndex.getTable()) + useIndex.getTable().getName() : useIndex.getTable().getAlias())
          .append(", INDEX(" + useIndex.getIndexName() + ")")
          .append(")");
      }
      if (hint instanceof UseImplicitJoinOrder) {
        builder.append("FORCE ORDER");
      }
    }

    return builder.append(")").toString();
  }


  /**
   * SQL server places a shared lock on a record when it is selected without doing anything else (no MVCC)
   * so no need to specify a lock mode.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getForUpdateSql()
   * @see http://stackoverflow.com/questions/10935850/when-to-use-select-for-update
   */
  @Override
  protected String getForUpdateSql() {
    return StringUtils.EMPTY;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#supportsWindowFunctions()
   */
  @Override
  public boolean supportsWindowFunctions() {
    return false; // SqlServer does not have full support for window functions before 2012
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.element.WindowFunction)
   */
  @Override
  protected String getSqlFrom(final WindowFunction windowFunctionField) {
    throw new UnsupportedOperationException(this.getClass().getSimpleName()+" does not support window functions.");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForLastDayOfMonth
   */
  @Override
  protected String getSqlForLastDayOfMonth(AliasedField date) {
    return "DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0," + getSqlFrom(date) + ")+1,0))";
  }


  /**
   * @see SqlDialect#getDeleteLimitPreFromClause(int)
   */
  @Override
  protected Optional<String> getDeleteLimitPreFromClause(int limit) {
    return Optional.of("TOP (" + limit + ")");
  }
}