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

package org.alfasoftware.morf.jdbc.oracle;

import static org.alfasoftware.morf.metadata.DataType.DECIMAL;
import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.Hint;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.OptimiseForRowCount;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UseImplicitJoinOrder;
import org.alfasoftware.morf.sql.UseIndex;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Implements Oracle specific statement generation.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class OracleDialect extends SqlDialect {

  private static final Log log = LogFactory.getLog(OracleDialect.class);

  /**
   * Used as the alias for the select statement in merge statements.
   */
  private static final String MERGE_SOURCE_ALIAS = "xmergesource";

  /**
   * Database platforms may order nulls first or last. My SQL always orders nulls first, Oracle defaults to ordering nulls last.
   * Fortunately on Oracle it is possible to specify that nulls should be ordered first.
   */
  public static final String DEFAULT_NULL_ORDER = "NULLS FIRST";


  /**
   * Creates an instance of the Oracle dialect.
   *
   * @param schemaName The database schema name.
   */
  public OracleDialect(String schemaName) {
    super(schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#truncateTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> truncateTableStatements(Table table) {
    String mainTruncate = "TRUNCATE TABLE " + schemaNamePrefix() + table.getName();
    if (table.isTemporary()) {
      return Arrays.asList(mainTruncate);
    } else {
      return Arrays.asList(mainTruncate + " REUSE STORAGE");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#deleteAllFromTableStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> deleteAllFromTableStatements(Table table) {
    return Arrays.asList("delete from " + schemaNamePrefix() + table.getName());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#tableDeploymentStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> internalTableDeploymentStatements(Table table) {
    return tableDeploymentStatements(table, false);
  }


  private Collection<String> tableDeploymentStatements(Table table, boolean asSelect) {
    return ImmutableList.<String>builder()
      .add(createTableStatement(table, asSelect))
      .addAll(buildRemainingStatementsAndComments(table))
      .build();
  }


  private String createTableStatement(Table table, boolean asSelect) {
    // Create the table deployment statement
    StringBuilder createTableStatement = new StringBuilder();
    createTableStatement.append("CREATE ");

    if (table.isTemporary()) {
      createTableStatement.append("GLOBAL TEMPORARY ");
    }

    createTableStatement.append("TABLE ");

    String truncatedTableName = truncatedTableName(table.getName());

    createTableStatement.append(schemaNamePrefix());
    createTableStatement.append(truncatedTableName);
    createTableStatement.append(" (");

    boolean first = true;
    for (Column column : table.columns()) {
      if (!first) {
        createTableStatement.append(", ");
      }

      createTableStatement.append(column.getName());
      if (asSelect) {
        createTableStatement.append(" " + sqlRepresentationOfColumnType(column, true, true, false));
      } else {
        createTableStatement.append(" " + sqlRepresentationOfColumnType(column));
      }

      first = false;
    }

    // Put on the primary key constraint
    if (!primaryKeysForTable(table).isEmpty()) {
      createTableStatement.append(", ");
      createTableStatement.append(primaryKeyConstraint(table));
    }

    createTableStatement.append(")");

    if (table.isTemporary()) {
      createTableStatement.append(" ON COMMIT PRESERVE ROWS");
    }

    if (asSelect) {
      createTableStatement.append(" PARALLEL NOLOGGING");
    }

    return createTableStatement.toString();
  }


  private Collection<String> createColumnComments(Table table) {

    List<String> columnComments = Lists.newArrayList();

    for (Column column : table.columns()) {
      columnComments.add(columnComment(column, truncatedTableName(table.getName())));
    }

    return columnComments;
  }


  private Column findAutonumberedColumn(Table table) {
    Column sequence = null;
    for (Column column : table.columns()) {

      if (column.isAutoNumbered()) {
        sequence = column;
        break;
      }
    }

    return sequence;
  }

  /**
   * Adds the table name into comments.
   */
  private String commentOnTable(String truncatedTableName) {
    return "COMMENT ON TABLE " + schemaNamePrefix() + truncatedTableName + " IS 'REALNAME:[" + truncatedTableName + "]'";
  }


  /**
   * CONSTRAINT DEF_PK PRIMARY KEY (X, Y, Z)
   */
  private String primaryKeyConstraint(String tableName, List<String> newPrimaryKeyColumns) {
    // truncate down to 27, since we add _PK to the end
    return "CONSTRAINT "+ primaryKeyConstraintName(tableName) + " PRIMARY KEY (" + Joiner.on(", ").join(newPrimaryKeyColumns) + ")";
  }


  /**
   * CONSTRAINT DEF_PK PRIMARY KEY (X, Y, Z)
   */
  private String primaryKeyConstraint(Table table) {
    return primaryKeyConstraint(table.getName(), namesOfColumns(primaryKeysForTable(table)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> dropStatements(Table table) {
    for (Column column : table.columns()) {
      if (column.isAutoNumbered()) {
        return Arrays.asList(
          dropTrigger(table),
          "DROP TABLE " + schemaNamePrefix() + table.getName(),
          dropSequence(table));
      }
    }
    return Arrays.asList("DROP TABLE " + schemaNamePrefix() + table.getName());
  }


  /**
   * Returns a SQL statement to safely drop a sequence, if it exists.
   *
   * @param table Table for which the sequence should be dropped.
   * @return SQL string.
   */
  private String dropSequence(Table table) {
    String sequenceName = sequenceName(table.getName());
    return new StringBuilder("DECLARE \n")
      .append("  query CHAR(255); \n")
      .append("BEGIN \n")
      .append("  select queryField into query from SYS.DUAL D left outer join (\n")
      .append("    select concat('drop sequence ").append(schemaNamePrefix()).append("', sequence_name) as queryField \n")
      .append("    from ALL_SEQUENCES S \n")
      .append("    where S.sequence_owner='").append(getSchemaName().toUpperCase()).append("' AND S.sequence_name = '").append(sequenceName.toUpperCase()).append("' \n")
      .append("  ) on 1 = 1; \n")
      .append("  IF query is not null THEN \n")
      .append("    execute immediate query; \n")
      .append("  END IF; \n")
      .append("END;")
      .toString();
  }


  /**
   * Returns a SQL statement to create a sequence for a table's autonumber column
   *
   * @param table Table for which the sequence should be created.
   * @param onColumn The autonumber column.
   * @return SQL string.
   */
  private String createNewSequence(Table table, Column onColumn) {
    int autoNumberStart = onColumn.getAutoNumberStart() == -1 ? 1 : onColumn.getAutoNumberStart();
    return new StringBuilder("CREATE SEQUENCE ")
      .append(schemaNamePrefix())
      .append(sequenceName(table.getName()))
      .append(" START WITH ")
      .append(autoNumberStart)
      .append(" CACHE 2000")
      .toString();
  }


  /**
   * Returns a SQL statement to create a sequence for a table's autonumber column, where
   * the sequence should start from the greater of either the autonumber column's start value
   * or the maximum value for that column existing in the table.
   *
   * @param table Table for which the sequence should be created.
   * @param onColumn The autonumber column.
   * @return  SQL string.
   */
  private String createSequenceStartingFromExistingData(Table table, Column onColumn) {
    String tableName = schemaNamePrefix() + truncatedTableName(table.getName());
    String sequenceName = schemaNamePrefix() + sequenceName(table.getName());
    return new StringBuilder("DECLARE query CHAR(255); \n")
      .append("BEGIN \n")
      .append("  SELECT 'CREATE SEQUENCE ").append(sequenceName).append(" START WITH ' || TO_CHAR(GREATEST(").append(onColumn.getAutoNumberStart()).append(", MAX(id)+1)) || ' CACHE 2000' INTO QUERY FROM \n")
      .append("    (SELECT MAX(").append(onColumn.getName()).append(") AS id FROM ").append(tableName).append(" UNION SELECT 0 AS id FROM SYS.DUAL); \n")
      .append("  EXECUTE IMMEDIATE query; \n")
      .append("END;")
      .toString();
  }


  /**
   * Returns a list of SQL statement to create a trigger to populate a table's autonumber column
   * from a sequence.
   *
   * @param table Table for which the trigger should be created.
   * @param onColumn The autonumber column.
   * @return SQL string list.
   */
  private List<String> createTrigger(Table table, Column onColumn) {
    List<String> createTriggerStatements = new ArrayList<>();
    createTriggerStatements.add(String.format("ALTER SESSION SET CURRENT_SCHEMA = %s", getSchemaName()));
    String tableName = truncatedTableName(table.getName());
    String sequenceName = sequenceName(table.getName());
    String triggerName = schemaNamePrefix() + triggerName(table.getName());
    createTriggerStatements.add(new StringBuilder("CREATE TRIGGER ").append(triggerName).append(" \n")
      .append("BEFORE INSERT ON ").append(tableName).append(" FOR EACH ROW \n")
      .append("BEGIN \n")
      .append("  IF (:new.").append(onColumn.getName()).append(" IS NULL) THEN \n")
      .append("    SELECT ").append(sequenceName).append(".nextval \n")
      .append("    INTO :new.").append(onColumn.getName()).append(" \n")
      .append("    FROM DUAL; \n")
      .append("  END IF; \n")
      .append("END;")
      .toString());
    return createTriggerStatements;
  }


  /**
   * Create statements to safely drop a trigger, if it exists.
   *
   * @param table Table for which the trigger should be dropped.
   * @return Query string.
   */
  private String dropTrigger(Table table) {
    String triggerName =  schemaNamePrefix() + triggerName(table.getName());
    return new StringBuilder()
      .append("DECLARE \n")
      .append("  e exception; \n")
      .append("  pragma exception_init(e,-4080); \n")
      .append("BEGIN \n")
      .append("  EXECUTE IMMEDIATE 'DROP TRIGGER ").append(triggerName).append("'; \n")
      .append("EXCEPTION \n")
      .append("  WHEN e THEN \n")
      .append("    null; \n")
      .append("END;")
      .toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#dropStatements(org.alfasoftware.morf.metadata.View)
   */
  @Override
  public Collection<String> dropStatements(View view) {
    return Arrays.asList("BEGIN FOR i IN (SELECT null FROM all_views WHERE OWNER='" + getSchemaName().toUpperCase() + "' AND VIEW_NAME='" + view.getName().toUpperCase() + "') LOOP EXECUTE IMMEDIATE 'DROP VIEW " + schemaNamePrefix() + view.getName() + "'; END LOOP; END;");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#postInsertWithPresetAutonumStatements(org.alfasoftware.morf.metadata.Table, boolean)
   */
  @Override
  public void postInsertWithPresetAutonumStatements(Table table, SqlScriptExecutor executor,Connection connection, boolean insertingUnderAutonumLimit) {
    // When we know we're definitely under the current next sequence value, there's no need to do anything.
    if (insertingUnderAutonumLimit) {
      return;
    }
    executor.execute(rebuildSequenceAndTrigger(table,getAutoIncrementColumnForTable(table)),connection);
  }


  /**
   * If the table has an auto-numbered column, rebuild its sequence and trigger.
   *
   * @param table The {@link Table}.
   * @return The SQL statements to run.
   */
  private Collection<String> rebuildSequenceAndTrigger(Table table,Column sequence) {
    // This requires drop/create trigger/sequence privileges so we avoid where we can.
    if(sequence == null) {
      return Lists.newArrayList(dropTrigger(table));
    }

    List<String> statements = new ArrayList<>();
    statements.add(dropTrigger(table));
    statements.add(dropSequence(table));
    statements.add(createSequenceStartingFromExistingData(table, sequence));
    statements.addAll(createTrigger(table, sequence));
    return statements;
  }


  /**
   * Form the standard name for a table's primary key constraint.
   *
   * @param tableName Name of the table for which the primary key constraint name is required.
   * @return Name of constraint.
   */
  private String primaryKeyConstraintName(String tableName) {
    return truncatedTableNameWithSuffix(tableName, "_PK");
  }


  /**
   * Form the standard name for a table's autonumber sequence.
   *
   * @param tableName Name of the table for which the sequence name is required.
   * @return Name of sequence.
   */
  private String sequenceName(String tableName) {
    return truncatedTableNameWithSuffix(tableName, "_SQ").toUpperCase();
  }


  /**
   * Form the standard name for a table's autonumber trigger.
   *
   * @param tableName Name of the table for which the trigger name is required.
   * @return Name of trigger.
   */
  private String triggerName(String tableName) {
    return truncatedTableNameWithSuffix(tableName, "_TG").toUpperCase();
  }


  /**
   * Truncate table names to 30 characters since this is the maximum supported by Oracle.
   */
  private String truncatedTableName(String tableName) {
    return StringUtils.substring(tableName, 0, 30);
  }


  /**
   * Truncate table names to 27 characters, then add a 3 character suffix since 30 is the maximum supported by Oracle.
   */
  private String truncatedTableNameWithSuffix(String tableName, String suffix) {
    return StringUtils.substring(tableName, 0, 27) + StringUtils.substring(suffix, 0, 3);
  }


  /**
   * Turn a string value into an SQL string literal which has that value.
   * <p>
   * We use {@linkplain StringUtils#isEmpty(String)} because we want to
   * differentiate between a single space and an empty string.
   * </p>
   * <p>
   * This is necessary because char types cannot be null and must contain
   * a single space.
   * <p>
   *
   * @param literalValue the literal value of the string.
   * @return SQL String Literal
   */
  @Override
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }

    return String.format("N'%s'", StringEscapeUtils.escapeSql(literalValue));
  }

  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getColumnRepresentation(org.alfasoftware.morf.metadata.DataType,
   *      int, int)
   */
  @Override
  protected String getColumnRepresentation(DataType dataType, int width, int scale) {
    switch (dataType) {
      case STRING:
        // the null suffix here is potentially controversial, since oracle does
        // not distinguish between null and blank.
        // obey the metadata for now, since this makes the process reversible.
        return String.format("NVARCHAR2(%d)", width);

      case DECIMAL:
        return String.format("DECIMAL(%d,%d)", width, scale);

      case DATE:
        return "DATE";

      case BOOLEAN:
        return "DECIMAL(1,0)";

      case INTEGER:
        return "INTEGER";

      case BIG_INTEGER:
        return "NUMBER(19)";

      case BLOB:
        return "BLOB";

      case CLOB:
        return  "NCLOB";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#prepareBooleanParameter(org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement, java.lang.Boolean, org.alfasoftware.morf.sql.element.SqlParameter)
   */
  @Override
  protected void prepareBooleanParameter(NamedParameterPreparedStatement statement, Boolean boolVal, SqlParameter parameter) throws SQLException {
    statement.setBigDecimal(
      parameter(parameter.getImpliedName()).type(DECIMAL).width(1),
      boolVal == null ? null : boolVal ? BigDecimal.ONE : BigDecimal.ZERO
    );
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
    return DatabaseType.Registry.findByIdentifier(Oracle.IDENTIFIER);
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
    return StringUtils.join(sql, " || ");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForIsNull(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForIsNull(Function function) {
    return "nvl(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ") ";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#buildSQLToStartTracing(java.lang.String)
   */
  @Override
  public List<String> buildSQLToStartTracing(String identifier) {
    return Arrays.asList("ALTER SESSION SET tracefile_identifier = '" + identifier + "'","ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT FOREVER, LEVEL 8'");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#buildSQLToStopTracing()
   */
  @Override
  public List<String> buildSQLToStopTracing() {
    return Arrays.asList("ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT OFF'");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlDialect#defaultNullOrder()
   */
  @Override
  protected String defaultNullOrder() {
    return DEFAULT_NULL_ORDER;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#addIndexStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> addIndexStatements(Table table, Index index) {
    return ImmutableList.of(
      // when adding indexes to existing tables, use PARALLEL NOLOGGING to efficiently build the index
      indexDeploymentStatement(table, index) + " PARALLEL NOLOGGING",
      indexPostDeploymentStatements(index)
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDeploymentStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  protected String indexDeploymentStatement(Table table, Index index) {
    StringBuilder createIndexStatement = new StringBuilder();

    // Specify the preamble
    createIndexStatement.append("CREATE ");
    if (index.isUnique()) {
      createIndexStatement.append("UNIQUE ");
    }

    // Name the index
    createIndexStatement
      .append("INDEX ")
      .append(schemaNamePrefix())
      .append(index.getName())

      // Specify which table the index is over
      .append(" ON ")
      .append(schemaNamePrefix())
      .append(truncatedTableName(table.getName()))

      // Specify the fields that are used in the index
      .append(" (")
      .append(Joiner.on(", ").join(index.columnNames()))
      .append(")");

    return createIndexStatement.toString();
  }


  /**
   * Generate the SQL to alter the index back to NOPARALLEL LOGGING.
   *
   * @param index The index we want to alter.
   * @return The SQL to alter the index.
   */
  private String indexPostDeploymentStatements(Index index) {
    return new StringBuilder()
      .append("ALTER INDEX ")
      .append(schemaNamePrefix())
      .append(index.getName())
      .append(" NOPARALLEL LOGGING")
      .toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableAddColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableAddColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    String truncatedTableName = truncatedTableName(table.getName());

    result.add(String.format("ALTER TABLE %s%s ADD (%s %s)",
      schemaNamePrefix(),
      truncatedTableName,
      column.getName(),
      sqlRepresentationOfColumnType(column, true)
    ));

    result.add(columnComment(column, truncatedTableName));

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#changePrimaryKeyColumns(java.lang.String, java.util.List, java.util.List)
   */
  @Override
  public Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    List<String> result = new ArrayList<>();
    String tableName = table.getName();
    // Drop existing primary key and make columns not null
    if (!oldPrimaryKeyColumns.isEmpty()) {
      result.add(dropPrimaryKeyConstraint(tableName));
      for (String columnName : oldPrimaryKeyColumns) {
        result.add(makeColumnNotNull(tableName, columnName));
      }
    }

    //Create new primary key constraint
    if (!newPrimaryKeyColumns.isEmpty()) {
      result.add(generatePrimaryKeyStatement(newPrimaryKeyColumns, table.getName()));
    }

    return result;
  }


  /**
   * It returns the SQL statement to make the column not null. The function
   * catches the exception with SQL error code ORA-01442: column to be modified
   * to NOT NULL is already NOT NULL.
   *
   * <p>Example of the generated SQL statement:</p>
   * <pre>
   * DECLARE
   *   e EXCEPTION;
   *   pragma exception_init(e,-1442);
   * BEGIN
   *   EXECUTE immediate 'alter table sandbox.genericglposting modify (version not null)';
   * EXCEPTION
   * WHEN e THEN
   *   NULL;
   * END;
   * </pre>
   *
   * @param tableName Table name to be altered
   * @param columnName Column name to make it not null
   * @return The SQL statement to make the column not null
   */
  private String makeColumnNotNull(String tableName, String columnName) {
    StringBuilder statement = new StringBuilder();
    statement.append("DECLARE \n").append("  e EXCEPTION; \n").append("  pragma exception_init(e,-1442); \n").append("BEGIN \n")
        .append("  EXECUTE immediate 'ALTER TABLE ").append(schemaNamePrefix()).append(tableName).append(" MODIFY (")
        .append(columnName).append(" NOT NULL)'; \n").append("EXCEPTION \n").append("WHEN e THEN \n").append("  NULL; \n")
        .append("END;");
    if (log.isDebugEnabled()) {
      log.debug(statement.toString());
    }
    return statement.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableChangeColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn) {
    List<String> result = new ArrayList<>();

    Table oldTable = oldTableForChangeColumn(table, oldColumn, newColumn);

    String truncatedTableName = truncatedTableName(oldTable.getName());

    boolean recreatePrimaryKey = oldColumn.isPrimaryKey() || newColumn.isPrimaryKey();

    if (recreatePrimaryKey && !primaryKeysForTable(oldTable).isEmpty()) {
      result.add(dropPrimaryKeyConstraint(truncatedTableName));
    }

    for (Index index : oldTable.indexes()) {
      for (String column : index.columnNames()) {
        if (column.equalsIgnoreCase(oldColumn.getName())) {
          result.addAll(indexDropStatements(oldTable, index));
        }
      }
    }

    if (!newColumn.getName().equals(oldColumn.getName())) {
      result.add("ALTER TABLE " + schemaNamePrefix() + truncatedTableName + " RENAME COLUMN " + oldColumn.getName() + " TO " + newColumn.getName());
    }

    boolean includeNullability = newColumn.isNullable() != oldColumn.isNullable();
    boolean includeColumnType = newColumn.getType() != oldColumn.getType() || newColumn.getWidth() != oldColumn.getWidth() || newColumn
        .getScale() != oldColumn.getScale();
    String sqlRepresentationOfColumnType = sqlRepresentationOfColumnType(newColumn, includeNullability, true, includeColumnType);

    if (!StringUtils.isBlank(sqlRepresentationOfColumnType)) {
      StringBuilder statement = new StringBuilder()
        .append("ALTER TABLE ")
        .append(schemaNamePrefix())
        .append(truncatedTableName)
        .append(" MODIFY (")
        .append(newColumn.getName())
        .append(' ')
        .append(sqlRepresentationOfColumnType)
        .append(")");

      result.add(statement.toString());
    }

    if (recreatePrimaryKey && !primaryKeysForTable(table).isEmpty()) {
      result.add(generatePrimaryKeyStatement(namesOfColumns(SchemaUtils.primaryKeysForTable(table)), truncatedTableName));
    }

    for (Index index : table.indexes()) {
      for (String column : index.columnNames()) {
        if (column.equalsIgnoreCase(newColumn.getName())) {
          result.addAll(addIndexStatements(table, index));
        }
      }
    }

    result.add(columnComment(newColumn, truncatedTableName));

    return result;
  }


  private String generatePrimaryKeyStatement(List<String> columnNames, String tableName) {
    StringBuilder primaryKeyStatement = new StringBuilder();
    primaryKeyStatement.append("ALTER TABLE ")
    .append(schemaNamePrefix())
    .append(tableName)
    .append(" ADD ")
    .append(primaryKeyConstraint(tableName, columnNames));
    return primaryKeyStatement.toString();
  }


  /**
   * ALTER TABLE ABC.DEF DROP PRIMARY KEY DROP INDEX
   */
  private String dropPrimaryKeyConstraint(String tableName) {
    // Drop the associated unique index at the same time
    return "ALTER TABLE " + schemaNamePrefix() + tableName + " DROP PRIMARY KEY DROP INDEX";
  }


  /**
   * Build the comment comment that allows the metadata reader to determine the correct lower case table names and types.
   */
  private String columnComment(Column column, String tableName) {
    StringBuilder comment = new StringBuilder ("COMMENT ON COLUMN " + schemaNamePrefix() + tableName + "." + column.getName() + " IS 'REALNAME:[" + column.getName() + "]/TYPE:[" + column.getType().toString() + "]");

    if (column.isAutoNumbered()) {
      int autoNumberStart = column.getAutoNumberStart() == -1 ? 1 : column.getAutoNumberStart();
      comment.append("/AUTONUMSTART:[" + autoNumberStart + "]");
    }

    comment.append("'");

    return comment.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#alterTableDropColumnStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public Collection<String> alterTableDropColumnStatements(Table table, Column column) {
    List<String> result = new ArrayList<>();

    String truncatedTableName = truncatedTableName(table.getName());

    StringBuilder statement = new StringBuilder()
    .append("ALTER TABLE ")
    .append(schemaNamePrefix())
    .append(truncatedTableName)
    .append(" SET UNUSED") // perform a logical (rather than physical) delete of the row for performance reasons.
    .append(" (").append(column.getName()).append(")");

    result.add(statement.toString());

    return result;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#indexDropStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Index)
   */
  @Override
  public Collection<String> indexDropStatements(Table table, Index indexToBeRemoved) {
    StringBuilder statement = new StringBuilder();

    statement.append("DROP INDEX ")
    .append(schemaNamePrefix())
    .append(indexToBeRemoved.getName());

    return Arrays.asList(statement.toString());
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
    return String.format("to_number(to_char(%s, 'yyyymmdd'))",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDateToYyyymmddHHmmss(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForDateToYyyymmddHHmmss(Function function) {
    return String.format("to_number(to_char(%s, 'yyyymmddHH24MISS'))",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForNow(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForNow(Function function) {
    return "SYSTIMESTAMP AT TIME ZONE 'UTC'";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForDaysBetween(org.alfasoftware.morf.sql.element.AliasedField, org.alfasoftware.morf.sql.element.AliasedField)
   */
  @Override
  protected String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate) {
    return String.format("%s - %s", getSqlFrom(toDate), getSqlFrom(fromDate));
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
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSubstringFunctionName()
   */
  @Override
  protected String getSubstringFunctionName() {
    return "SUBSTR";
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
      "%s + %s",
      getSqlFrom(function.getArguments().get(0)),
      getSqlFrom(function.getArguments().get(1))
    );
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForAddMonths(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForAddMonths(Function function) {
    return "ADD_MONTHS(" +
      getSqlFrom(function.getArguments().get(0)) + ", " +
      getSqlFrom(function.getArguments().get(1)) + ")";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameTableStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> renameTableStatements(Table fromTable, Table toTable) {
    String from = truncatedTableName(fromTable.getName());
    String fromConstraint = primaryKeyConstraintName(fromTable.getName());

    String to = truncatedTableName(toTable.getName());
    String toConstraint = primaryKeyConstraintName(toTable.getName());

    ArrayList<String> statements = new ArrayList<>();

    if (!primaryKeysForTable(fromTable).isEmpty()) {
      // Rename the PK constraint
      statements.add("ALTER TABLE " + schemaNamePrefix() + from + " RENAME CONSTRAINT " + fromConstraint + " TO " + toConstraint);
      // Rename the index for the PK constraint the Oracle uses to manage the PK
      statements.add("ALTER INDEX " + schemaNamePrefix() + fromConstraint + " RENAME TO " + toConstraint);
    }

    // Rename the table itself
    statements.add("ALTER TABLE " + schemaNamePrefix() + from + " RENAME TO " + to);
    statements.add(commentOnTable(to));

    return statements;
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
    StringBuilder sqlBuilder = new StringBuilder("MERGE INTO ");

    // Now add the into clause
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);

    // Add USING
    sqlBuilder.append(" USING (");
    sqlBuilder.append(getSqlFrom(statement.getSelectStatement()));
    sqlBuilder.append(") ");
    sqlBuilder.append(MERGE_SOURCE_ALIAS);

    // Add the matching keys
    sqlBuilder.append(" ON (");
    sqlBuilder.append(matchConditionSqlForMergeFields(statement.getTableUniqueKey(), MERGE_SOURCE_ALIAS, destinationTableName));

    // What to do if matched
    if (getNonKeyFieldsFromMergeStatement(statement).iterator().hasNext()) {
      sqlBuilder.append(") WHEN MATCHED THEN UPDATE SET ");
      sqlBuilder.append(assignmentSqlForMergeFields(getNonKeyFieldsFromMergeStatement(statement), MERGE_SOURCE_ALIAS, destinationTableName));
    } else {
      sqlBuilder.append(")");
    }

    // What to do if no match
    sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (");
    Iterable<String> insertField = Iterables.transform(statement.getSelectStatement().getFields(), new com.google.common.base.Function<AliasedField, String>() {
        @Override
        public String apply(AliasedField field) {
          return field.getImpliedName();
        }
      });
    sqlBuilder.append(Joiner.on(", ").join(insertField));

    // Values to insert
    sqlBuilder.append(") VALUES (");
    Iterable<String> valueFields = Iterables.transform(statement.getSelectStatement().getFields(), new com.google.common.base.Function<AliasedField, String>() {
      @Override
      public String apply(AliasedField field) {
        return MERGE_SOURCE_ALIAS + "." + field.getImpliedName();
      }
    });
    sqlBuilder.append(Joiner.on(", ").join(valueFields));

    sqlBuilder.append(")");

    return sqlBuilder.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#renameIndexStatements(org.alfasoftware.morf.metadata.Table, java.lang.String, java.lang.String)
   */
  @Override
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.of(String.format("ALTER INDEX %s%s RENAME TO %s", schemaNamePrefix(), fromIndexName, toIndexName));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#addTableFromStatements(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.sql.SelectStatement)
   */
  @Override
  public Collection<String> addTableFromStatements(Table table, SelectStatement selectStatement) {
    Builder<String> result = ImmutableList.<String>builder();
    result.add(new StringBuilder()
        .append(createTableStatement(table, true))
        .append(" AS ")
        .append(convertStatementToSQL(selectStatement))
        .toString()
      );
    result.add("ALTER TABLE " + qualifiedTableName(table) + " NOPARALLEL LOGGING");

    if (!primaryKeysForTable(table).isEmpty()) {
      result.add("ALTER INDEX " + schemaNamePrefix() + primaryKeyConstraintName(table.getName()) + " NOPARALLEL LOGGING");
    }

    result.addAll(buildRemainingStatementsAndComments(table));

    return result.build();
  }


  /**
   * Builds the remaining statements (triggers, sequences and comments).
   *
   * @param table The table to create the statements.
   * @return the collection of statements.
   */
  private Collection<String> buildRemainingStatementsAndComments(Table table) {

    List<String> statements = Lists.newArrayList();

    Column sequence = findAutonumberedColumn(table);
    if (sequence != null) {
      statements.add(dropTrigger(table));
      statements.add(dropSequence(table));
      statements.add(createNewSequence(table, sequence));
      statements.addAll(createTrigger(table, sequence));
    }

    String truncatedTableName = truncatedTableName(table.getName());
    statements.add(commentOnTable(truncatedTableName));

    statements.addAll(createColumnComments(table));

    return statements;
  }


  /**
   * SqlPlus requires sql statement lines to be less than 2500 characters in length.
   * Additionally PL\SQL statements must be ended with "/".
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#formatSqlStatement(java.lang.String)
   */
  @Override
  public String formatSqlStatement(String sqlStatement) {
    // format statement ending
    StringBuilder builder = new StringBuilder(sqlStatement);
    if (sqlStatement.endsWith("END;")) {
      builder.append(System.getProperty("line.separator"));
      builder.append("/");
    } else {
      builder.append(";");
    }

    return splitSqlStatement(builder.toString());
  }


  /**
   * If the SQL statement line is greater than 2499 characters then split
   * it into multiple lines where each line is less than 2500 characters in
   * length. The split is done on a space character; if a space character
   * cannot be found then a warning will be logged but the statement line
   * will still be returned exceeding 2499 characters in length.
   *
   * @param sqlStatement the statement to split
   * @return the correctly formatted statement
   */
  private String splitSqlStatement(String sqlStatement) {
    StringBuilder sql = new StringBuilder();
    if (sqlStatement.length() >= 2500) {
      int splitAt = sqlStatement.lastIndexOf(' ', 2498);
      if (splitAt == -1) {
        log.warn("SQL statement greater than 2499 characters in length but unable to find white space (\" \") to split on.");
        sql.append(sqlStatement);
      } else {
        sql.append(sqlStatement.substring(0, splitAt));
        sql.append(System.getProperty("line.separator"));
        sql.append(splitSqlStatement(sqlStatement.substring(splitAt + 1)));
      }
    } else {
      sql.append(sqlStatement);
    }

    return sql.toString();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandom()
   */
  @Override
  protected String getSqlForRandom() {
    return "dbms_random.value";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlForRandomString(org.alfasoftware.morf.sql.element.Function)
   */
  @Override
  protected String getSqlForRandomString(Function function) {
    return String.format("dbms_random.string('A', %s)",getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getFromDummyTable()
   */
  @Override
  protected String getFromDummyTable() {
    return " FROM dual";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#getSqlFrom(org.alfasoftware.morf.sql.SelectFirstStatement)
   */
  @Override
  protected String getSqlFrom(SelectFirstStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT MIN(");

    // Start by adding the field
    result.append(getSqlFrom(stmt.getFields().get(0))).append(") KEEP (DENSE_RANK FIRST");

    appendOrderBy(result, stmt);
    result.append(")");

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);

    return result.toString().trim();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#selectStatementPreFieldDirectives(org.alfasoftware.morf.sql.SelectStatement)
   */
  @Override
  protected String selectStatementPreFieldDirectives(SelectStatement selectStatement) {
    if (selectStatement.getHints().isEmpty()) {
      return super.selectStatementPreFieldDirectives(selectStatement);
    }

    StringBuilder builder = new StringBuilder().append("/*+");

    for (Hint hint : selectStatement.getHints()) {
      if (hint instanceof OptimiseForRowCount) {
        builder.append(" FIRST_ROWS(")
          .append(((OptimiseForRowCount)hint).getRowCount())
          .append(")");
      }
      if (hint instanceof UseIndex) {
        UseIndex useIndex = (UseIndex)hint;
        builder.append(" INDEX(")
          // No schema name - see http://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements006.htm#BABIEJEB
          .append(StringUtils.isEmpty(useIndex.getTable().getAlias()) ? useIndex.getTable().getName() : useIndex.getTable().getAlias())
          .append(" ")
          .append(useIndex.getIndexName())
          .append(")");
      }
      if (hint instanceof UseImplicitJoinOrder) {
        builder.append(" ORDERED");
      }
    }

    return builder.append(" */ ").toString();
  };


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#rebuildTriggers(org.alfasoftware.morf.metadata.Table)
   */
  @Override
  public Collection<String> rebuildTriggers(Table table) {
    return rebuildSequenceAndTrigger(table,getAutoIncrementColumnForTable(table));
  }


  /**
   * Fetch rows in blocks of 200, rather than the default of 1.
   */
  @Override
  public int fetchSizeForBulkSelects() {
    return 200;
  }


  /**
   * We do use NVARCHAR for strings on Oracle.
   *
   * @see org.alfasoftware.morf.jdbc.SqlDialect#usesNVARCHARforStrings()
   */
  @Override
  public boolean usesNVARCHARforStrings() {
    return true;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.SqlDialect#supportsWindowFunctions()
   */
  @Override
  public boolean supportsWindowFunctions() {
    return true;
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
    return ImmutableList.of(
                     "BEGIN \n" +
                       "DBMS_STATS.GATHER_TABLE_STATS(ownname=> '" + getSchemaName() + "', "
                          + "tabname=>'" + table.getName() + "', "
                          + "cascade=>true, degree=>DBMS_STATS.AUTO_DEGREE, no_invalidate=>false); \n"
                   + "END;");
  }
}