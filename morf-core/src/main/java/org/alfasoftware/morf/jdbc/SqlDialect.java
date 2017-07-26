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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy.ALL;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.dataset.RecordHelper;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.AbstractSelectStatement;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SetOperator;
import org.alfasoftware.morf.sql.SqlElementCallback;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UnionSetOperator;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.BracketedExpression;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldFromSelectFirst;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.alfasoftware.morf.upgrade.ChangeColumn;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.LocalDate;
import org.joda.time.Months;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Provides functionality for generating SQL statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public abstract class SqlDialect implements DatabaseSafeStringToRecordValueConverter {

  /**
   *
   */
  protected static final String          ID_INCREMENTOR_TABLE_COLUMN_VALUE = "value";

  /**
   *
   */
  protected static final String          ID_INCREMENTOR_TABLE_COLUMN_NAME  = "name";

  /**
   * The width of the id field
   */
  public static final int                ID_COLUMN_WIDTH                   = 19;

  /**
   * Empty collection of strings that implementations can return if required.
   */
  public static final Collection<String> NO_STATEMENTS                     = Collections.emptyList();

  /**
   * Database schema name.
   */
  private final String                   schemaName;

  /**
   * Returns the database schema name. May be null.
   * @return The schema name
   */
  public String getSchemaName() {
    return schemaName;
  }


  /**
   * @param schemaName The schema to use for statements.
   */
  public SqlDialect(String schemaName) {
    super();
    this.schemaName = schemaName;
  }


  /**
   * Creates SQL to deploy a database table and its associated indexes.
   *
   * @param table The meta data for the table to deploy.
   * @return The statements required to deploy the table and its indexes.
   */
  public Collection<String> tableDeploymentStatements(Table table) {
    Builder<String> statements = ImmutableList.<String>builder();

    statements.addAll(internalTableDeploymentStatements(table));

    for (Index index : table.indexes()) {
      statements.add(indexDeploymentStatement(table, index));
    }

    return statements.build();
  }


  /**
   * Creates the SQL to deploy a database table.
   *
   * @param table The meta data for the table to deploy.
   * @return The statements required to deploy the table.
   */
  protected abstract Collection<String> internalTableDeploymentStatements(Table table);


  /**
   * Creates SQL to deploy a database view.
   *
   * @param view The meta data for the view to deploy.
   * @return The statements required to deploy the view.
   */
  public Collection<String> viewDeploymentStatements(View view) {
    List<String> statements = new ArrayList<>();

    // Create the table deployment statement
    StringBuilder createTableStatement = new StringBuilder();
    createTableStatement.append("CREATE ");
    createTableStatement.append("VIEW ");
    createTableStatement.append(schemaNamePrefix());
    createTableStatement.append(view.getName());
    createTableStatement.append(" AS (");
    createTableStatement.append(convertStatementToSQL(view.getSelectStatement()));
    createTableStatement.append(")");

    statements.add(createTableStatement.toString());

    return statements;
  }


  /**
   * Creates SQL to truncate a table (may require DBA rights on some databases
   * e.g. Oracle).
   *
   * @param table The database table.
   * @return SQL statements required to clear a table and prepare it for data
   *         population.
   */
  public abstract Collection<String> truncateTableStatements(Table table);


  /**
   * Creates SQL to rename a table.
   *
   * @param from - table to rename
   * @param to - table with new name
   * @return SQL statements required to change a table name.
   */
  public abstract Collection<String> renameTableStatements(Table from, Table to);


  /**
   * Creates SQL to rename an index.
   *
   * @param table table on which the index exists
   * @param fromIndexName The index to rename
   * @param toIndexName The new index name
   * @return SQL Statements required to rename an index
   */
  public abstract Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName);


  /**
   * @param table - table to perform this action on
   * @param oldPrimaryKeyColumns - the existing primary key columns
   * @param newPrimaryKeyColumns - the new primary key columns
   * @return SQL Statements required to change the primary key columns
   */
  public abstract Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns,
      List<String> newPrimaryKeyColumns);


  /**
   * Creates SQL to delete all records from a table (doesn't use truncate).
   *
   * @param table the database table to clear
   * @return SQL statements required to clear the table.
   */
  public abstract Collection<String> deleteAllFromTableStatements(Table table);


  /**
   * Creates SQL to execute prior to bulk-inserting to a table.
   *
   * @param table {@link Table} to be inserted to.
   * @param insertingUnderAutonumLimit  Determines whether we are inserting under an auto-numbering limit.
   * @return SQL statements to be executed prior to insert.
   */
  @SuppressWarnings("unused")
  public Collection<String> preInsertWithPresetAutonumStatements(Table table, boolean insertingUnderAutonumLimit){
    return Collections.emptyList();
  }


  /**
   * Creates SQL to execute after bulk-inserting to a table.
   *
   * @param table The table that was populated.
   * @param executor The executor to use
   * @param connection The connection to use
   * @param insertingUnderAutonumLimit  Determines whether we are inserting under an auto-numbering limit.
   */
  @SuppressWarnings("unused")
  public void postInsertWithPresetAutonumStatements(Table table,SqlScriptExecutor executor,Connection connection, boolean insertingUnderAutonumLimit) {
  }


  /**
   * Make sure the table provided has its next autonum value set to at least the value specified in the column metadata.
   *
   * <p>Generally databases do not need to do anything special here, but MySQL can lose the value.</p>
   *
   * @param table The table to repair.
   * @param executor The executor to use
   * @param connection The connection to use
   */
  @SuppressWarnings("unused")
  public void repairAutoNumberStartPosition(Table table,SqlScriptExecutor executor,Connection connection) {
  }


  /**
   * Returns an SQL statement which can be used to test that a connection
   * remains alive and usable.
   * <p>
   * The connection has to be fast to execute and side-effect free.
   * </p>
   *
   * @return A connection test statement, or null if none is available for this
   *         dialect.
   */
  public abstract String connectionTestStatement();


  /**
   * Converts a {@link Statement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @param databaseMetadata the database schema. If null, no defaulting is
   *          performed.
   * @param idTable the id table. If null, no automatic setting of id numbers is
   *          performed.
   * @return a string containing the SQL to run against the database
   */
  public List<String> convertStatementToSQL(Statement statement, Schema databaseMetadata, Table idTable) {
    if (statement instanceof InsertStatement) {
      InsertStatement insert = (InsertStatement) statement;
      if (databaseMetadata == null || isAutonumbered(insert, databaseMetadata)) {
        return convertStatementToSQL(insert);
      } else {
        return convertStatementToSQL(insert, databaseMetadata, idTable);
      }
    } else if (statement instanceof UpdateStatement) {
      UpdateStatement update = (UpdateStatement) statement;
      return Collections.singletonList(convertStatementToSQL(update));
    } else if (statement instanceof DeleteStatement) {
      DeleteStatement delete = (DeleteStatement) statement;
      return Collections.singletonList(convertStatementToSQL(delete));
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement truncateStatement = (TruncateStatement) statement;
      return Collections.singletonList(convertStatementToSQL(truncateStatement));
    } else if (statement instanceof MergeStatement) {
      MergeStatement merge = (MergeStatement) statement;
      return Collections.singletonList(convertStatementToSQL(merge));
    } else {
      throw new UnsupportedOperationException("Executed statement operation not supported for [" + statement.getClass() + "]");
    }
  }


  /**
   * Checks whether the table the InsertStatement is referring to has any
   * autonumbered columns
   *
   * @param statement The statement to check
   * @return true if autonumbered, false otherwise
   */
  private boolean isAutonumbered(InsertStatement statement, Schema databaseMetadata) {
    if (statement.getTable() != null) {
      Table tableInserting = databaseMetadata.getTable(statement.getTable().getName());
      for (Column col : tableInserting.columns()) {
        if (col.isAutoNumbered()) {
          return true;
        }
      }
    }
    return false;
  }


  /**
   * Converts a simple parameterised insert statement into a single SQL string.
   *
   * @param statement the statement to convert
   * @param databaseMetadata the database schema.
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(InsertStatement statement, Schema databaseMetadata) {
    if (!statement.isParameterisedInsert()) {
      throw new IllegalArgumentException("Non-parameterised insert statements must supply the id table.");
    }

    return buildParameterisedInsert(statement, databaseMetadata);
  }


  /**
   * Converts a structured {@link InsertStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public List<String> convertStatementToSQL(InsertStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    expandInnerSelectFields(statement.getSelectStatement());

    // If this is a specific values insert then use a standard converter
    if (statement.isSpecificValuesInsert()) {
      return buildSpecificValueInsert(statement, null, null);
    }

    return getSqlFromInsert(statement, null, null);
  }


  /**
   * Converts a structured {@link InsertStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @param databaseMetadata the database schema. If null, no defaulting is
   *          performed.
   * @param idTable the id table. If null, no automatic setting of id numbers is
   *          performed.
   * @return a string containing the SQL to run against the database
   */
  public List<String> convertStatementToSQL(InsertStatement statement, Schema databaseMetadata, Table idTable) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    expandInnerSelectFields(statement.getSelectStatement());
    InsertStatement defaultedStatement = new InsertStatementDefaulter(databaseMetadata).defaultMissingFields(statement);

    // If this is a parameterised insert then use a standard converter
    if (statement.isParameterisedInsert()) {
      return ImmutableList.of(buildParameterisedInsert(defaultedStatement, databaseMetadata));
    }

    // If this is a specific values insert then use a standard converter
    if (statement.isSpecificValuesInsert()) {
      return buildSpecificValueInsert(defaultedStatement, databaseMetadata, idTable);
    }

    return getSqlFromInsert(expandInsertStatement(defaultedStatement, databaseMetadata), databaseMetadata, idTable);
  }


  /**
   * Creates the fields from any inner selects on the outer select.
   *
   * @param statement the select statement to expand.
   */
  private void expandInnerSelectFields(SelectStatement statement) {
    if (statement == null || !statement.getFields().isEmpty() || statement.getFromSelects().isEmpty()) {
      return;
    }

    for (SelectStatement selectStatement : statement.getFromSelects()) {
      expandInnerSelectFields(selectStatement);

      for (AliasedField field : selectStatement.getFields()) {
        statement.appendFields(new FieldReference(new TableReference(selectStatement.getAlias()), field.getAlias()));
      }
    }
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(SelectStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(SelectFirstStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(MergeStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(UpdateStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(DeleteStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(TruncateStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * Extracts the parameters from a SQL statement.
   *
   * @param statement the SQL statement.
   * @return the list of parameters.
   */
  public List<SqlParameter> extractParameters(InsertStatement statement) {
    SqlParameterExtractor extractor = new SqlParameterExtractor();
    ObjectTreeTraverser.forCallback(extractor).dispatch(statement);
    return extractor.list;
  }


  /**
   * SQL visitor which extracts parameters to a list.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  private static class SqlParameterExtractor extends SqlElementCallback {
    final List<SqlParameter> list = Lists.newArrayList();

    @Override
    public void visit(AliasedField field) {
      if (field instanceof SqlParameter) {
        list.add((SqlParameter)field);
      }
    }
  }


  /**
   * Converts a structured {@link UpdateStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(UpdateStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    return getSqlFrom(statement);
  }


  /**
   * Converts a structured {@link MergeStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(MergeStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    return getSqlFrom(statement);
  }


  /**
   * Converts a structured {@link DeleteStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(DeleteStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    return getSqlFrom(statement);
  }


  /**
   * Converts a {@link TruncateStatement} to SQL.
   *
   * @param statement The statement to convert
   * @return The SQL represented by the statement
   */
  public String convertStatementToSQL(TruncateStatement statement) {
    return truncateTableStatements(table(statement.getTable().getName())).iterator().next();
  }


  /**
   * Whether insert statement batching should be used for this dialect to
   * improve performance.
   *
   * @return <var>true</var> if code should use statement batches to reduce
   *         overhead when bulk inserting data.
   */
  public boolean useInsertBatching() {
    return true;
  }


  /**
   * Different JDBC drivers and platforms have different behaviour for paging results
   * into a {@link ResultSet} as they are fetched.  For example, MySQL defaults
   * to <a href="http://stackoverflow.com/questions/20496616/fetchsize-in-resultset-set-to-0-by-default">fetching
   * <em>all</em> records</a> into memory, whereas Oracle defaults to fetching
   * <a href="https://docs.oracle.com/cd/A87860_01/doc/java.817/a83724/resltse5.htm">10
   * records</a> at a time.
   *
   * <p>The impact mostly rears its head during bulk loads (when loading large numbers
   * of records).  MySQL starts to run out of memory, and Oracle does not run at
   * optimal speed due to unnecessary round-trips.</p>
   *
   * <p>This provides the ability for us to specify different fetch sizes for bulk loads
   * on different platforms.  Refer to the individual implementations for reasons for
   * the choices there.</p>
   *
   * @return The number of rows to try and fetch at a time (default) when
   *         performing bulk select operations.
   * @see #fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()
   */
  public int fetchSizeForBulkSelects() {
    return 1;
  }


  /**
   * When using a "streaming" {@link ResultSet} (i.e. any where the fetch size indicates that fewer
   * than all the records should be returned at a time), MySQL does not permit the connection
   * to be used for anything else.  Therefore we have an alternative fetch size here specifically
   * for the scenario where this is unavoidable.
   *
   * <p>In practice this returns the same value except for on MySQL, where we use it to
   * effectively disable streaming if we know the connection will be used.  This means
   * certain types of processing are liable to cause high memory usage on MySQL.</p>
   *
   * @return The number of rows to try and fetch at a time (default) when
   *         performing bulk select operations and needing to use the connection while
   *         the {@link ResultSet} is open.
   * @see #fetchSizeForBulkSelects()
   */
  public int fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming() {
    return fetchSizeForBulkSelects();
  }


  /**
   * @return The schema prefix (including the dot) or blank if the schema's
   *         blank.
   */
  public String schemaNamePrefix() {
    if (StringUtils.isEmpty(schemaName)) {
      return "";
    }

    return schemaName.toUpperCase() + ".";
  }


  /**
   * @param tableRef The table reference from which the schema name will be extracted
   * @return The schema prefix of the specified table (including the dot), the
   *         dialect's schema prefix or blank if neither is specified (in that
   *         order).
   */
  protected String schemaNamePrefix(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getSchemaName())) {
      return schemaNamePrefix();
    } else {
      return tableRef.getSchemaName().toUpperCase() + ".";
    }
  }


  /**
   * Creates SQL to drop the named table.
   *
   * @param table The table to drop
   * @return The SQL statements as strings.
   */
  public abstract Collection<String> dropStatements(Table table);


  /**
   * Creates SQL to drop the named view.
   *
   * @param view The view to drop
   * @return The SQL statements as strings.
   */
  public abstract Collection<String> dropStatements(View view);


  /**
   * Convert a {@link SelectStatement} into standards compliant SQL.
   * <p>
   * For example, the following code:
   * </p>
   * <blockquote>
   *
   * <pre>
   * SelectStatement stmt = new SelectStatement().from(new Table(&quot;agreement&quot;));
   *                                                                            String result = sqlgen.getSqlFrom(stmt);
   * </pre>
   *
   * </blockquote>
   * <p>
   * Will populate {@code result} with:
   * </p>
   * <blockquote>
   *
   * <pre>
   *    SELECT * FROM agreement
   * </pre>
   *
   * </blockquote>
   *
   * @param stmt the select statement to generate SQL for
   * @return a standards compliant SQL SELECT statement
   */
  protected String getSqlFrom(final SelectStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT ");

    // Any hint directives which should be inserted before the field list
    result.append(selectStatementPreFieldDirectives(stmt));

    // Start by checking if this is a distinct call, then add the field list
    if (stmt.isDistinct()) {
      result.append("DISTINCT ");
    }
    if (stmt.getFields().isEmpty()) {
      result.append("*");
    } else {
      boolean firstField = true;
      for (AliasedField currentField : stmt.getFields()) {
        if (!firstField) {
          result.append(", ");
        }

        result.append(getSqlFrom(currentField));

        // Put an alias in, if requested
        appendAlias(result, currentField);

        firstField = false;
      }
    }

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);
    appendGroupBy(result, stmt);
    appendHaving(result, stmt);
    appendUnionSet(result, stmt);
    appendOrderBy(result, stmt);

    if (stmt.isForUpdate()) {
      if (stmt.isDistinct() || !stmt.getGroupBys().isEmpty() || !stmt.getJoins().isEmpty()) {
        throw new IllegalArgumentException("GROUP BY, JOIN or DISTINCT cannot be combined with FOR UPDATE (H2 limitations)");
      }
      result.append(getForUpdateSql());
    }

    // Any hint directives which should be inserted right at the end of the statement
    result.append(selectStatementPostStatementDirectives(stmt));

    return result.toString();
  }


  /**
   * Returns any SQL code which should be added between a <code>SELECT</code> and the field
   * list for dialect-specific reasons.
   *
   * @param selectStatement The select statement
   * @return Any hint code required.
   */
  protected String selectStatementPreFieldDirectives(@SuppressWarnings("unused") SelectStatement selectStatement) {
    return StringUtils.EMPTY;
  }


  /**
   * Returns any SQL code which should be added at the end of a statement for dialect-specific reasons.
   *
   * @param selectStatement The select statement
   * @return Any hint code required.
   */
  protected String selectStatementPostStatementDirectives(@SuppressWarnings("unused") SelectStatement selectStatement) {
    return StringUtils.EMPTY;
  }


  /**
   * Default behaviour for FOR UPDATE. Can be overridden.
   * @return The String representation of the FOR UPDATE clause.
   */
  protected String getForUpdateSql() {
    return " FOR UPDATE";
  }


  /**
   * appends alias to the result
   *
   * @param result alias will be appended to this
   * @param currentField field to be aliased
   */
  private void appendAlias(StringBuilder result, AliasedField currentField) {
    if (!StringUtils.isBlank(currentField.getAlias())) {
      result.append(String.format(" AS %s", currentField.getAlias()));
    }
  }


  /**
   * Convert a {@link SelectFirstStatement} into SQL. This is the same format
   * used for H2, MySQL. Oracle and SqlServer implementation override this
   * function.
   *
   * @param stmt the select statement to generate SQL for
   * @return SQL string specific for database platform
   */
  protected String getSqlFrom(final SelectFirstStatement stmt) {
    StringBuilder result = new StringBuilder("SELECT ");
    // Start by adding the field
    result.append(getSqlFrom(stmt.getFields().get(0)));

    appendFrom(result, stmt);
    appendJoins(result, stmt, innerJoinKeyword(stmt));
    appendWhere(result, stmt);
    appendOrderBy(result, stmt);

    result.append(" LIMIT 0,1");

    return result.toString().trim();
  }


  /**
   * appends joins clauses to the result
   *
   * @param result joins will be appended here
   * @param stmt statement with joins clauses
   * @param innerJoinKeyword The keyword for INNER JOIN
   * @param <T> The type of {@link AbstractSelectStatement}
   */
  protected <T extends AbstractSelectStatement<T>> void appendJoins(StringBuilder result, AbstractSelectStatement<T> stmt, String innerJoinKeyword) {
    for (Join currentJoin : stmt.getJoins()) {
      appendJoin(result, currentJoin, innerJoinKeyword);
    }
  }


  /**
   * appends order by clause to the result
   *
   * @param result order by clause will be appended here
   * @param stmt statement with order by clause
   * @param <T> The type of AbstractSelectStatement
   */
  protected <T extends AbstractSelectStatement<T>> void appendOrderBy(StringBuilder result, AbstractSelectStatement<T> stmt) {
    if (!stmt.getOrderBys().isEmpty()) {
      result.append(" ORDER BY ");

      boolean firstOrderByField = true;
      for (AliasedField currentOrderByField : stmt.getOrderBys()) {
        if (!firstOrderByField) {
          result.append(", ");
        }

        result.append(getSqlForOrderByField(currentOrderByField));

        firstOrderByField = false;
      }
    }
  }


  /**
   * appends union set operators to the result
   *
   * @throws UnsupportedOperationException if any other than
   *           {@link UnionSetOperator} set operation found
   * @param result union set operators will be appended here
   * @param stmt statement with set operators
   */
  protected void appendUnionSet(StringBuilder result, SelectStatement stmt) {
    if (stmt.getSetOperators() != null) {
      for (SetOperator operator : stmt.getSetOperators()) {
        if (operator instanceof UnionSetOperator) {
          result.append(getSqlFrom((UnionSetOperator) operator));
        } else {
          throw new UnsupportedOperationException("Unsupported set operation");
        }
      }
    }
  }


  /**
   * appends having clause to the result
   *
   * @param result having clause will be appended here
   * @param stmt statement with having clause
   */
  protected void appendHaving(StringBuilder result, SelectStatement stmt) {
    if (stmt.getHaving() != null) {
      result.append(" HAVING ");
      result.append(getSqlFrom(stmt.getHaving()));
    }
  }


  /**
   * appends group by clause to the result
   *
   * @param result group by clause will be appended here
   * @param stmt statement with group by clause
   */
  protected void appendGroupBy(StringBuilder result, SelectStatement stmt) {
    if (stmt.getGroupBys().size() > 0) {
      result.append(" GROUP BY ");

      boolean firstGroupByField = true;
      for (AliasedField currentGroupByField : stmt.getGroupBys()) {
        if (!firstGroupByField) {
          result.append(", ");
        }

        result.append(getSqlFrom(currentGroupByField));

        firstGroupByField = false;
      }
    }
  }


  /**
   * appends where clause to the result
   *
   * @param result where clause will be appended here
   * @param stmt statement with where clause
   * @param <T> The type of AbstractSelectStatement
   */
  protected <T extends AbstractSelectStatement<T>> void appendWhere(StringBuilder result, AbstractSelectStatement<T> stmt) {
    if (stmt.getWhereCriterion() != null) {
      result.append(" WHERE ");
      result.append(getSqlFrom(stmt.getWhereCriterion()));
    }
  }


  /**
   * appends from clause to the result
   *
   * @param result from clause will be appended here
   * @param stmt statement with from clause
   * @param <T> The type of AbstractSelectStatement
   */
  protected <T extends AbstractSelectStatement<T>> void appendFrom(StringBuilder result, AbstractSelectStatement<T> stmt) {
    if (stmt.getTable() != null) {
      result.append(" FROM ");
      result.append(schemaNamePrefix(stmt.getTable()));
      result.append(stmt.getTable().getName());

      // Add a table alias if necessary
      if (!stmt.getTable().getAlias().equals("")) {
        result.append(" ");
        result.append(stmt.getTable().getAlias());
      }
    } else if (!stmt.getFromSelects().isEmpty()) {
      result.append(" FROM ");

      boolean first = true;
      for (SelectStatement innerSelect : stmt.getFromSelects()) {

        checkSelectStatementHasNoHints(innerSelect, "Hints not currently permitted on subqueries");

        if (!first) {
          result.append(", ");
        }
        first = false;

        result.append(String.format("(%s)", getSqlFrom(innerSelect)));
        if (StringUtils.isNotBlank(innerSelect.getAlias())) {
          result.append(String.format(" %s", innerSelect.getAlias()));
        }
      }
    } else {
      result.append(getFromDummyTable());
    }
  }


  /**
   * gets SQL for field inside order by clause. it covers null values handling and direction.
   * <p>
   * If the field if {@link FieldReference} then overloaded method is called,
   * otherwise default SQL (with default null values handling type and direction) is applied
   * </p>
   *
   * @param currentOrderByField field within order by clause
   * @return SQL for the field inside order by clause
   */
  private String getSqlForOrderByField(AliasedField currentOrderByField) {

    if (currentOrderByField instanceof FieldReference) {
      return getSqlForOrderByField((FieldReference) currentOrderByField);
    }

    StringBuilder result = new StringBuilder(getSqlFrom(currentOrderByField));
    result.append(" ").append(defaultNullOrder());

    return result.toString().trim();
  }


  /**
   * When executing order by a database platform may apply its defaults for: -
   * null values handling (to either put them first or last) - direction
   * (ascending or descending)
   * <p>
   * In order for explicit settings. e.g: order by field asc nulls last.
   * </p>
   * <p>
   * This method is an implementation for H2 and Oracle. For other dialects it required being overridden.
   * </p>
   *
   * @param orderByField field reference ordered by
   * @return sql for order by field
   */
  protected String getSqlForOrderByField(FieldReference orderByField) {

    StringBuilder result = new StringBuilder(getSqlFrom(orderByField));

    switch (orderByField.getDirection()) {
      case DESCENDING:
        result.append(" DESC");
        break;
      case ASCENDING:
      case NONE:
      default:
        break;
    }

    result.append(getSqlForOrderByFieldNullValueHandling(orderByField));

    return result.toString().trim();
  }


  /**
   * Get the SQL expression for NULL values handling.
   * @param orderByField The order by clause
   * @return The resulting SQL String
   *
   */
  protected String getSqlForOrderByFieldNullValueHandling(FieldReference orderByField) {

    if (orderByField.getNullValueHandling().isPresent()) {
      switch (orderByField.getNullValueHandling().get()) {
        case FIRST:
          return " NULLS FIRST";
        case LAST:
          return " NULLS LAST";
        case NONE:
        default:
          return "";
      }
    } else {
      return " " + defaultNullOrder();
    }
  }


  /**
   * An additional clause to use in SELECT statements where there is no select
   * source, which allows us to include "FROM &lt;dummy table&gt;" on RDBMSes such as
   * Oracle where selecting from no table is not allowed but the RDBMS provides
   * a dummy table (such as "dual").
   *
   * @return the additional clause.
   */
  protected String getFromDummyTable() {
    return StringUtils.EMPTY;
  }


  /**
   * @param result the string builder to append to
   * @param join the join statement
   */
  private void appendJoin(StringBuilder result, Join join, String innerJoinKeyword) {
    // Put the type in
    switch (join.getType()) {
      case INNER_JOIN:
        result.append(" ").append(innerJoinKeyword).append(" ");
        break;
      case LEFT_OUTER_JOIN:
        result.append(" LEFT OUTER JOIN ");
        break;
      case RIGHT_OUTER_JOIN:
        result.append(" RIGHT OUTER JOIN ");
        break;
      default:
        throw new UnsupportedOperationException("Cannot perform join of type [" + join.getType() + "] on database");
    }

    if (join.getTable() == null && (join.getSubSelect() == null || join.getSubSelect().getAlias() == null)) {
      throw new IllegalArgumentException("Join clause does not specify table or sub-select with an alias");
    }

    if (join.getTable() == null) {
      result.append('(');
      result.append(getSqlFrom(join.getSubSelect()));
      result.append(") ");
      result.append(join.getSubSelect().getAlias());
    } else {
      // Now add the table name
      result.append(String.format("%s%s", schemaNamePrefix(join.getTable()), join.getTable().getName()));

      // And add an alias if necessary
      if (!join.getTable().getAlias().isEmpty()) {
        result.append(" ").append(join.getTable().getAlias());
      }
    }

    if (join.getCriterion() != null) {
      result.append(" ON ");

      // Then put the join fields into the output
      result.append(getSqlFrom(join.getCriterion()));

    } else if (join.getType() == JoinType.LEFT_OUTER_JOIN || join.getType() == JoinType.RIGHT_OUTER_JOIN) {
      throw new IllegalArgumentException(join.getType() + " must have ON criteria");

    } else {
      // MySql supports no ON criteria and ON TRUE, but the other platforms
      // don't, so just keep things simple.
      result.append(String.format(" ON 1=1"));
    }
  }


  /**
   * @param stmt The statement.
   * @return The keyword to use for an inner join on the specified statement.  This only differs
   *         in response to hints.
   */
  protected String innerJoinKeyword(@SuppressWarnings("unused") AbstractSelectStatement<?> stmt) {
    return "INNER JOIN";
  }


  /**
   * Converts a {@link UnionSetOperator} into SQL.
   *
   * @param operator the union to convert.
   * @return a string representation of the field.
   */
  private String getSqlFrom(UnionSetOperator operator) {
    return String.format(" %s %s", operator.getUnionStrategy() == ALL ? "UNION ALL" : "UNION",
      getSqlFrom(operator.getSelectStatement()));
  }


  /**
   * A database platform may need to specify the null order.
   * <p>
   * If a null order is not required for a SQL dialect descendant classes need
   * to implement this method.
   * </p>
   *
   * @return the null order for an SQL dialect
   */
  protected String defaultNullOrder() {
    return StringUtils.EMPTY;
  }


  /**
   * Convert an {@link InsertStatement} into standards compliant SQL.
   * <p>
   * For example, the following code:
   * </p>
   * <blockquote>
   *
   * <pre>
   * InsertStatement stmt = new InsertStatement().into(new Table(&quot;agreement&quot;)).from(new Table(&quot;agreement&quot;));
   *                                                                                                         String result = dialect
   *                                                                                                                           .getSqlFrom(stmt);
   * </pre>
   *
   * </blockquote>
   * <p>
   * Will populate {@code result} with:
   * </p>
   * <blockquote>
   *
   * <pre>
   *    INSERT INTO agreement (id, version, ...) SELECT id, version, ... FROM agreement
   * </pre>
   *
   * </blockquote>
   *
   * @param stmt the insert statement to generate SQL for
   * @param metadata the database schema.
   * @param idTable the ID Table.
   * @return a standards compliant SQL INSERT statement
   */
  private List<String> getSqlFromInsert(InsertStatement stmt, Schema metadata, Table idTable) {
    if (stmt.getTable() == null) {
      throw new IllegalArgumentException("Cannot specify a null destination table in an insert statement");
    }

    if (stmt.getSelectStatement() == null) {
      throw new IllegalArgumentException("Cannot specify a null for the source select statement in getSqlFrom");
    }

    SelectStatement sourceStatement = stmt.getSelectStatement();

    List<String> result = new LinkedList<>();

    StringBuilder stringBuilder = new StringBuilder();

    stringBuilder.append("INSERT INTO ");
    stringBuilder.append(schemaNamePrefix(stmt.getTable()));
    stringBuilder.append(stmt.getTable().getName());

    stringBuilder.append(" ");

    // Add the destination fields
    if (!stmt.getFields().isEmpty()) {

      // Only check the field count if we're operating with full knowledge of
      // the schema.
      // If we're not, then frankly the code concerned should know what it's
      // doing (e.g.
      // using DB auto-incremement columns or allowing fields to self-default)
      if (metadata != null && stmt.getFields().size() != sourceStatement.getFields().size()) {
        throw new IllegalArgumentException(String.format(
          "Insert statement and source select statement must use the same number of columns. Insert has [%d] but select has [%d].",
          stmt.getFields().size(), sourceStatement.getFields().size()));
      }

      // Use the fields specified by the caller
      stringBuilder.append("(");

      boolean firstRun = true;
      boolean explicitIdColumn = false;
      boolean explicitVersionColumn = false;
      for (AliasedField currentField : stmt.getFields()) {
        if (!(currentField instanceof FieldReference)) {
          throw new IllegalArgumentException("Cannot use a non-field reference in the fields section of an insert statement: ["
              + currentField.getAlias() + "]");
        }

        FieldReference fieldRef = (FieldReference) currentField;
        if (!firstRun) {
          stringBuilder.append(", ");
        }
        stringBuilder.append(fieldRef.getName());

        // Track if we have an id column (i.e. we don't need to default one in)
        explicitIdColumn |= fieldRef.getName().equalsIgnoreCase("id");

        // Track if we have a version column (i.e. we don't need to default one
        // in)
        explicitVersionColumn |= fieldRef.getName().equalsIgnoreCase("version");

        firstRun = false;
      }

      // Only augment the statement if we have the schema to work from
      if (metadata != null && idTable != null) {
        if (!explicitIdColumn && hasColumnNamed(stmt.getTable().getName(), metadata, "id")) {
          result.addAll(buildSimpleAutonumberUpdate(stmt.getTable(), "id", idTable, ID_INCREMENTOR_TABLE_COLUMN_NAME,
            ID_INCREMENTOR_TABLE_COLUMN_VALUE));

          AliasedField idValue = nextIdValue(stmt.getTable(), stmt.getSelectStatement().getTable(), idTable,
            ID_INCREMENTOR_TABLE_COLUMN_NAME, ID_INCREMENTOR_TABLE_COLUMN_VALUE);
          stringBuilder.append(", id");

          // Augment the select statement
          sourceStatement = sourceStatement.deepCopy();
          sourceStatement.appendFields(idValue);
        }

        if (!explicitVersionColumn && hasColumnNamed(stmt.getTable().getName(), metadata, "version")) {
          stringBuilder.append(", version");

          // Augment the select statement
          sourceStatement = sourceStatement.deepCopy();
          sourceStatement.appendFields(new FieldLiteral(0).as("version"));
        }
      }

      stringBuilder.append(") ");
    }

    // Add the select statement
    stringBuilder.append(getSqlFrom(sourceStatement));

    result.add(stringBuilder.toString());

    return result;
  }


  /**
   * Checks the schema to see if the {@code tableName} has a named column as
   * provided.
   *
   * @param tableName the table name.
   * @param metadata the schema.
   * @param columnName the column name to check for.
   * @return true if a column with the name 'id' is found.
   */
  private boolean hasColumnNamed(String tableName, Schema metadata, String columnName) {
    for (Column currentColumn : metadata.getTable(tableName).columns()) {
      if (currentColumn.getName().equalsIgnoreCase(columnName)) {
        return true;
      }
    }

    return false;
  }


  /**
   * Convert a {@link Criterion} to a standards compliant expression.
   *
   * @param criterion the criterion to convert into a standard SQL statement
   * @return the string representation of the criterion
   */
  protected String getSqlFrom(Criterion criterion) {
    if (criterion == null) {
      throw new IllegalArgumentException("Cannot get SQL for a null criterion object");
    }

    // Start building the string
    StringBuilder result = new StringBuilder("(");

    boolean firstInList = true;
    switch (criterion.getOperator()) {
      case AND:
      case OR:
        for (Criterion currentCriterion : criterion.getCriteria()) {
          if (!firstInList) {
            result.append(String.format(" %s ", criterion.getOperator()));
          }
          result.append(getSqlFrom(currentCriterion));
          firstInList = false;
        }
        break;
      case EQ:
        result.append(getOperatorLine(criterion, "="));
        break;
      case NEQ:
        result.append(getOperatorLine(criterion, "<>"));
        break;
      case GT:
        result.append(getOperatorLine(criterion, ">"));
        break;
      case GTE:
        result.append(getOperatorLine(criterion, ">="));
        break;
      case LT:
        result.append(getOperatorLine(criterion, "<"));
        break;
      case LTE:
        result.append(getOperatorLine(criterion, "<="));
        break;
      case LIKE:
        result.append(getOperatorLine(criterion, "LIKE") + likeEscapeSuffix());
        break;
      case ISNULL:
        result.append(String.format("%s IS NULL", getSqlFrom(criterion.getField())));
        break;
      case ISNOTNULL:
        result.append(String.format("%s IS NOT NULL", getSqlFrom(criterion.getField())));
        break;
      case NOT:
        result.append(String.format("NOT %s", getSqlFrom(criterion.getCriteria().get(0))));
        break;
      case EXISTS:
        result.append(String.format("EXISTS (%s)", getSqlFrom(criterion.getSelectStatement())));
        break;
      case IN:
        String content;
        if (criterion.getSelectStatement() == null) {
          content = getSqlForCriterionValueList(criterion);
        } else {
          content = getSqlFrom(criterion.getSelectStatement());
        }
        result.append(String.format("%s IN (%s)", getSqlFrom(criterion.getField()), content));
        break;
      default:
        throw new UnsupportedOperationException("Operator of type [" + criterion.getOperator()
            + "] is not supported in this database");
    }

    result.append(")");

    return result.toString().trim();
  }


  /**
   * @return The string used to set the SQL LIKE escape character - specified after all LIKE expressions
   */
  protected String likeEscapeSuffix() {
    return " ESCAPE '\\'";
  }


  /**
   * Converts a list of values on a criterion into a comma-separated list.
   *
   * @param criterion The criterion to convert
   * @return The converted criterion as a String
   */
  @SuppressWarnings("unchecked")
  protected String getSqlForCriterionValueList(Criterion criterion) {
    if (!(criterion.getValue() instanceof List)) {
      throw new IllegalStateException("Invalid parameter for IN criterion");
    }
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (Object o : (List<Object>) criterion.getValue()) {
      if (!first) {
        builder.append(", ");
      }
      builder.append(getSqlForCriterionValue(o));
      first = false;
    }
    return builder.toString();
  }


  /**
   * Convert a {@link AliasedField} into a standard field reference. If the
   * field has a table reference then this will be used as a prefix to the
   * field.
   *
   * @param field the field to generate a reference for
   * @return a string representation of the field
   */
  protected String getSqlFrom(AliasedField field) {
    if (field instanceof NullFieldLiteral) {
      return "null";
    }

    if (field instanceof SqlParameter) {
      return String.format(":%s", ((SqlParameter)field).getImpliedName());
    }

    if (field instanceof FieldLiteral) {
      return getSqlFrom((FieldLiteral) field);
    }

    if (field instanceof Function) {
      return getSqlFrom((Function) field);
    }

    if (field instanceof FieldReference) {
      FieldReference fieldRef = (FieldReference) field;

      String prefix = "";
      if (fieldRef.getTable() != null) {
        if (StringUtils.isEmpty(fieldRef.getTable().getAlias())) {
          prefix = fieldRef.getTable().getName() + ".";
        } else {
          prefix = fieldRef.getTable().getAlias() + ".";
        }
      }

      return prefix + fieldRef.getName();
    }

    if (field instanceof FieldFromSelect) {
      return "(" + getSqlFrom((FieldFromSelect) field) + ")";
    }

    if (field instanceof FieldFromSelectFirst) {
      return "(" + getSqlFrom((FieldFromSelectFirst) field) + ")";
    }

    if (field instanceof CaseStatement) {
      return getSqlFrom((CaseStatement) field);
    }

    if (field instanceof ConcatenatedField) {
      return getSqlFrom((ConcatenatedField) field);
    }

    if (field instanceof MathsField) {
      return getSqlFrom((MathsField) field);
    }

    if (field instanceof BracketedExpression) {
      return getSqlFrom((BracketedExpression) field);
    }

    if (field instanceof Cast) {
      return getSqlFrom((Cast) field);
    }

    if(field instanceof WindowFunction) {
      return getSqlFrom((WindowFunction) field);
    }

    throw new IllegalArgumentException("Aliased Field of type [" + field.getClass().getSimpleName() + "] is not supported");
  }


  /**
   * Convert {@link CaseStatement} into its SQL representation.
   *
   * @param field representing a case statement
   * @return Returns the SQL representation of {@link CaseStatement}.
   */
  protected String getSqlFrom(CaseStatement field) {
    if (field.getWhenConditions().isEmpty()) {
      throw new IllegalArgumentException("Need to specify when conditions for a case statement");
    }

    if (field.getDefaultValue() == null) {
      throw new IllegalArgumentException("default value needs to be specified");
    }

    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CASE");

    for (WhenCondition when : field.getWhenConditions()) {
      sqlBuilder.append(" WHEN ");
      sqlBuilder.append(getSqlFrom(when.getCriterion()));
      sqlBuilder.append(" THEN ");
      sqlBuilder.append(getSqlFrom(when.getValue()));
    }

    sqlBuilder.append(" ELSE ");
    sqlBuilder.append(getSqlFrom(field.getDefaultValue()));
    sqlBuilder.append(" END");
    return sqlBuilder.toString();
  }


  /**
   * Convert a {@link FieldLiteral} into a standard literal.
   *
   * @param field the field to generate a literal for
   * @return a string representation of the field literal
   */
  protected String getSqlFrom(FieldLiteral field) {
    switch (field.getDataType()) {
      case BOOLEAN:
        return Boolean.valueOf(field.getValue()) ? "1" : "0";
      case STRING:
        return makeStringLiteral(field.getValue());
      case DATE:
        // This is the ISO standard date literal format
        return String.format("DATE '%s'", field.getValue());
      case DECIMAL:
      case BIG_INTEGER:
      case INTEGER:
      case BLOB:
      case CLOB:
        return field.getValue();
      default:
        throw new UnsupportedOperationException("Cannot convert the specified field literal into an SQL literal: ["
            + field.getValue() + "]");
    }
  }


  /**
   * Turn a string value into an SQL string literal which has that value.
   * <p>
   * We use {@linkplain StringUtils#isEmpty(String)} because we want to
   * differentiate between a single space and an empty string.
   * </p>
   * <p>
   * This is necessary because char types cannot be null and must contain a
   * single space.
   * <p>
   *
   * @param literalValue the literal value of the string.
   * @return SQL String Literal
   */
  protected String makeStringLiteral(String literalValue) {
    if (StringUtils.isEmpty(literalValue)) {
      return "NULL";
    }

    return String.format("'%s'", escapeSql(literalValue));
  }


  protected String escapeSql(String literalValue) {
    return StringEscapeUtils.escapeSql(literalValue);
  }


  /**
   * Convert a {@link ConcatenatedField} into standards compliant sql.
   *
   * @param concatenatedField the field to generate SQL for
   * @return a string representation of the field literal
   */
  protected abstract String getSqlFrom(ConcatenatedField concatenatedField);


  /**
   * Get the name of a function.
   *
   * @param function the function to get the name of
   * @return a string which is the name of the function
   */
  protected String getSqlFrom(Function function) {
    switch (function.getType()) {
      case COUNT:
        if (function.getArguments().isEmpty()) {
          return "COUNT(*)";
        } else if (function.getArguments().size() == 1) {
          return "COUNT(" + getSqlFrom(function.getArguments().get(0)) + ")";
        } else {
          throw new IllegalArgumentException("The COUNT function should have only have one or zero arguments. This function has "
              + function.getArguments().size());
        }
      case AVERAGE:
        return "AVG(" + getSqlFrom(function.getArguments().get(0)) + ")";
      case LENGTH:
        return getSqlforLength(function);
      case MAX:
      case MIN:
      case SUM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The " + function.getType()
              + " function should have only one argument. This function has " + function.getArguments().size());
        }

        return function.getType().name() + "(" + getSqlFrom(function.getArguments().get(0)) + ")";

      case IS_NULL:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The IS_NULL function should have two arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForIsNull(function);

      case MOD:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The MOD function should have two arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForMod(function);

      case SUBSTRING:
        if (function.getArguments().size() != 3) {
          throw new IllegalArgumentException("The SUBSTRING function should have three arguments. This function has "
              + function.getArguments().size());
        }

        return getSubstringFunctionName() + "(" + getSqlFrom(function.getArguments().get(0)) + ", "
            + getSqlFrom(function.getArguments().get(1)) + ", " + getSqlFrom(function.getArguments().get(2)) + ")";

      case YYYYMMDD_TO_DATE:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The YYYYMMDD_TO_DATE function should have one argument. This function has "
              + function.getArguments().size());
        }

        return getSqlForYYYYMMDDToDate(function);

      case DATE_TO_YYYYMMDD:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The DATE_TO_YYYYMMDD function should have one argument. This function has "
              + function.getArguments().size());
        }

        return getSqlForDateToYyyymmdd(function);

      case DATE_TO_YYYYMMDDHHMMSS:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The DATE_TO_YYYYMMDDHHMMSS function should have one argument. This function has "
              + function.getArguments().size());
        }

        return getSqlForDateToYyyymmddHHmmss(function);

      case NOW:
        if (!function.getArguments().isEmpty()) {
          throw new IllegalArgumentException("The NOW function should have zero arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForNow(function);

      case DAYS_BETWEEN:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The DAYS_BETWEEN function should have two arguments. This function has"
              + function.getArguments().size());
        }

        return getSqlForDaysBetween(function.getArguments().get(0), function.getArguments().get(1));

      case MONTHS_BETWEEN:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The MONTHS_BETWEEN function should have two arguments. This function has"
              + function.getArguments().size());
        }

        return getSqlForMonthsBetween(function.getArguments().get(0), function.getArguments().get(1));

      case COALESCE:
        if (function.getArguments().size() == 0) {
          throw new IllegalArgumentException("The COALESCE function requires at least one argument. This function has"
              + function.getArguments().size());
        }

        StringBuilder expression = new StringBuilder();
        expression.append(getCoalesceFunctionName()).append('(');
        boolean first = true;
        for (AliasedField f : function.getArguments()) {
          if (!first) {
            expression.append(", ");
          }

          expression.append(getSqlFrom(f));
          first = false;
        }

        expression.append(')');
        return expression.toString();

      case LEFT_TRIM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LEFT_TRIM function should have one argument. This function has "
              + function.getArguments().size());
        }

        return leftTrim(function);

      case RIGHT_TRIM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The RIGHT_TRIM function should have one argument. This function has "
              + function.getArguments().size());
        }

        return rightTrim(function);

      case ADD_DAYS:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ADD_DAYS function should have two arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForAddDays(function);

      case ADD_MONTHS:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ADD_MONTHS function should have two arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForAddMonths(function);

      case ROUND:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ROUND function should have two arguments. This function has "
              + function.getArguments().size());
        }

        return getSqlForRound(function);

      case FLOOR:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The FLOOR function should have one argument. This function has "
              + function.getArguments().size());
        }

        return getSqlForFloor(function);

      case RANDOM:
        return getSqlForRandom();

      case RANDOM_STRING:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The RANDOM_STRING function should have one argument. This function has "
              + function.getArguments().size());
        }

        return getSqlForRandomString(function);

      case LOWER:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LOWER function should have one argument. This function has "
              + function.getArguments().size());
        }
        return getSqlForLower(function);

      case UPPER:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The UPPER function should have one argument. This function has "
              + function.getArguments().size());
        }
        return getSqlForUpper(function);

      case POWER:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The POWER function should have two arguments. This function has "
              + function.getArguments().size());
        }
        return getSqlForPower(function);

      case LEFT_PAD:
        if (function.getArguments().size() != 3) {
          throw new IllegalArgumentException("The LEFT_PAD function should have three arguments. This function has "
              + function.getArguments().size());
        }
        return getSqlForLeftPad(function.getArguments().get(0), function.getArguments().get(1), function.getArguments().get(2));

      case LAST_DAY_OF_MONTH:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LAST_DAY_OF_MONTH function should have one argument. This function has"
              + function.getArguments().size());
        }

        return getSqlForLastDayOfMonth(function.getArguments().get(0));

      default:
        throw new UnsupportedOperationException("This database does not currently support the [" + function.getType()
            + "] function");
    }
  }


  /**
   * Converts the power function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#power(AliasedField,
   *      AliasedField)
   */
  private String getSqlForPower(Function function) {
    return String.format("POWER(%s, %s)", getSqlFrom(function.getArguments().get(0)), getSqlFrom(function.getArguments().get(1)));
  }


  /**
   * Converts the mod function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForMod(Function function) {
    return String.format("MOD(%s, %s)", getSqlFrom(function.getArguments().get(0)), getSqlFrom(function.getArguments().get(1)));
  }


  /**
   * Converts the ROUND function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#round(AliasedField,
   *      AliasedField)
   */
  protected String getSqlForRound(Function function) {
    return "ROUND(" + getSqlFrom(function.getArguments().get(0)) + ", " + getSqlFrom(function.getArguments().get(1)) + ")";
  }


  /**
   * Converts the FLOOR function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#floor(AliasedField)
   */
  protected String getSqlForFloor(Function function) {
    return "FLOOR(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the LENGTH function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#length(AliasedField)
   */
  protected String getSqlforLength(Function function){
    return String.format("LENGTH(%s)", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @return The name of the coalesce function
   */
  protected String getCoalesceFunctionName() {
    return "COALESCE";
  }


  /**
   * Performs the ANSI SQL date difference returning an interval in days.
   *
   * @param toDate The date we are subtracting from
   * @param fromDate The date we are subtracting
   * @return a string representation of the SQL
   */
  protected abstract String getSqlForDaysBetween(AliasedField toDate, AliasedField fromDate);


  /**
   * The number of whole months between two dates. The logic used is equivalent
   * to
   * {@link Months#monthsBetween(org.joda.time.ReadableInstant, org.joda.time.ReadableInstant)}
   * .
   * <p>
   * As an example, assuming two dates are in the same year and the
   * {@code fromDate} is from two months prior to the {@code toDate} (i.e.
   * {@code MONTH(toDate) - MONTH(fromDate) = 2)} then:
   * </p>
   * <ul>
   * <li>If the {@code toDate} day of the month is greater than or equal to the
   * {@code fromDate} day of the month, then the difference is two months;
   * <li>If the {@code toDate} day of the month lies on the end of the month,
   * then the difference is two months, to account for month length differences
   * (e.g. 31 Jan &gt; 28 Feb = 1; 30 Jan &gt; 27 Feb = 0);
   * <li>Otherwise, the difference is one (e.g. if the day of {@code fromDate}
   * &gt; day of {@code toDate}).
   * </ul>
   *
   * @param toDate The date we are subtracting from
   * @param fromDate The date we are subtracting
   * @return a string representation of the SQL
   */
  protected abstract String getSqlForMonthsBetween(AliasedField toDate, AliasedField fromDate);


  /**
   * Produce SQL for finding the last day of the month
   *
   * @param date the date for which the last day of its month will be found.
   * @return a string representation of the SQL for finding the last day of the month.
   */
  protected abstract String getSqlForLastDayOfMonth(AliasedField date);


  /**
   * Gets the function name required to perform a substring command.
   * <p>
   * The default is provided here and should be overridden in child classes as
   * neccessary.
   * </p>
   *
   * @return The substring function name.
   */
  protected String getSubstringFunctionName() {
    return "SUBSTRING";
  }


  /**
   * Converts the cast function into SQL.
   *
   * @param cast the cast to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlFrom(Cast cast) {
    return String.format("CAST(%s AS %s)", getSqlFrom(cast.getExpression()),
      getColumnRepresentation(cast.getDataType(), cast.getWidth(), cast.getScale()));
  }


  /**
   * Gets the column representation for the datatype, etc.
   *
   * @param dataType the column datatype.
   * @param width the column width.
   * @param scale the column scale.
   * @return a string representation of the column definition.
   */
  protected abstract String getColumnRepresentation(DataType dataType, int width, int scale);


  /**
   * Converts the isNull function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForIsNull(Function function);


  /**
   * Converts the DateToYyyymmdd function into SQL. Assumes an 8 digit date is
   * supplied in YYYYMMDD using the {@linkplain DataType#STRING} format. TODO:
   * does it?
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForDateToYyyymmdd(Function function);


  /**
   * Converts the DateToYyyymmddHHmmss function into SQL. Assumes an 8 digit
   * date concatenated with a 6 digit time is supplied in YYYYMMDDHHmmss using
   * the {@linkplain DataType#STRING} format.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForDateToYyyymmddHHmmss(Function function);


  /**
   * Converts the YYYYMMDDToDate function into SQL. Assumes an 8 digit date is
   * supplied in YYYYMMDD using the {@linkplain DataType#STRING} format.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForYYYYMMDDToDate(Function function);


  /**
   * Converts the Now function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForNow(Function function);


  /**
   * Converts the LEFT_TRIM function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String leftTrim(Function function);


  /**
   * Converts the RIGHT_TRIM function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String rightTrim(Function function);


  /**
   * Converts the LEFT_PAD function into SQL. This is the same format used for
   * H2, MySQL and Oracle. SqlServer implementation overrides this function.
   *
   * @param field The field to pad
   * @param length The length of the padding
   * @param character The character to use for the padding
   * @return string representation of the SQL.
   */
  protected String getSqlForLeftPad(AliasedField field, AliasedField length, AliasedField character) {
    return "LPAD(" + getSqlFrom(field) + ", " + getSqlFrom(length) + ", " + getSqlFrom(character) + ")";
  }


  /**
   * Converts the ADD_DAYS function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForAddDays(Function function);


  /**
   * Converts the ADD_MONTHS function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForAddMonths(Function function);


  /**
   * Converts the RANDOM function into SQL. This returns a random number between 0 and 1.
   *
   * @return a string representation of the SQL.
   */
  protected String getSqlForRandom() {
    return "RAND()";
  }


  /**
   * Converts the RANDOM_STRING function into SQL.
   *
   * @param function the function representing the desired length of the
   *          generated string.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#randomString(AliasedField)
   */
  protected abstract String getSqlForRandomString(Function function);


  /**
   * Converts the <code>LOWER</code> function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForLower(Function function) {
    return "LOWER(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the <code>UPPER</code> function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForUpper(Function function) {
    return "UPPER(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Convert a {@link FieldFromSelect} into sql select.
   *
   * @param field the field to generate a sql for
   * @return a string representation of the field from select
   */
  protected String getSqlFrom(FieldFromSelect field) {
    return getSqlFrom(field.getSelectStatement());
  }


  /**
   * Convert a {@link FieldFromSelect} into sql select.
   *
   * @param field the field to generate a sql for
   * @return a string representation of the field from select
   */
  protected String getSqlFrom(FieldFromSelectFirst field) {
    return getSqlFrom(field.getSelectFirstStatement());
  }


  /**
   * Converts a {@link MathsField} into SQL.
   *
   * @param field the field to convert.
   * @return a string representation of the field.
   */
  protected String getSqlFrom(MathsField field) {
    return String.format("%s %s %s", getSqlFrom(field.getLeftField()), field.getOperator(), getSqlFrom(field.getRightField()));
  }


  /**
   * Converts a {@link BracketedExpression} into SQL.
   *
   * @param expression the bracket expression to convert.
   * @return a string representation of the expression.
   */
  protected String getSqlFrom(BracketedExpression expression) {
    return String.format("(%s)", getSqlFrom(expression.getInnerExpression()));
  }


  /**
   * Convert a {@link String} containing a value into a SQL string literal.
   *
   * @param literalValue value of string literal.
   * @return quoted string literal.
   */
  protected String getSqlFrom(String literalValue) {
    return makeStringLiteral(literalValue);
  }


  /**
   * Convert a {@link LocalDate} to a SQL string literal.
   *
   * @param literalValue value of date literal.
   * @return SQL date literal.
   */
  protected String getSqlFrom(LocalDate literalValue) {
    return String.format("DATE '%s'", literalValue.toString("yyyy-MM-dd"));
  }


  /**
   * Convert a boolean to a SQL string literal.
   *
   * @param literalValue value of boolean literal.
   * @return SQL boolean literal.
   */
  protected String getSqlFrom(Boolean literalValue) {
    return literalValue ? "1" : "0";
  }


  /**
   * Convert the an Object criterion value (i.e. right hand side) to valid SQL
   * based on its type.
   *
   * @param value the object to convert to a string
   * @return a string representation of the object
   */
  protected String getSqlForCriterionValue(Object value) {
    if (value instanceof String) {
      return getSqlFrom((String) value);
    }

    if (value instanceof Boolean) {
      return getSqlFrom((Boolean) value);
    }

    if (value instanceof LocalDate) {
      return getSqlFrom((LocalDate) value);
    }

    if (value instanceof Criterion) {
      return getSqlFrom((Criterion) value);
    }

    if (value instanceof AliasedField) {
      return getSqlFrom((AliasedField) value);
    }

    return value.toString();
  }


  /**
   * Convert a criterion into a string expression of the form
   * "[operand] [operator] [operand]".
   *
   * @param criterion the criterion to convert
   * @param operator the operator to use in the expression
   * @return a string representation of the criterion
   */
  protected String getOperatorLine(Criterion criterion, String operator) {
    return getSqlFrom(criterion.getField()) + " " + operator + " " + getSqlForCriterionValue(criterion.getValue());
  }


  /**
   * Converts a structured {@link SelectStatement} to the equivalent SQL text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(SelectStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    return getSqlFrom(statement);
  }


  /**
   * Converts a structured {@link SelectFirstStatement} to the equivalent SQL
   * text.
   *
   * @param statement the statement to convert
   * @return a string containing the SQL to run against the database
   */
  public String convertStatementToSQL(SelectFirstStatement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Cannot convert a null statement to SQL");
    }

    if (statement.getOrderBys().isEmpty()) {
      throw new IllegalArgumentException("Invalid select first statement - missing order by clause");
    }

    return getSqlFrom(statement);
  }


  /**
   * Converts a structured {@code SELECT} statement to a hash representation.
   *
   * @param statement the statement to convert
   * @return A hash representation of {@code statement}.
   */
  public String convertStatementToHash(SelectStatement statement) {
    return DigestUtils.md5Hex(convertStatementToSQL(statement));
  }


  /**
   * Converts a structured {@code SELECT} statement to a hash representation.
   *
   * @param statement the statement to convert
   * @return A hash representation of {@code statement}.
   */
  public String convertStatementToHash(SelectFirstStatement statement) {
    return DigestUtils.md5Hex(convertStatementToSQL(statement));
  }


  /**
   * Creates an SQL statement to insert values with positional parameterised
   * fields based on the insert statement specified.
   *
   * @param statement the insert statement to build an SQL query for
   * @param metadata the metadata for the database
   * @return a string containing a parameterised insert query for the specified
   *         table
   */
  public String buildParameterisedInsert(final InsertStatement statement, Schema metadata) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create parameterised SQL for a blank table");
    }

    if (metadata == null) {
      throw new IllegalArgumentException("Cannot specify null for the source metadata");
    }

    if (!metadata.tableExists(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create parameterised SQL for table [" + destinationTableName
          + "] without metadata");
    }

    Table destinationTable = metadata.getTable(destinationTableName);

    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder values = new StringBuilder(") VALUES (");

    // -- Work out the literal values...
    //
    Map<String, String> literalValueMap = new HashMap<>();
    for (AliasedField f : statement.getFields()) {
      literalValueMap.put(f.getAlias().toUpperCase(), literalValue(f));
    }
    for (Entry<String, AliasedField> value : statement.getFieldDefaults().entrySet()) {
      literalValueMap.put(value.getKey().toUpperCase(), literalValue(value.getValue()));
    }

    // -- Add the preamble...
    //
    sqlBuilder.append("INSERT INTO ");
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);
    sqlBuilder.append(" (");

    boolean first = true;
    for (Column currentColumn : destinationTable.columns()) {
      if (!first) {
        sqlBuilder.append(", ");
        values.append(", ");
      }
      first = false;

      sqlBuilder.append(currentColumn.getName());

      String literalValue = literalValueMap.get(currentColumn.getName().toUpperCase());
      if (literalValue == null) {
        values.append(getSqlFrom(new SqlParameter(currentColumn)));
      } else {
        values.append(literalValue);
      }
    }
    values.append(")");
    sqlBuilder.append(values);
    return sqlBuilder.toString();
  }


  /**
   * Creates an SQL statement to insert specific values into the columns
   * specified.
   *
   * @param statement The insert statement to build an SQL query for.
   * @param metadata the database schema. If null, the SQL statement will be
   *          treated "as is". If not null, the schema will be used to decorate
   *          the statement further with the default values from any columns not
   *          specified.
   * @param idTable the ID table. Only required if the table has a
   *          non-autonumbered id column and the schema has been supplied.
   * @return a string containing a specific value insert query for the specified
   *         table and column values.
   */
  protected List<String> buildSpecificValueInsert(final InsertStatement statement, Schema metadata, Table idTable) {
    List<String> result = new LinkedList<>();

    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create specified value insert SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder values = new StringBuilder("VALUES (");

    // -- Add the preamble...
    //
    sqlBuilder.append("INSERT INTO ");
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);
    sqlBuilder.append(" (");

    Set<String> columnNamesAdded = new HashSet<>();

    boolean firstField = true;
    for (AliasedField fieldWithValue : statement.getValues()) {
      if (!firstField) {
        sqlBuilder.append(", ");
        values.append(", ");
      }

      if (StringUtils.isBlank(fieldWithValue.getAlias())) {
        throw new IllegalArgumentException("Field value in insert statement does not have an alias");
      }

      sqlBuilder.append(fieldWithValue.getAlias());
      values.append(getSqlFrom(fieldWithValue));

      columnNamesAdded.add(fieldWithValue.getAlias().toUpperCase());

      firstField = false;
    }

    // If we have a schema, then we can add defaults for missing column values
    if (metadata != null) {
      for (Column currentColumn : metadata.getTable(destinationTableName).columns()) {

        // Default date columns to null and skip columns we've already added.
        if (columnNamesAdded.contains(currentColumn.getName().toUpperCase())) {
          continue;
        }

        // Allow identity columns to be defaulted by the database - nothing to
        // do
        if (currentColumn.isAutoNumbered()) {
          continue;
        }

        // Non-autonumbered identity columns should be populated using the id
        // table
        if (currentColumn.getName().equalsIgnoreCase("id")) {
          sqlBuilder.append(", ");
          values.append(", ");

          result.addAll(buildSimpleAutonumberUpdate(statement.getTable(), "id", idTable, ID_INCREMENTOR_TABLE_COLUMN_NAME,
            ID_INCREMENTOR_TABLE_COLUMN_VALUE));

          String fieldValue = autoNumberId(statement, idTable);
          if (StringUtils.isNotEmpty(fieldValue)) {
            sqlBuilder.append("id");
            values.append(fieldValue);
          }
          continue;
        }

        // If there is a default for the field, use it
        if (statement.getFieldDefaults().containsKey(currentColumn.getName())) {
          AliasedField fieldWithValue = statement.getFieldDefaults().get(currentColumn.getName());

          sqlBuilder.append(", ");
          values.append(", ");

          sqlBuilder.append(fieldWithValue.getAlias());
          values.append(literalValue(fieldWithValue));
          continue;
        }

      }
    }

    sqlBuilder.append(") ");
    values.append(")");

    sqlBuilder.append(values);

    result.add(sqlBuilder.toString());

    return result;
  }


  /**
   * Builds SQL to get the autonumber value.
   *
   * @param statement the insert statement to get for.
   * @param idTable the ID Table.
   * @return SQL fetching the AutoNumber value.
   */
  private String autoNumberId(InsertStatement statement, Table idTable) {
    AliasedField idValue = nextIdValue(statement.getTable(), null, idTable, ID_INCREMENTOR_TABLE_COLUMN_NAME,
      ID_INCREMENTOR_TABLE_COLUMN_VALUE);
    return getSqlFrom(idValue);
  }


  /**
   * Infers the value of the field in a format suitable for direct substitution
   * into an sql statement.
   * <p>
   * If the supplied field is not a {@link FieldLiteral}, {@code null} will be
   * returned. If it is, the type will be taken directly from the
   * {@linkplain FieldLiteral} itself and single quotes (') added to the value
   * as appropriate.
   * </p>
   * <p>
   * This method also escapes the characters in the value to be suitable to pass
   * to an SQL query.
   * </p>
   *
   * @param field The field to generate the SQL literal for.
   * @return The literal value.
   */
  private String literalValue(AliasedField field) {
    if (field instanceof FieldLiteral && !(field instanceof NullFieldLiteral)) {
      return getSqlFrom((FieldLiteral) field);
    }

    if (field instanceof NullFieldLiteral) {
      return "null";
    }

    return null;
  }


  /**
   * Creates an SQL statement to delete rows from a table based on the
   * {@linkplain DeleteStatement} specified.
   *
   * @param statement the delete statement to build an SQL query for
   * @return a string containing a parameterised delete query for the specified
   *         table
   */
  protected String getSqlFrom(final DeleteStatement statement) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();

    // Add the preamble
    sqlBuilder.append("DELETE FROM ");

    // Now add the from clause
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);

    // Add a table alias if necessary
    if (!statement.getTable().getAlias().equals("")) {
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
   * Creates an SQL statement to update values with positional parameterised
   * fields based on the update statement specified.
   *
   * @param statement the insert statement to build an SQL query for
   * @return a string containing a parameterised insert query for the specified
   *         table
   */
  protected String getSqlFrom(final UpdateStatement statement) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();

    // Add the preamble
    sqlBuilder.append("UPDATE ");

    // Now add the from clause
    sqlBuilder.append(schemaNamePrefix(statement.getTable()));
    sqlBuilder.append(destinationTableName);

    // Add a table alias if necessary
    if (!statement.getTable().getAlias().equals("")) {
      sqlBuilder.append(String.format(" %s", statement.getTable().getAlias()));
    }

    // Put in the standard fields
    sqlBuilder.append(getUpdateStatementSetFieldSql(statement.getFields()));

    // Now put the where clause in
    if (statement.getWhereCriterion() != null) {
      sqlBuilder.append(" WHERE ");
      sqlBuilder.append(getSqlFrom(statement.getWhereCriterion()));
    }

    return sqlBuilder.toString();
  }


  /**
   * Creates an SQL statement to merge values with into a table.
   *
   * @param statement the insert statement to build an SQL query for.
   * @return a string containing a parameterised insert query for the specified
   *         table.
   */
  protected abstract String getSqlFrom(final MergeStatement statement);


  /**
   * Throws {@link IllegalArgumentException} if the select statement has hints.
   *
   * @param statement The select statement.
   * @param errorMessage The message for the exception.
   */
  protected void checkSelectStatementHasNoHints(SelectStatement statement, String errorMessage) {
    if (!statement.getHints().isEmpty()) {
      throw new IllegalArgumentException(errorMessage);
    }
  }


  /**
   * Returns the SET statement for an SQL UPDATE statement based on the
   * {@link List} of {@link AliasedField}s provided.
   *
   * @param fields The {@link List} of {@link AliasedField}s to create the SET
   *          statement from
   * @return The SET statement as a string
   */
  protected String getUpdateStatementSetFieldSql(final List<AliasedField> fields) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(" SET ");

    Iterable<String> setStatements = Iterables.transform(fields, new com.google.common.base.Function<AliasedField, String>() {

      @Override
      public String apply(AliasedField field) {
        return field.getAlias() + " = " + getSqlFrom(field);
      }

    });

    sqlBuilder.append(Joiner.on(", ").join(setStatements));
    return sqlBuilder.toString();
  }


  /**
   * Creates a new {@link InsertStatement} where the source table has been
   * expanded out into a {@link SelectStatement}.
   * <p>
   * The expansion will match fields in the destination to fields in the source
   * table using their names. If a field with the matching name cannot be found
   * then the literal value will be firstly sourced from the
   * <i>fieldDefaults</i> map. If it cannot be found in that map, then the
   * default for the field type will be used.
   * </p>
   *
   * @param statement the source statement to expand
   * @param metadata the table metadata from the database
   * @return a new instance of {@link InsertStatement} with an expanded from
   *         table definition
   */
  protected InsertStatement expandInsertStatement(final InsertStatement statement, Schema metadata) {
    // If we're neither specified the source table nor the select statement then
    // throw and exception
    if (statement.getFromTable() == null && statement.getSelectStatement() == null) {
      throw new IllegalArgumentException("Cannot expand insert statement as it has no from table specified");
    }

    // If we've already got a select statement then just return a copy of the
    // source insert statement
    if (statement.getSelectStatement() != null) {
      return copyInsertStatement(statement);
    }

    Map<String, AliasedField> fieldDefaults = statement.getFieldDefaults();

    InsertStatement result = new InsertStatement();

    // Copy the destination table
    result.into(statement.getTable());

    // Expand the from table
    String sourceTableName = statement.getFromTable().getName();
    String destinationTableName = statement.getTable().getName();

    // Perform a couple of checks
    if (!metadata.tableExists(sourceTableName)) {
      throw new IllegalArgumentException("Source table [" + sourceTableName + "] is not available in the database metadata");
    }

    if (!metadata.tableExists(destinationTableName)) {
      throw new IllegalArgumentException("Destination table [" + destinationTableName
          + "] is not available in the database metadata");
    }

    // Convert the source table field list to a map for convenience
    Map<String, Column> sourceColumns = new HashMap<>();
    for (Column currentColumn : metadata.getTable(sourceTableName).columns()) {
      // Convert everything to the same case to avoid match failure based on
      // case.
      sourceColumns.put(currentColumn.getName().toUpperCase(), currentColumn);
    }

    // Build up the select statement from field list
    SelectStatement selectStatement = new SelectStatement();

    for (Column currentColumn : metadata.getTable(destinationTableName).columns()) {
      String currentColumnName = currentColumn.getName();

      // Add the destination column
      result.getFields().add(new FieldReference(currentColumnName));

      // If there is a default for this column in the defaults list then use it
      if (fieldDefaults.containsKey(currentColumnName)) {
        selectStatement.getFields().add(fieldDefaults.get(currentColumnName));
        continue;
      }

      // If there is a column in the source table with the same name then link
      // them
      // and move on to the next column
      if (sourceColumns.containsKey(currentColumnName.toUpperCase())) {
        selectStatement.getFields().add(new FieldReference(currentColumnName));
        continue;
      }
    }

    // Set the source table
    selectStatement.from(statement.getFromTable());

    result.from(selectStatement);

    return result;
  }


  /**
   * Copies an insert statement to a duplicate instance.
   *
   * @param statement the {@linkplain InsertStatement} to copy
   * @return a new instance of the {@linkplain InsertStatement}
   */
  protected InsertStatement copyInsertStatement(final InsertStatement statement) {
    InsertStatement result = new InsertStatement();

    // Copy the destination table
    result.into(statement.getTable());

    // Copy the fields
    result.getFields().addAll(statement.getFields());

    // Copy the select statement, if any
    if (statement.getSelectStatement() != null) {
      result.from(statement.getSelectStatement().deepCopy());
    }

    // Copy the source table, if any
    if (statement.getFromTable() != null) {
      result.from(statement.getFromTable());
    }

    return result;
  }


  /**
   * @return The {@link DatabaseType}
   */
  public abstract DatabaseType getDatabaseType();


  /**
   * Whether this table has any BLOB columns.
   *
   * @param table The table.
   * @return true if the table has one or more BLOB columns.
   */
  protected boolean tableHasBlobColumns(Table table) {
    for (Column column : table.columns()) {
      if (column.getType() == DataType.BLOB) {
        return true;
      }
    }
    return false;
  }


  /**
   * Builds a simple repair script for AutoNumbers that deletes and inserts a
   * value.
   *
   * @param dataTable the table to update for.
   * @param generatedFieldName Name of the field which has a generated value to
   *          build an update statement for.
   * @param autoNumberTable the table to insert the autonumber to.
   * @param nameColumn the name of the name column.
   * @param valueColumn the name of the value column.
   * @return SQL allowing the repair of the AutoNumber table.
   */
  private List<String> buildSimpleAutonumberUpdate(TableReference dataTable, String generatedFieldName, Table autoNumberTable,
      String nameColumn, String valueColumn) {
    String autoNumberName = getAutoNumberName(dataTable.getName());

    if (autoNumberName.equals("autonumber")) {
      return new ArrayList<>();
    }

    List<String> sql = new ArrayList<>();
    sql.add(String.format("DELETE FROM %s where %s = '%s'", qualifiedTableName(autoNumberTable), nameColumn,
      autoNumberName));
    sql.add(String.format("INSERT INTO %s (%s, %s) VALUES('%s', (%s))", qualifiedTableName(autoNumberTable),
      nameColumn, valueColumn, autoNumberName, getExistingMaxAutoNumberValue(dataTable, generatedFieldName)));

    return sql;
  }


  protected String schemaNamePrefix(@SuppressWarnings("unused") Table tableRef) {
    return schemaNamePrefix();
  }


  /**
   * Builds SQL statements to repair an autonumber value with a specific
   * {@code value}.
   *
   * @param autoNumberName the name of the autonumber to update.
   * @param value the value to update with.
   * @param tableName the name of the table to update.
   * @param nameColumn the name of the column that holds the autonumber name.
   * @param valueColumn the name of the column that holds the autonumber value.
   * @return SQL allowing repairing of an autonumber.
   */
  public List<String> buildSpecificAutonumberUpdate(String autoNumberName, long value, String tableName, String nameColumn,
      String valueColumn) {
    String actualAutoNumberName = getAutoNumberName(autoNumberName);

    if (actualAutoNumberName.equals("autonumber")) {
      return new ArrayList<>();
    }

    List<String> sql = new ArrayList<>();
    sql.add(String.format("DELETE FROM %s%s where %s = '%s'", schemaNamePrefix(), tableName, nameColumn, actualAutoNumberName));
    sql.add(String.format("INSERT INTO %s%s (%s, %s) VALUES('%s', %d)", schemaNamePrefix(), tableName, nameColumn, valueColumn,
      actualAutoNumberName, value));

    return sql;
  }


  /**
   * Builds SQL to repair the value of an AutoNumber.
   *
   * @param dataTable the table to update for.
   * @param fieldName Name of the generated field to update for.
   * @param tableName the name of the Autonumber table.
   * @param nameColumn the name of the column holding the autonumber name.
   * @param valueColumn the name of the column holding the autonumber value.
   * @return SQL allowing the repair of the AutoNumber.
   */
  public List<String> buildAutonumberUpdate(TableReference dataTable, String fieldName, String tableName, String nameColumn,
      String valueColumn) {
    String autoNumberName = getAutoNumberName(dataTable.getName());

    if (autoNumberName.equals("autonumber")) {
      return new ArrayList<>();
    }

    StringBuilder sql = new StringBuilder();
    sql.append("MERGE INTO ");
    sql.append(schemaNamePrefix());
    sql.append(tableName);
    sql.append(" A ");
    sql.append("USING (");
    sql.append(getExistingMaxAutoNumberValue(dataTable, fieldName));
    sql.append(") S ");
    sql.append("ON (A.");
    sql.append(nameColumn);
    sql.append(" = '");
    sql.append(autoNumberName);
    sql.append("') ");
    sql.append("WHEN MATCHED THEN UPDATE SET A.");
    sql.append(valueColumn);
    sql.append(" = S.CurrentValue WHERE A.");
    sql.append(valueColumn);
    sql.append(" < S.CurrentValue ");
    sql.append("WHEN NOT MATCHED THEN INSERT (");
    sql.append(nameColumn);
    sql.append(", ");
    sql.append(valueColumn);
    sql.append(") VALUES ('");
    sql.append(autoNumberName);
    sql.append("', S.CurrentValue)");

    return ImmutableList.of(sql.toString());
  }


  /**
   * Builds SQL to get the maximum value of the specified column on the
   * specified {@code dataTable}.
   *
   * @param dataTable the table to query over.
   * @param fieldName Name of the field to query over for the max value.
   * @return SQL getting the maximum value from the {@code dataTable}.
   */
  protected String getExistingMaxAutoNumberValue(TableReference dataTable, String fieldName) {
    return getSqlFrom(new SelectStatement(Function.isnull(
      new MathsField(Function.max(new FieldReference(fieldName)), MathsOperator.PLUS, new FieldLiteral(1)), new FieldLiteral(1))
        .as("CurrentValue")).from(dataTable));
  }


  /**
   * Creates a field reference to provide id column values.
   *
   * @param sourceTable the source table.
   * @param sourceReference a reference lookup to add the ID to.
   * @param autoNumberTable the name of the table to query over.
   * @param nameColumn the name of the column holding the Autonumber name.
   * @param valueColumn the name of the column holding the Autonumber value.
   * @return a field reference.
   */
  public AliasedField nextIdValue(TableReference sourceTable, TableReference sourceReference, Table autoNumberTable,
      String nameColumn, String valueColumn) {
    String autoNumberName = getAutoNumberName(sourceTable.getName());

    if (sourceReference == null) {
      return new FieldFromSelect(new SelectStatement(Function.isnull(new FieldReference(valueColumn), new FieldLiteral(1))).from(
        new TableReference(autoNumberTable.getName())).where(
        new Criterion(Operator.EQ, new FieldReference(nameColumn), autoNumberName)));
    } else {
      return new MathsField(new FieldFromSelect(new SelectStatement(Function.isnull(new FieldReference(valueColumn),
        new FieldLiteral(0))).from(new TableReference(autoNumberTable.getName())).where(
        new Criterion(Operator.EQ, new FieldReference(nameColumn), autoNumberName))), MathsOperator.PLUS, new FieldReference(
          sourceReference, "id"));
    }
  }


  /**
   * Gets the autonumber name for the {@code destinationReference}.
   *
   * @param destinationReference the table name to get the autonumber name for.
   * @return the autonumber name.
   */
  protected String getAutoNumberName(String destinationReference) {
    String autoNumberName = destinationReference;
    if (autoNumberName.contains("_")) {
      autoNumberName = autoNumberName.substring(0, autoNumberName.lastIndexOf('_'));
    }
    return autoNumberName;
  }


  /**
   * @param identifier Unique identifier for trace file name, can be null.
   * @return Sql required to turn on tracing, or null if tracing is not
   *         supported.
   */
  public List<String> buildSQLToStartTracing(@SuppressWarnings("unused") String identifier) {
    return null;
  }


  /**
   * @return Sql required to turn on tracing, or null if tracing is not
   *         supported.
   */
  public List<String> buildSQLToStopTracing() {
    return null;
  }


  /**
   * Creates the SQL representation of a column data type.
   *
   * @param column The column to get the SQL representation for.
   * @param includeNullability Indicates whether or not the produced SQL should
   *          include nullability of the column.
   * @param includeDefaultValue Indicates whether or not the produced SQL should
   *          include the default value of the column.
   * @param includeColumnType ndicates whether or not the produced SQL should
   *          include the type of the column.
   * @return The SQL representation for the column type.
   */
  protected String sqlRepresentationOfColumnType(Column column, boolean includeNullability, boolean includeDefaultValue, boolean includeColumnType) {
    String sql = "";
    StringBuilder suffix = new StringBuilder("");
    if (includeDefaultValue) {
      suffix = new StringBuilder(StringUtils.isNotEmpty(column.getDefaultValue()) ? " DEFAULT " + sqlForDefaultClauseLiteral(column) : "");
    }

    if (includeNullability) {
      suffix.append(column.isNullable() ? " NULL" : " NOT NULL");
    }

    if (includeColumnType) {
      sql = getColumnRepresentation(column.getType(), column.getWidth(), column.getScale()) + suffix;
    } else {
      sql = suffix.toString();
    }

    return sql;
  }


  /**
   * Creates the representation of the default clause literal value.
   * @param column The column whose default will be converted.
   *
   * @return An SQL fragment representing the literal in a DEFAULT clause in an SQL statement
   */
  protected String sqlForDefaultClauseLiteral(Column column) {
    return getSqlFrom(new FieldLiteral(column.getDefaultValue(), column.getType()));
  }


  /**
   * Creates the SQL representation of a column data type.
   *
   * @param column The column to map.
   * @param includeNullability Indicates whether or not the produced SQL should
   *          include nullability of the column.
   * @return The SQL representation for the column type.
   * @see #sqlRepresentationOfColumnType(Column, boolean, boolean, boolean)
   */
  protected String sqlRepresentationOfColumnType(Column column, boolean includeNullability) {
    return sqlRepresentationOfColumnType(column, includeNullability, true, true);
  }


  /**
   * Creates the SQL representation of a column data type.
   *
   * @param column The column to map.
   * @return The SQL representation for the column type.
   * @see #sqlRepresentationOfColumnType(Column, boolean, boolean, boolean)
   */
  protected String sqlRepresentationOfColumnType(Column column) {
    StringBuilder defaultSqlRepresentation = new StringBuilder(sqlRepresentationOfColumnType(column, false, true, true));

    // Many RDBMS implementations get funny about specifying nullability at all
    // on autonumbered columns, and it's irrelevant in any case, so we just
    // avoid it.
      if (!column.isAutoNumbered()) {
        defaultSqlRepresentation.append(column.isNullable() ? "" : " NOT NULL");
      }

    return defaultSqlRepresentation.toString();
  }


  /**
   * Scans the specified {@link Table} for any autonumbered columns and returns
   * that {@link Column} if it is found, or null otherwise.
   *
   * @param table The table to check.
   * @return The autonumber column, or null if none exists.
   */
  protected Column getAutoIncrementColumnForTable(Table table) {
    for (Column column : table.columns()) {
      if (column.isAutoNumbered()) {
        return column;
      }
    }
    return null;
  }


  /**
   * Generate the SQL to add a column to a table.
   *
   * @param table The table to add the column to (The column will already have
   *          been added to this Table view)
   * @param column The column to add to the specified table.
   * @return The SQL statements to add a column to a table.
   */
  public abstract Collection<String> alterTableAddColumnStatements(Table table, Column column);


  /**
   * Generate the SQL to change an existing column on a table.
   *
   * @param table The table to change the column definition on. (The column will
   *          already have been altered in this Table view)
   * @param oldColumn The old column definition.
   * @param newColumn The new column definition.
   * @return The SQL statements to modify the specified column.
   */
  public abstract Collection<String> alterTableChangeColumnStatements(Table table, Column oldColumn, Column newColumn);


  /**
   * Generate the SQL to drop a column from a table.
   *
   * @param table The table to drop the column from. (The column will already
   *          have been dropped from this Table view)
   * @param column The column to drop from the specified table.
   * @return The SQL to drop the specified column.
   */
  public abstract Collection<String> alterTableDropColumnStatements(Table table, Column column);


  /**
   * Generate the SQL to drop an index from a table.
   *
   * @param table The table to drop the index from.
   * @param indexToBeRemoved The index to be dropped.
   * @return The SQL to drop the specified index.
   */
  public Collection<String> indexDropStatements(@SuppressWarnings("unused") Table table, Index indexToBeRemoved) {
    return Arrays.asList("DROP INDEX " + indexToBeRemoved.getName());
  }


  /**
   * Creates a qualified (with schema prefix) table name string, from a table reference.
   *
   * @param table The table metadata.
   * @return The table's qualified name.
   */
  protected String qualifiedTableName(Table table) {
    return schemaNamePrefix() + table.getName();
  }


  /**
   * Generates the SQL to create a table and insert the data specified in the {@link SelectStatement}.
   *
   * @param table The table to create.
   * @param selectStatement The {@link SelectStatement}
   * @return A collection of SQL statements
   */
  public Collection<String> addTableFromStatements(Table table, SelectStatement selectStatement) {
    return ImmutableList.<String>builder()
      .addAll(
        tableDeploymentStatements(table)
      )
      .addAll(convertStatementToSQL(
        insert().into(tableRef(table.getName())).from(selectStatement))
      )
      .build();
  }


  /**
   * Generates the SQL to add an index to an existing table.
   *
   * @param table The existing table.
   * @param index The new index being added.
   * @return A collection of SQL statements.
   */
  public Collection<String> addIndexStatements(Table table, Index index) {
    return Collections.singletonList(indexDeploymentStatement(table, index));
  }


  /**
   * Generate the SQL to deploy an index on a table.
   *
   * @param table The table to deploy the index on.
   * @param index The index to deploy on the table.
   * @return The SQL to deploy the index on the table.
   */
  protected String indexDeploymentStatement(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ")
      .append(schemaNamePrefix())
      .append(index.getName())
      .append(" ON ")
      .append(schemaNamePrefix())
      .append(table.getName())
      .append(" (")
      .append(Joiner.on(", ").join(index.columnNames()))
      .append(')');

    return statement.toString();
  }


  /**
   * Decorate the table name in an appropriate manner for temporary table in the
   * relevant database.
   *
   * @param undecoratedName core name.
   * @return decorated version.
   */
  public String decorateTemporaryTableName(String undecoratedName) {
    return undecoratedName;
  }


  /**
   * Sets up a parameter on a {@link NamedParameterPreparedStatement} with the specified value
   * from a {@link Record}.
   *
   * @param statement The {@link PreparedStatement} to set up
   * @param parameter The name of the parameter.
   * @param value The value, expressed as a string
   */
  public void prepareStatementParameter(NamedParameterPreparedStatement statement, SqlParameter parameter, String value) {
    try {
      switch (parameter.getMetadata().getType()) {
        case BOOLEAN:
          if (value != null) {
            statement.setBoolean(parameter, Boolean.valueOf(value));
          } else {
            statement.setObject(parameter, null);
          }
          break;
        case DATE:
          if (value != null) {
            statement.setDate(parameter, java.sql.Date.valueOf(value));
          } else {
            statement.setObject(parameter, null);
          }
          break;
        case DECIMAL:
          // this avoids mis-parsing 123.45 when we're in a
          // comma-for-decimal-point locale
          // (BigDecimal always expects a period)
          statement.setBigDecimal(parameter, value == null ? null : new BigDecimal(value));
          break;
        case STRING:
          // since web-9161 for *ALL* databases
          // - we are using EmptyStringHQLAssistant
          // - and store empty strings as null
          if (value == null || value.equals("")) {
            statement.setString(parameter, null);
          } else {
            statement.setString(parameter, value);
          }
          break;
        case INTEGER:
          if (value != null) {
            statement.setInt(parameter, Integer.parseInt(value));
          } else {
            statement.setObject(parameter, null);
          }
          break;
        case BIG_INTEGER:
          if (value != null) {
            statement.setLong(parameter, Long.parseLong(value));
          } else {
            statement.setObject(parameter, null);
          }
          break;
        case BLOB:
          if (value == null || value.equals("")) {
            statement.setBlob(parameter, new byte[] {});
          } else {
            statement.setBlob(parameter, /* Replace with java.util.Base64.Encoder once we have Java 8 */ Base64.decodeBase64(value));
          }
          break;
        case CLOB:
          if (value == null || value.equals("")) {
            statement.setString(parameter, null);
          } else {
            statement.setString(parameter, value);
          }
          break;

        default:
          throw new RuntimeException(String.format("Unexpected DataType [%s]", parameter.getMetadata().getType()));
      }
    } catch (SQLException e) {
      throw new RuntimeException(String.format("Error setting parameter value, column [%s], value [%s] on prepared statement",
        parameter.getMetadata().getName(), value), e);
    }
  }


  /**
   * Converts a column's value to a safe string. The code will replace nulls
   * with the supplied <code>valueForNull</code> and convert decimals into a
   * database safe representation.
   *
   * @param column the column that the source value applies to
   * @param sourceValue the value of the column
   * @param valueForNull the value to use when the column is null
   * @return a safe string representation of the column's value
   */
  protected String convertColumnValueToDatabaseSafeString(Column column, String sourceValue, String valueForNull) {

    Comparable<?> comparableType = RecordHelper.convertToComparableType(column, sourceValue);

    // Return the valueForNull if the comparableType was null.
    if (comparableType == null) {
      return valueForNull;
    } else if (comparableType instanceof Boolean) {
      return (Boolean) comparableType ? "1" : "0";
    } else {
      return comparableType.toString();
    }
  }


  /**
   * Formats the SQL statement provided.
   *
   * @param sqlStatement The statement to format
   * @return the formatted SQL statement
   */
  public String formatSqlStatement(final String sqlStatement) {
    return sqlStatement + ";";
  }


  /**
   * Convert a string to an SQL comment
   *
   * @param string The comment string
   * @return An SQL comment containing the comment string
   */
  public String convertCommentToSQL(String string) {
    return "-- "+string;
  }


  /**
   * @param sql The SQL to test
   * @return true if the sql provided is a comment
   */
  public boolean sqlIsComment(String sql) {
    return sql.startsWith("--") && !sql.contains("\n"); // multi-line statements may have comments as the top line
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseSafeStringToRecordValueConverter#databaseSafeStringtoRecordValue(org.alfasoftware.morf.metadata.DataType,
   *      java.sql.ResultSet, int, java.lang.String)
   */
  @Override
  public String databaseSafeStringtoRecordValue(DataType type, ResultSet resultSet, int columnIndex, String columnName) {
    try {
      if (type == DataType.BLOB) {

        try (InputStream inputStream = resultSet.getBinaryStream(columnIndex)) {
          if (inputStream == null) {
            return null;
          }
          try (
            InputStreamReader reader = new InputStreamReader(
              // Replace with java.util.Base64.Encoder once we have Java 8
              new Base64InputStream(
                inputStream,
                true /* encode */,
                -1 /* line length (unlimited) */,
                new byte[] {} /* line terminator (none) */
              ),
              Charsets.UTF_8)) {
            StringWriter stringWriter = new StringWriter();
            IOUtils.copy(reader, stringWriter);
            return stringWriter.toString();
          }
        }
      } else if (type == DataType.DECIMAL) {

        BigDecimal decimal = resultSet.getBigDecimal(columnIndex);

        if (decimal == null) {
          return null;
        }
        return formatDecimal(decimal);

      } else if (type == DataType.BOOLEAN) {

        // All implementations of boolean use a numeric type which should safely
        // cast to a boolean
        if (resultSet.getString(columnIndex) == null) {
          return null;
        }
        return String.valueOf(resultSet.getBoolean(columnIndex));

      } else if (type == DataType.DATE) {

        if (resultSet.getString(columnIndex) == null) {
          return null;
        }
        return LocalDate.fromDateFields(resultSet.getDate(columnIndex)).toString("yyyy-MM-dd");

      } else {
        return resultSet.getString(columnIndex);
      }

    } catch (SQLException e) {
      throw new RuntimeSqlException("Error retrieving value from result set with name [" + columnName + "]", e);
    } catch (IOException e) {
      throw new RuntimeException("Error converting binary value from result set with name [" + columnName + "]", e);
    }
  }


  /**
   * Returns the non key fields from a merge statement.
   *
   * @param statement a merge statement
   * @return the non key fields
   */
  protected Iterable<AliasedField> getNonKeyFieldsFromMergeStatement(final MergeStatement statement) {
    return Iterables.filter(statement.getSelectStatement().getFields(), new Predicate<AliasedField>() {
      @Override
      public boolean apply(AliasedField input) {
        boolean keyField = false;
        for (AliasedField field : statement.getTableUniqueKey()) {
          if (input.getImpliedName().equals(field.getImpliedName())) keyField = true;
        }
        return !keyField;
      }
    });
  }


  /**
   * Creates matching conditions SQL for a list of fields used in the ON section
   * of a Merge Statement. For example:
   * "table1.fieldA = table2.fieldA AND table1.fieldB = table2.fieldB".
   *
   * @param aliasedFields an iterable of aliased fields.
   * @param selectAlias the alias of the select statement of a merge statement.
   * @param targetTableName the name of the target table into which to merge.
   * @return The corresponding SQL
   */
  protected String matchConditionSqlForMergeFields(final Iterable<AliasedField> aliasedFields, final String selectAlias,
      final String targetTableName) {
    return Joiner.on(" AND ").join(createEqualsSqlForFields(aliasedFields, selectAlias, targetTableName));
  }


  /**
   * Creates assignment SQL for a list of fields used in the SET section of a
   * Merge Statement. For example:
   * "table1.fieldA = table2.fieldA, table1.fieldB = table2.fieldB".
   *
   * @param aliasedFields an iterable of aliased fields.
   * @param selectAlias the alias of the select statement of a merge statement.
   * @param targetTableName the name of the target table into which to merge.
   * @return The resulting SQL
   */
  protected String assignmentSqlForMergeFields(final Iterable<AliasedField> aliasedFields, final String selectAlias,
      final String targetTableName) {
    return Joiner.on(", ").join(createEqualsSqlForFields(aliasedFields, selectAlias, targetTableName));
  }


  /**
   * Transforms the entries of a list of Aliased Fields so that each entry
   * become a String: "fieldName = selectAlias.fieldName".
   *
   * @param aliasedFields an iterable of aliased fields.
   * @param selectAlias the alias of the source select of the merge statement
   * @return an iterable containing strings
   */
  private Iterable<String> createEqualsSqlForFields(final Iterable<AliasedField> aliasedFields, final String selectAlias,
      final String targetTableName) {
    return Iterables.transform(aliasedFields, new com.google.common.base.Function<AliasedField, String>() {
      @Override
      public String apply(AliasedField field) {
        return String.format("%s.%s = %s.%s", targetTableName, field.getImpliedName(), selectAlias, field.getImpliedName());
      }
    });
  }


  /**
   * Formats decimal values as strings
   *
   * @param decimal Value to format.
   * @return The decimal value as a string.
   */
  private String formatDecimal(BigDecimal decimal) {

    String decimalString = decimal.toPlainString();

    // remove trailing zeros and clean up decimal point
    int decimalPointIdx = decimalString.indexOf('.');

    if (decimalPointIdx == -1) {
      return decimalString;
    }

    int idx;
    // Work back from the end of the string, looking for zeros, stopping when we find anything that isn't...
    for (idx = decimalString.length()-1; idx>=decimalPointIdx; idx--) {
      if (decimalString.charAt(idx) != '0') break;
    }

    // If the last thing is now the decimal point, step past that...
    if (idx == decimalPointIdx) {
      idx--;
    }

    return decimalString.substring(0, idx+1);
  }


  /**
   * Construct the old table for a change column
   * @param table The table to change
   * @param oldColumn The old column
   * @param newColumn The new column
   * @return The 'old' table
   *
   */
  protected Table oldTableForChangeColumn(Table table, final Column oldColumn, Column newColumn) {
    return new ChangeColumn(table.getName(), oldColumn, newColumn).reverse(schema(table)).getTable(table.getName());
  }


  /**
   * Drops and recreates the triggers and supporting items for the target table.
   *
   * @param table the table for which to rebuild triggers
   * @return a collection of sql statements to execute
   */
  public abstract Collection<String> rebuildTriggers(Table table);


  /**
   * Indicates whether the dialect uses NVARCHAR or VARCHAR to store string values.
   *
   * @return true if NVARCHAR is used, false is VARCHAR is used.
   */
  public boolean usesNVARCHARforStrings() {
    return false;
  }


   /**
   * @return true if the dialect supports window functions (e.g. PARTITION BY).
   *
   **/
   public abstract boolean supportsWindowFunctions();


  /**
   * Convert a {@link WindowFunction} into standards compliant SQL.
   * @param windowFunctionField The field to convert
   * @return The resulting SQL
   **/
  protected String getSqlFrom(final WindowFunction windowFunctionField) {

    StringBuilder statement = new StringBuilder().append(getSqlFrom(windowFunctionField.getFunction()));

    statement.append(" OVER (");

    if (windowFunctionField.getPartitionBys().size() > 0) {

      statement.append("PARTITION BY ");

      boolean firstField = true;
      for (AliasedField field : windowFunctionField.getPartitionBys()) {
        if (!firstField) {
          statement.append(", ");
        }

        statement.append(getSqlFrom(field));
        firstField = false;
      }
    }

    if (windowFunctionField.getOrderBys().size() > 0) {
      statement.append(" ORDER BY ");

      boolean firstField = true;
      for (AliasedField field : windowFunctionField.getOrderBys()) {
        if (!firstField) {
          statement.append(", ");
        }

        statement.append(getSqlForOrderByField(field));
        firstField = false;
      }
    }

    statement.append(")");

    return statement.toString();
  }


  /**
   * Class representing the structor of an ID Table.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  public static final class IdTable implements Table {

    /**
     * The name of the ID Table.
     */
    private final String tableName;


    /**
     * For testing only - the tableName might not be appropriate for your
     * dialect!
     *
     * @param tableName table name for the id table.
     * @return {@link IdTable}.
     */
    public static IdTable withDeterministicName(String tableName) {
      return new IdTable(tableName);
    }


    /**
     * Use this to create an {@link IdTable} which is guaranteed to have a legal
     * name for the dialect.
     *
     * @param dialect {@link SqlDialect} that knows what temp table names are
     *          allowed.
     * @param prefix prefix for the unique name generated.
     * @return {@link IdTable}
     */
    public static IdTable withPrefix(SqlDialect dialect, String prefix) {
      return new IdTable(dialect.decorateTemporaryTableName(prefix + RandomStringUtils.randomAlphabetic(5)));
    }


    /**
     * Constructor used by tests for generating a predictable table name.
     *
     * @param tableName table name for the temporary table.
     */
    private IdTable(String tableName) {
      this.tableName = tableName;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#indexes()
     */
    @Override
    public List<Index> indexes() {
      return new ArrayList<>();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#getName()
     */
    @Override
    public String getName() {
      return tableName;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#columns()
     */
    @Override
    public List<Column> columns() {
      List<Column> columns = new ArrayList<>();
      columns.add(column(ID_INCREMENTOR_TABLE_COLUMN_NAME, DataType.STRING, 132).primaryKey());
      columns.add(column(ID_INCREMENTOR_TABLE_COLUMN_VALUE, DataType.BIG_INTEGER, 19));

      return columns;
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Table#isTemporary()
     */
    @Override
    public boolean isTemporary() {
      return true;
    }
  }
}
