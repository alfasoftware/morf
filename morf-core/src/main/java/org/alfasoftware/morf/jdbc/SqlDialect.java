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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.alfasoftware.morf.util.SchemaValidatorUtil.validateSchemaName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.AbstractSelectStatement;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.ExceptSetOperator;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SelectStatementBuilder;
import org.alfasoftware.morf.sql.SetOperator;
import org.alfasoftware.morf.sql.SqlElementCallback;
import org.alfasoftware.morf.sql.SqlUtils;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UnionSetOperator;
import org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.BlobFieldLiteral;
import org.alfasoftware.morf.sql.element.BracketedExpression;
import org.alfasoftware.morf.sql.element.CaseStatement;
import org.alfasoftware.morf.sql.element.Cast;
import org.alfasoftware.morf.sql.element.ClobFieldLiteral;
import org.alfasoftware.morf.sql.element.ConcatenatedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldFromSelectFirst;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Function;
import org.alfasoftware.morf.sql.element.FunctionType;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.MathsField;
import org.alfasoftware.morf.sql.element.MathsOperator;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.PortableSqlFunction;
import org.alfasoftware.morf.sql.element.SequenceReference;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.sql.element.WhenCondition;
import org.alfasoftware.morf.sql.element.WindowFunction;
import org.alfasoftware.morf.upgrade.ChangeColumn;
import org.alfasoftware.morf.upgrade.SchemaAutoHealer;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.LocalDate;
import org.joda.time.Months;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.CharSource;

/**
 * Provides functionality for generating SQL statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public abstract class SqlDialect {

  /**
   *
   */
  protected static final String          ID_INCREMENTOR_TABLE_COLUMN_VALUE = "nextvalue";

  /**
   *
   */
  protected static final String          ID_INCREMENTOR_TABLE_COLUMN_NAME  = "name";

  /**
   * The width of the id field
   */
  public static final int                ID_COLUMN_WIDTH                   = 19;

  /**
   * Label to identify the real name of an object in an SQL comment
   */
  public static final String             REAL_NAME_COMMENT_LABEL           = "REALNAME";


  /**
   * Empty collection of strings that implementations can return if required.
   */
  public static final Collection<String> NO_STATEMENTS                     = ImmutableList.of();

  /**
   * IllegalArgumentException message
   */
  private static final String            CANNOT_CONVERT_NULL_STATEMENT_TO_SQL  =  "Cannot convert a null statement to SQL";

  private static final Set<FunctionType> WINDOW_FUNCTION_REQUIRING_ORDERBY = ImmutableSet.of(FunctionType.ROW_NUMBER);


  /**
   * Used as the alias for the select statement in merge statements.
   */
  protected static final String MERGE_SOURCE_ALIAS = "xmergesource";



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
    this.schemaName = validateSchemaName(schemaName);
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
      statements.addAll(indexDeploymentStatements(table, index));
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
   * Creates the SQL to deploy a database sequence.
   *
   * @param sequence The meta data for the sequence to deploy.
   * @return The statements required to deploy the table.
   */
  protected abstract Collection<String> internalSequenceDeploymentStatements(Sequence sequence);


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
   * Creates SQL script to deploy a database view.
   *
   * @param view The meta data for the view to deploy.
   * @return The statements required to deploy the view joined into a script.
   */
  public String viewDeploymentStatementsAsScript(View view) {
    final String firstLine = "-- " + getDatabaseType().identifier() + "\n";
    return viewDeploymentStatements(view)
        .stream().collect(Collectors.joining(";\n", firstLine, ";"));
  }


  /**
   * Creates SQL script to deploy a database view.
   *
   * @param view The meta data for the view to deploy.
   * @return The statements required to deploy the view joined into a script and prepared as literals.
   */
  public AliasedField viewDeploymentStatementsAsLiteral(View view) {
    return SqlUtils.clobLiteral(viewDeploymentStatementsAsScript(view));
  }

  /**
   * Creates SQL to truncate a table (may require DBA rights on some databases
   * e.g. Oracle).
   *
   * @param table The database table.
   * @return SQL statements required to clear a table and prepare it for data
   *         population.
   */
  public Collection<String> truncateTableStatements(Table table) {
    return ImmutableList.of("TRUNCATE TABLE " + schemaNamePrefix(table) + table.getName());
  }


  /**
   * Creates SQL to rename a table.
   *
   * @param from - table to rename
   * @param to - table with new name
   * @return SQL statements required to change a table name.
   */
  public Collection<String> renameTableStatements(Table from, Table to) {
    return ImmutableList.of("ALTER TABLE " + schemaNamePrefix(from) + from.getName() + " RENAME TO " + to.getName());
  }


  /**
   * Creates SQL to rename an index.
   *
   * @param table table on which the index exists
   * @param fromIndexName The index to rename
   * @param toIndexName The new index name
   * @return SQL Statements required to rename an index
   */
  public Collection<String> renameIndexStatements(Table table, String fromIndexName, String toIndexName) {
    return ImmutableList.of("ALTER INDEX " + schemaNamePrefix(table) + fromIndexName + " RENAME TO " + toIndexName);
  }


  /**
   * @param table - table to perform this action on
   * @param oldPrimaryKeyColumns - the existing primary key columns
   * @param newPrimaryKeyColumns - the new primary key columns
   * @return SQL Statements required to change the primary key columns
   */
  public abstract Collection<String> changePrimaryKeyColumns(Table table, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns);


  /**
   * Creates SQL to delete all records from a table (doesn't use truncate).
   *
   * @param table the database table to clear
   * @return SQL statements required to clear the table.
   */
  public Collection<String> deleteAllFromTableStatements(Table table) {
    return ImmutableList.of("DELETE FROM " + schemaNamePrefix(table) + table.getName());
  }


  /**
   * Creates SQL to execute prior to bulk-inserting to a table.
   *
   * @param table {@link Table} to be inserted to.
   * @param insertingUnderAutonumLimit  Determines whether we are inserting under an auto-numbering limit.
   * @return SQL statements to be executed prior to insert.
   */
  @SuppressWarnings("unused")
  public Collection<String> preInsertWithPresetAutonumStatements(Table table, boolean insertingUnderAutonumLimit) {
    return ImmutableList.of();
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
  public void postInsertWithPresetAutonumStatements(Table table, SqlScriptExecutor executor, Connection connection, boolean insertingUnderAutonumLimit) {
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
  public void repairAutoNumberStartPosition(Table table, SqlScriptExecutor executor, Connection connection) {
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
   * Windowing function usually have the following syntax
   * <b>FUNCTION</b> OVER (<b>partitionClause</b> <b>orderByClause</b>)
   * The partitionClause is generally optional, but the orderByClause is mandatory for certain functions. This method
   * specifies for which function the orderByClause is mandatory.
   * Certain dialects may behave differently with respect to this behaviour, which will be overridden as per behaviour
   * required of the dialect.
   *
   * @param function the windowing function
   * @return true if orderBy clause is mandatory
   */
  protected boolean requiresOrderByForWindowFunction(Function function) {
    return WINDOW_FUNCTION_REQUIRING_ORDERBY.contains(function.getType());
  }


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
      return ImmutableList.of(convertStatementToSQL(update));
    } else if (statement instanceof DeleteStatement) {
      DeleteStatement delete = (DeleteStatement) statement;
      return ImmutableList.of(convertStatementToSQL(delete));
    } else if (statement instanceof TruncateStatement) {
      TruncateStatement truncateStatement = (TruncateStatement) statement;
      return ImmutableList.of(convertStatementToSQL(truncateStatement));
    } else if (statement instanceof MergeStatement) {
      MergeStatement merge = (MergeStatement) statement;
      return ImmutableList.of(convertStatementToSQL(merge));
    } else {
      throw new UnsupportedOperationException("Executed statement operation not supported for [" + statement.getClass() + "]");
    }
  }


  /**
   * Checks whether the table the InsertStatement is referring to has any
   * autonumbered columns
   *
   * @param statement The statement to check
   * @param databaseMetadata the database schema
   * @return true if autonumbered, false otherwise
   */
  protected boolean isAutonumbered(InsertStatement statement, Schema databaseMetadata) {
    if (statement.getTable() != null) {
      Table tableInserting = databaseMetadata.getTable(statement.getTable().getName());
      if (tableInserting == null) {
        throw new IllegalStateException(String.format("Unable to find [%s] in schema", statement.getTable().getName()));
      }
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
    return truncateTableStatements(SchemaUtils.table(statement.getTable().getName())).iterator().next();
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
    return 2000;
  }


  /**
   * @deprecated this method returns the legacy value and is primarily for backwards compatibility.
   * Please use {@link SqlDialect#fetchSizeForBulkSelects()} for the new recommended default value.
   * @see SqlDialect#fetchSizeForBulkSelects()
   *
   * @return The number of rows to try and fetch at a time (default) when
   *         performing bulk select operations.
   */
  @Deprecated
  public int legacyFetchSizeForBulkSelects() {
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
   * @deprecated this method returns the legacy value and is primarily for backwards compatibility.
   * Please use {@link SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()} for the new recommended default value.
   * @see SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()
   *
   * @return The number of rows to try and fetch at a time (default) when
   *         performing bulk select operations.
   */
  @Deprecated
  public int legacyFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming() {
    return legacyFetchSizeForBulkSelects();
  }


  /**
   * @return The schema prefix (including the dot) or blank if the schema's blank.
   */
  public String schemaNamePrefix() {
    if (StringUtils.isEmpty(schemaName)) {
      return "";
    }

    return schemaName.toUpperCase() + ".";
  }


  /**
   * @param table The table for which the schema name will be retrieved
   * @return Base implementation calls {@link #schemaNamePrefix()}.
   */
  protected String schemaNamePrefix(@SuppressWarnings("unused") Table table) {
    return schemaNamePrefix();
  }


  /**
   * @param sequence The sequence for which the schema name will be retrieved
   * @return Base implementation calls {@link #schemaNamePrefix()}.
   */
  protected String schemaNamePrefix(@SuppressWarnings("unused") Sequence sequence) {
    return schemaNamePrefix();
  }


  /**
   * @param tableRef The table for which the schema name will be retrieved
   * @return full table name that includes a schema name and DB-link if present
   */
  protected String tableNameWithSchemaName(TableReference tableRef) {
    if (StringUtils.isEmpty(tableRef.getSchemaName())) {
      return schemaNamePrefix() + tableRef.getName();
    } else {
      return tableRef.getSchemaName().toUpperCase() + "." + tableRef.getName();
    }
  }


  /**
   * Creates SQL to drop the named table.
   *
   * @param table The table to drop
   * @return The SQL statements as strings.
   */
  public Collection<String> dropStatements(Table table) {
    return dropTables(Lists.newArrayList(table), false, false);
  }


  /**
   * Creates SQL to drop the named tables.
   *
   * @param ifExists Should check if table exists before dropping
   * @param cascade If supported by the dialect, will drop tables/views that depend on any of the provided tables
   * @param tables The tables to drop
   * @return The SQL statements as strings.
   */
  public Collection<String> dropTables(List<Table> tables, boolean ifExists, boolean cascade) {
    return ImmutableList.of(
            "DROP TABLE "
                    + (ifExists ? "IF EXISTS " : "")
                    + tables.stream().map(table -> schemaNamePrefix(table) + table.getName()).collect(Collectors.joining(", "))
                    + (cascade ? " CASCADE" : "")
    );
  }


  /**
   * Creates SQL to drop the named view.
   *
   * @param view The view to drop
   * @return The SQL statements as strings.
   */
  public Collection<String> dropStatements(View view) {
    return ImmutableList.of("DROP VIEW " + schemaNamePrefix() + view.getName() + " IF EXISTS CASCADE");
  }


  /**
   * Creates SQL to deploy a database sequence.
   *
   * @param sequence The meta data for the sequence to deploy.
   * @return The statements required to deploy the sequence.
   */
  public Collection<String> sequenceDeploymentStatements(Sequence sequence) {
    Builder<String> statements = ImmutableList.<String>builder();

    // Create the table deployment statement
    statements.addAll(internalSequenceDeploymentStatements(sequence));

    return statements.build();
  }


  /**
   * Creates SQL to drop the named sequence.
   *
   * @param sequence The sequence to drop
   * @return The SQL statements as strings.
   */
  public Collection<String> dropStatements(Sequence sequence) {
    return ImmutableList.of("DROP SEQUENCE IF EXISTS " + schemaNamePrefix() + sequence.getName());
  }


  /**
   * Some databases might require forced serial table creation instead of a parallel one.
   * @return true if forced serial import is enabled
   */
  public abstract boolean useForcedSerialImport();


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
  protected String getSqlFrom(SelectStatement stmt) {
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
    appendExceptSet(result, stmt);
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
   * Returns any SQL code which should be added between a <code>UPDATE</code> and the table
   * for dialect-specific reasons.
   *
   * @param updateStatement The update statement
   * @return Any hint code required.
   */
  protected String updateStatementPreTableDirectives(@SuppressWarnings("unused") UpdateStatement updateStatement) {
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
  protected void appendAlias(StringBuilder result, AliasedField currentField) {
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
  protected String getSqlFrom(SelectFirstStatement stmt) {
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
   * @param result union set operators will be appended here
   * @param stmt statement with set operators
   */
  protected void appendUnionSet(StringBuilder result, SelectStatement stmt) {
    if (stmt.getSetOperators() != null) {
      for (SetOperator operator : stmt.getSetOperators()) {
        if (operator instanceof UnionSetOperator) {
          result.append(getSqlFrom((UnionSetOperator) operator));
        }
      }
    }
  }


  /**
   * appends except set operators to the result
   *
   * @param result except set operators will be appended here
   * @param stmt   statement with set operators
   */
  protected void appendExceptSet(StringBuilder result, SelectStatement stmt) {
    if (stmt.getSetOperators() != null) {
      for (SetOperator operator : stmt.getSetOperators()) {
        if (operator instanceof ExceptSetOperator) {
          result.append(getSqlFrom((ExceptSetOperator) operator));
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
      result.append(tableNameWithSchemaName(stmt.getTable()));

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
  protected String getSqlForOrderByField(AliasedField currentOrderByField) {

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
   * @param innerJoinKeyword usually an INNER JOIN, but this can be changed for optimisations
   */
  protected void appendJoin(StringBuilder result, Join join, String innerJoinKeyword) {
    // Put the type in
    switch (join.getType()) {
      case INNER_JOIN:
        result.append(" ").append(innerJoinKeyword).append(" ");
        break;
      case LEFT_OUTER_JOIN:
        result.append(" LEFT OUTER JOIN ");
        break;
      case FULL_OUTER_JOIN:
        result.append(" FULL OUTER JOIN ");
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
      result.append(tableNameWithSchemaName(join.getTable()));

      // And add an alias if necessary
      if (!join.getTable().getAlias().isEmpty()) {
        result.append(" ").append(join.getTable().getAlias());
      }
    }

    if (join.getCriterion() != null) {
      result.append(" ON ");

      // Then put the join fields into the output
      result.append(getSqlFrom(join.getCriterion()));

    } else if (join.getType() == JoinType.LEFT_OUTER_JOIN || join.getType() == JoinType.FULL_OUTER_JOIN) {
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
   * @param operator the union set operation to convert.
   * @return a string representation of the union set operation.
   */
  protected String getSqlFrom(UnionSetOperator operator) {
    return String.format(" %s %s", operator.getUnionStrategy() == UnionStrategy.ALL ? "UNION ALL" : "UNION",
      getSqlFrom(operator.getSelectStatement()));
  }


  /**
   * Converts a {@link ExceptSetOperator} into SQL.
   *
   * @param operator the except set operator to convert.
   * @return a string representation of the except set operator.
   */
  protected String getSqlFrom(ExceptSetOperator operator) {
    return String.format(" EXCEPT %s",
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
  protected List<String> getSqlFromInsert(InsertStatement stmt, Schema metadata, Table idTable) {
    if (stmt.getTable() == null) {
      throw new IllegalArgumentException("Cannot specify a null destination table in an insert statement");
    }

    if (stmt.getSelectStatement() == null) {
      throw new IllegalArgumentException("Cannot specify a null for the source select statement in getSqlFrom");
    }

    SelectStatement sourceStatement = stmt.getSelectStatement();

    List<String> result = new LinkedList<>();

    StringBuilder stringBuilder = new StringBuilder();

    stringBuilder.append(getSqlForInsertInto(stmt));
    stringBuilder.append(tableNameWithSchemaName(stmt.getTable()));

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
          sourceStatement = sourceStatement.shallowCopy().fields(idValue).build();
        }

        if (!explicitVersionColumn && hasColumnNamed(stmt.getTable().getName(), metadata, "version")) {
          stringBuilder.append(", version");

          // Augment the select statement
          sourceStatement = sourceStatement.shallowCopy().fields(SqlUtils.literal(0).as("version")).build();
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

    if (field instanceof SqlParameter) {
      return getSqlFrom((SqlParameter)field);
    }

    if (field instanceof BlobFieldLiteral) {
      return getSqlFrom((BlobFieldLiteral)field);
    }

    if (field instanceof ClobFieldLiteral) {
      return getSqlFrom((ClobFieldLiteral) field);
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

    if (field instanceof MergeStatement.InputField) {
      return getSqlFrom((MergeStatement.InputField) field);
    }

    if (field instanceof SequenceReference) {
      return getSqlFrom((SequenceReference) field);
    }

    if (field instanceof PortableSqlFunction) {
      return getSqlFrom((PortableSqlFunction) field);
    }

    throw new IllegalArgumentException("Aliased Field of type [" + field.getClass().getSimpleName() + "] is not supported");
  }


  /**
   * Convert {@link SqlParameter} into its SQL representation.
   *
   * @param field representing a SQL parameter
   * @return Returns the SQL representation of {@link SqlParameter}.
   */
  protected String getSqlFrom(SqlParameter field) {
    return String.format(":%s", field.getMetadata().getName());
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
        return getSqlFrom(Boolean.valueOf(field.getValue()));
      case STRING:
      case CLOB:
        return makeStringLiteral(field.getValue());
      case DATE:
        // This is the ISO standard date literal format
        return String.format("DATE '%s'", field.getValue());
      case DECIMAL:
      case BIG_INTEGER:
      case INTEGER:
        return field.getValue();
      case NULL:
        if (field.getValue() != null) {
          throw new UnsupportedOperationException("Literals of type NULL must have a null value. Got [" + field.getValue() + "]");
        }
        return "null";
      default:
        throw new UnsupportedOperationException("Cannot convert the specified field literal into an SQL literal: ["
            + field.getValue() + "]");
    }
  }


  /**
   * Converts the operation on the sequence into SQL.
   *
   * @param sequenceReference the sequence on which the operation is being performed.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlFrom(SequenceReference sequenceReference);


  /**
   * Default implementation will just return the Base64 representation of the binary data, which may not necessarily work with all SQL dialects.
   * Hence appropriate conversions to the appropriate type based on facilities provided by the dialect's SQL vendor implementation should be used.
   *
   * @param field the BLOB field literal
   * @return the SQL construct or base64 string representation of the binary value
   */
  protected String getSqlFrom(BlobFieldLiteral field) {
    return String.format("'%s'", field.getValue());
  }


  /**
   * Default implementation will just return the Field literal implementation of the getSqlFrom method.
   *
   * @param field the CLOB field literal
   * @return a string representation of the clob field literal
   */
  protected String getSqlFrom(ClobFieldLiteral field) {
    return getSqlFrom((FieldLiteral) field);
  }


  /**
   * Turn a string value into an SQL string literal which has that value.
   * <p>
   * We use {@linkplain StringUtils#isEmpty(CharSequence)} because we want to
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


  /**
  * Turn a string value into an SQL string literal which has that value.
  * This escapes single quotes as double-single quotes.
   * @param literalValue the value to escape
   * @return escaped value
  */
  protected String escapeSql(String literalValue) {
    if (literalValue == null) {
      return null;
    }
    return StringUtils.replace(literalValue, "'", "''");
  }


  /**
   * Convert a {@link ConcatenatedField} into standards compliant sql.
   *
   * @param concatenatedField the field to generate SQL for
   * @return a string representation of the field literal
   */
  protected String getSqlFrom(ConcatenatedField concatenatedField) {
    List<String> sql = new ArrayList<>();
    for (AliasedField field : concatenatedField.getConcatenationFields()) {
      // Interpret null values as empty strings
      sql.add("COALESCE(" + getSqlFrom(field) + ",'')");
    }
    return StringUtils.join(sql, " || ");
  }


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
          return getSqlForCount();
        }
        if (function.getArguments().size() == 1) {
          return getSqlForCount(function);
        }
        throw new IllegalArgumentException("The COUNT function should have only have one or zero arguments. This function has " + function.getArguments().size());

      case COUNT_DISTINCT:
        checkSingleArgument(function);
        return getSqlForCountDistinct(function);

      case AVERAGE:
        checkSingleArgument(function);
        return getSqlForAverage(function);

      case AVERAGE_DISTINCT:
        checkSingleArgument(function);
        return getSqlForAverageDistinct(function);

      case LENGTH:
        checkSingleArgument(function);
        return getSqlforLength(function);

      case BLOB_LENGTH:
        checkSingleArgument(function);
        return getSqlforBlobLength(function);

      case SOME:
        checkSingleArgument(function);
        return getSqlForSome(function);

      case EVERY:
        checkSingleArgument(function);
        return getSqlForEvery(function);

      case MAX:
        checkSingleArgument(function);
        return getSqlForMax(function);

      case MIN:
        checkSingleArgument(function);
        return getSqlForMin(function);

      case SUM:
        checkSingleArgument(function);
        return getSqlForSum(function);

      case SUM_DISTINCT:
        checkSingleArgument(function);
        return getSqlForSumDistinct(function);

      case IS_NULL:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The IS_NULL function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForIsNull(function);

      case MOD:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The MOD function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForMod(function);

      case SUBSTRING:
        if (function.getArguments().size() != 3) {
          throw new IllegalArgumentException("The SUBSTRING function should have three arguments. This function has " + function.getArguments().size());
        }
        return getSqlForSubstring(function);

      case YYYYMMDD_TO_DATE:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The YYYYMMDD_TO_DATE function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForYYYYMMDDToDate(function);

      case DATE_TO_YYYYMMDD:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The DATE_TO_YYYYMMDD function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForDateToYyyymmdd(function);

      case DATE_TO_YYYYMMDDHHMMSS:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The DATE_TO_YYYYMMDDHHMMSS function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForDateToYyyymmddHHmmss(function);

      case NOW:
        if (!function.getArguments().isEmpty()) {
          throw new IllegalArgumentException("The NOW function should have zero arguments. This function has " + function.getArguments().size());
        }
        return getSqlForNow(function);

      case DAYS_BETWEEN:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The DAYS_BETWEEN function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForDaysBetween(function.getArguments().get(0), function.getArguments().get(1));

      case MONTHS_BETWEEN:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The MONTHS_BETWEEN function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForMonthsBetween(function.getArguments().get(0), function.getArguments().get(1));

      case COALESCE:
        if (function.getArguments().size() == 0) {
          throw new IllegalArgumentException("The COALESCE function requires at least one argument. This function has " + function.getArguments().size());
        }
        return getSqlForCoalesce(function);

      case GREATEST:
        if (function.getArguments().size() == 0) {
          throw new IllegalArgumentException("The GREATEST function requires at least one argument. This function has " + function.getArguments().size());
        }
        return getSqlForGreatest(function);

      case LEAST:
        if (function.getArguments().size() == 0) {
          throw new IllegalArgumentException("The LEAST function requires at least one argument. This function has " + function.getArguments().size());
        }
        return getSqlForLeast(function);

      case TRIM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The TRIM function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForTrim(function);

      case LEFT_TRIM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LEFT_TRIM function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForLeftTrim(function);

      case RIGHT_TRIM:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The RIGHT_TRIM function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForRightTrim(function);

      case ADD_DAYS:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ADD_DAYS function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForAddDays(function);

      case ADD_MONTHS:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ADD_MONTHS function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForAddMonths(function);

      case ROUND:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The ROUND function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForRound(function);

      case FLOOR:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The FLOOR function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForFloor(function);

      case RANDOM:
        if (function.getArguments().size() != 0) {
          throw new IllegalArgumentException("The " + function.getType() + " function should have no arguments. This function has " + function.getArguments().size());
        }
        return getSqlForRandom();

      case RANDOM_STRING:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The RANDOM_STRING function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForRandomString(function);

      case LOWER:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LOWER function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForLower(function);

      case UPPER:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The UPPER function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForUpper(function);

      case POWER:
        if (function.getArguments().size() != 2) {
          throw new IllegalArgumentException("The POWER function should have two arguments. This function has " + function.getArguments().size());
        }
        return getSqlForPower(function);

      case LEFT_PAD:
        if (function.getArguments().size() != 3) {
          throw new IllegalArgumentException("The LEFT_PAD function should have three arguments. This function has " + function.getArguments().size());
        }
        return getSqlForLeftPad(function.getArguments().get(0), function.getArguments().get(1), function.getArguments().get(2));

      case RIGHT_PAD:
        if (function.getArguments().size() != 3) {
          throw new IllegalArgumentException("The RIGHT_PAD function should have three arguments. This function has " + function.getArguments().size());
        }
        return getSqlForRightPad(function.getArguments().get(0), function.getArguments().get(1), function.getArguments().get(2));

      case LAST_DAY_OF_MONTH:
        if (function.getArguments().size() != 1) {
          throw new IllegalArgumentException("The LAST_DAY_OF_MONTH function should have one argument. This function has " + function.getArguments().size());
        }
        return getSqlForLastDayOfMonth(function.getArguments().get(0));

      case ROW_NUMBER:
        if (!function.getArguments().isEmpty()) {
          throw new IllegalArgumentException("The ROW_NUMBER function should have zero arguments. This function has " + function.getArguments().size());
        }
        return getSqlForRowNumber();

      default:
        throw new UnsupportedOperationException("This database does not currently support the [" + function.getType() + "] function");
    }
  }

  private void checkSingleArgument(Function function) {
    if (function.getArguments().size() != 1) {
      throw new IllegalArgumentException("The " + function.getType() + " function should have only one argument. This function has " + function.getArguments().size());
    }
  }


  /**
   * Converts the count function into SQL.
   *
   * @return a string representation of the SQL
   */
  protected String getSqlForCount() {
    return "COUNT(*)";
  }


  /**
   * Converts the count function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForCount(Function function) {
    return "COUNT(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the count function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForCountDistinct(Function function) {
    return "COUNT(DISTINCT " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the average function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForAverage(Function function) {
    return "AVG(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the average function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForAverageDistinct(Function function) {
    return "AVG(DISTINCT " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the substring function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForSubstring(Function function) {
    return getSubstringFunctionName() + "("
        + getSqlFrom(function.getArguments().get(0)) + ", "
        + getSqlFrom(function.getArguments().get(1)) + ", "
        + getSqlFrom(function.getArguments().get(2)) + ")";
  }


  /**
   * Converts the coalesce function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForCoalesce(Function function) {
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
  }


  /**
   * Converts the greatest function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForGreatest(Function function) {
    return getGreatestFunctionName() + '(' + Joiner.on(", ").join(function.getArguments().stream().map(f -> getSqlFrom(f)).iterator()) + ')';
  }


  /**
   * Converts the least function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForLeast(Function function) {
    return getLeastFunctionName() + '(' + Joiner.on(", ").join(function.getArguments().stream().map(f -> getSqlFrom(f)).iterator()) + ')';
  }


  /**
   * Converts the max function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForMax(Function function) {
    return "MAX(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the min function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForMin(Function function) {
    return "MIN(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the sum function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForSum(Function function) {
    return "SUM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the sum function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForSumDistinct(Function function) {
    return "SUM(DISTINCT " + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the some function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForSome(Function function) {
    return getSqlForMax(function);
  }


  /**
   * Converts the every function into SQL.
   *
   * @param function the function details
   * @return a string representation of the SQL
   */
  protected String getSqlForEvery(Function function) {
    return getSqlForMin(function);
  }


  /**
   * Converts the power function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#power(AliasedField,
   *      AliasedField)
   */
  protected String getSqlForPower(Function function) {
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
  protected String getSqlforLength(Function function) {
    return String.format("LENGTH(%s)", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * Converts the function get LENGTH of Blob data or field into SQL.
   * Use LENGTH instead of OCTET_LENGTH as they are synonymous in MySQl and PostGreSQL. In H2 LENGTH returns the correct
   * number of bytes, whereas OCTET_LENGTH returns 2 times the byte length.
   * @param function the function to convert.
   * @return a string representation of the SQL.
   * @see org.alfasoftware.morf.sql.element.Function#blobLength(AliasedField)
   */
  protected String getSqlforBlobLength(Function function) {
    return String.format("LENGTH(%s)", getSqlFrom(function.getArguments().get(0)));
  }


  /**
   * @return The name of the coalesce function
   */
  protected String getCoalesceFunctionName() {
    return "COALESCE";
  }


  /**
   * @return The name of the GREATEST function
   */
  protected String getGreatestFunctionName() {
    return "GREATEST";
  }


  /**
   * @return The name of the LEAST function
   */
  protected String getLeastFunctionName() {
    return "LEAST";
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
   * Produce SQL for getting the row number of the row in the partition
   *
   * @return a string representation of the SQL for finding the last day of the month.
   */
  protected String getSqlForRowNumber(){
    return "ROW_NUMBER()";
  }


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
      getDataTypeRepresentation(cast.getDataType(), cast.getWidth(), cast.getScale()));
  }


  /**
   * Gets the column representation for the datatype, etc.
   *
   * @param dataType the column datatype.
   * @param width the column width.
   * @param scale the column scale.
   * @return a string representation of the column definition.
   */
  protected String getDataTypeRepresentation(DataType dataType, int width, int scale) {
    return getColumnRepresentation(dataType, width, scale);
  }


  /**
   * Gets the column representation for the datatype, etc.
   *
   * @param dataType the column datatype.
   * @param width the column width.
   * @param scale the column scale.
   * @return a string representation of the column definition.
   */
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
        return "BLOB";

      case CLOB:
        return "CLOB";

      default:
        throw new UnsupportedOperationException("Cannot map column with type [" + dataType + "]");
    }
  }


  /**
   * Converts the isNull function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForIsNull(Function function) {
    return getSqlForCoalesce(function);
  }


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
   * Converts the current time function into SQL and returns the timestamp of the database in UTC.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected abstract String getSqlForNow(Function function);


  /**
   * Converts the TRIM function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForTrim(Function function) {
    return "TRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the LEFT_TRIM function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForLeftTrim(Function function) {
    return "LTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the RIGHT_TRIM function into SQL.
   *
   * @param function the function to convert.
   * @return a string representation of the SQL.
   */
  protected String getSqlForRightTrim(Function function) {
    return "RTRIM(" + getSqlFrom(function.getArguments().get(0)) + ")";
  }


  /**
   * Converts the LEFT_PAD function into SQL. This is the same format used for
   * H2, MySQL, Oracle and PostgreSQL. SqlServer implementation overrides this function.
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
   * Converts the RIGHT_PAD function into SQL. This is the same format used for
   * H2, MySQL, Oracle and PostgreSQL. SqlServer implementation overrides this function.
   *
   * @param field The field to pad
   * @param length The length of the padding
   * @param character The character to use for the padding
   * @return string representation of the SQL.
   */
  protected String getSqlForRightPad(AliasedField field, AliasedField length, AliasedField character) {
    return "RPAD(" + getSqlFrom(field) + ", " + getSqlFrom(length) + ", " + getSqlFrom(character) + ")";
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
      throw new IllegalArgumentException(CANNOT_CONVERT_NULL_STATEMENT_TO_SQL);
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
    return md5HashHexEncoded(convertStatementToSQL(statement));
  }



  /**
   * Converts a structured {@code SELECT} statement to a hash representation.
   *
   * @param statement the statement to convert
   * @return A hash representation of {@code statement}.
   */
  public String convertStatementToHash(SelectFirstStatement statement) {
    return md5HashHexEncoded(convertStatementToSQL(statement));
  }


  /**
   * @param toHash the String to convert
   * @return the md5 hash of the string.
   */
  @SuppressWarnings("deprecation")
  private String md5HashHexEncoded(String toHash) {
    try {
      return CharSource.wrap(toHash).asByteSource(StandardCharsets.UTF_8).hash(Hashing.md5()).toString();
    } catch (IOException e) {
      throw new RuntimeException("error when hashing string [" + toHash + "]", e);
    }
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
  public String buildParameterisedInsert(InsertStatement statement, Schema metadata) {
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
    sqlBuilder.append(getSqlForInsertInto(statement));
    sqlBuilder.append(tableNameWithSchemaName(statement.getTable()));
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
  protected List<String> buildSpecificValueInsert(InsertStatement statement, Schema metadata, Table idTable) {
    List<String> result = new LinkedList<>();

    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create specified value insert SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();
    StringBuilder values = new StringBuilder("VALUES (");

    // -- Add the preamble...
    //
    sqlBuilder.append(getSqlForInsertInto(statement));
    sqlBuilder.append(tableNameWithSchemaName(statement.getTable()));
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
  protected String getSqlFrom(DeleteStatement statement) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();

    // Add the preamble
    sqlBuilder.append("DELETE ");

    // For appropriate dialects, append the delete limit here
    if (statement.getLimit().isPresent() && getDeleteLimitPreFromClause(statement.getLimit().get()).isPresent()) {
      sqlBuilder.append(getDeleteLimitPreFromClause(statement.getLimit().get()).get() + " ");
    }

    sqlBuilder.append("FROM ");

    // Now add the from clause
    sqlBuilder.append(tableNameWithSchemaName(statement.getTable()));

    // Add a table alias if necessary
    if (!statement.getTable().getAlias().equals("")) {
      sqlBuilder.append(String.format(" %s", statement.getTable().getAlias()));
    }

    // Prepare to append the where clause or, for appropriate dialects, the delete limit
    if (statement.getWhereCriterion() != null || statement.getLimit().isPresent() && getDeleteLimitWhereClause(statement.getLimit().get()).isPresent()) {
      sqlBuilder.append(" WHERE ");
    }

    // Now put the where clause in
    if (statement.getWhereCriterion() != null) {
      sqlBuilder.append(getSqlFrom(statement.getWhereCriterion()));
    }

    // Append the delete limit, for appropriate dialects
    if (statement.getLimit().isPresent() && getDeleteLimitWhereClause(statement.getLimit().get()).isPresent()) {
      if (statement.getWhereCriterion() != null) {
        sqlBuilder.append(" AND ");
      }

      sqlBuilder.append(getDeleteLimitWhereClause(statement.getLimit().get()).get());
    }

    // For appropriate dialects, append the delete limit suffix
    if (statement.getLimit().isPresent() && getDeleteLimitSuffix(statement.getLimit().get()).isPresent()) {
      sqlBuilder.append(" " + getDeleteLimitSuffix(statement.getLimit().get()).get());
    }

    return sqlBuilder.toString();
  }


  /**
   * Returns the SQL that specifies the deletion limit ahead of the FROM clause, if any, for the dialect.
   *
   * @param limit The delete limit.
   * @return The SQL fragment.
   */
  protected Optional<String> getDeleteLimitPreFromClause(@SuppressWarnings("unused") int limit) {
    return Optional.empty();
  }


  /**
   * Returns the SQL that specifies the deletion limit in the WHERE clause, if any, for the dialect.
   *
   * @param limit The delete limit.
   * @return The SQL fragment.
   */
  protected Optional<String> getDeleteLimitWhereClause(@SuppressWarnings("unused") int limit) {
    return Optional.empty();
  }


  /**
   * Returns the SQL that specifies the deletion limit as a suffix, if any, for the dialect.
   *
   * @param limit The delete limit.
   * @return The SQL fragment.
   */
  protected Optional<String> getDeleteLimitSuffix(@SuppressWarnings("unused") int limit) {
    return Optional.empty();
  }


  /**
   * Creates an SQL statement to update values with positional parameterised
   * fields based on the update statement specified.
   *
   * @param statement the insert statement to build an SQL query for
   * @return a string containing a parameterised insert query for the specified
   *         table
   */
  protected String getSqlFrom(UpdateStatement statement) {
    String destinationTableName = statement.getTable().getName();

    if (StringUtils.isBlank(destinationTableName)) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    StringBuilder sqlBuilder = new StringBuilder();

    // Add the preamble
    sqlBuilder.append("UPDATE ");
    sqlBuilder.append(updateStatementPreTableDirectives(statement));

    // Now add the from clause
    sqlBuilder.append(tableNameWithSchemaName(statement.getTable()));

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
  protected String getSqlFrom(MergeStatement statement) {

    if (StringUtils.isBlank(statement.getTable().getName())) {
      throw new IllegalArgumentException("Cannot create SQL for a blank table");
    }

    checkSelectStatementHasNoHints(statement.getSelectStatement(), "MERGE may not be used with SELECT statement hints");

    final StringBuilder sqlBuilder = new StringBuilder();

    // MERGE INTO schema.Table
    sqlBuilder.append("MERGE INTO ")
              .append(tableNameWithSchemaName(statement.getTable()));

    // USING (SELECT ...) xmergesource
    sqlBuilder.append(" USING (")
              .append(getSqlFrom(statement.getSelectStatement()))
              .append(") ")
              .append(MERGE_SOURCE_ALIAS);

    // ON (Table.id = xmergesource.id)
    sqlBuilder.append(" ON (")
              .append(matchConditionSqlForMergeFields(statement, MERGE_SOURCE_ALIAS, statement.getTable().getName()))
              .append(")");

    // WHEN MATCHED THEN UPDATE ...
    if (getNonKeyFieldsFromMergeStatement(statement).iterator().hasNext()) {
      Iterable<AliasedField> updateExpressions = getMergeStatementUpdateExpressions(statement);
      String updateExpressionsSql = getMergeStatementAssignmentsSql(updateExpressions);

      sqlBuilder.append(" WHEN MATCHED THEN UPDATE SET ")
                .append(updateExpressionsSql);
    }

    // WHEN NOT MATCHED THEN INSERT ...
    String insertFieldsSql = Joiner.on(", ").join(FluentIterable.from(statement.getSelectStatement().getFields()).transform(AliasedField::getImpliedName));
    String insertValuesSql = Joiner.on(", ").join(FluentIterable.from(statement.getSelectStatement().getFields()).transform(field -> MERGE_SOURCE_ALIAS + "." + field.getImpliedName()));

    sqlBuilder.append(" WHEN NOT MATCHED THEN INSERT (")
              .append(insertFieldsSql)
              .append(") VALUES (")
              .append(insertValuesSql)
              .append(")");

    return sqlBuilder.toString();
  }


  /**
   * Convert a {@link MergeStatement.InputField} into SQL.
   *
   * @param field the field to generate SQL for
   * @return a string representation of the field
   */
  protected String getSqlFrom(MergeStatement.InputField field) {
    return MERGE_SOURCE_ALIAS + "." + field.getName();
  }


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
   * Returns the SET clause for an SQL UPDATE statement based on the
   * {@link List} of {@link AliasedField}s provided.
   *
   * @param fields The {@link List} of {@link AliasedField}s to create the SET
   *          statement from
   * @return The SET clause as a string
   */
  protected String getUpdateStatementSetFieldSql(List<AliasedField> fields) {
    return " SET " + getUpdateStatementAssignmentsSql(fields);
  }


  /**
   * Returns the assignments for the SET clause of an SQL UPDATE statement
   * based on the {@link List} of {@link AliasedField}s provided.
   *
   * @param fields The {@link List} of {@link AliasedField}s to create the assignments from
   * @return the assignments for the SET clause as a string
   */
  protected String getUpdateStatementAssignmentsSql(Iterable<AliasedField> fields) {
    Iterable<String> setStatements = Iterables.transform(fields, field -> field.getAlias() + " = " + getSqlFrom(field));
    return Joiner.on(", ").join(setStatements);
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
   * @param insertStatement the source statement to expand
   * @param metadata the table metadata from the database
   * @return a new instance of {@link InsertStatement} with an expanded from
   *         table definition
   */
  protected InsertStatement expandInsertStatement(InsertStatement insertStatement, Schema metadata) {
    // If we're neither specified the source table nor the select statement then
    // throw and exception
    if (insertStatement.getFromTable() == null && insertStatement.getSelectStatement() == null) {
      throw new IllegalArgumentException("Cannot expand insert statement as it has no from table specified");
    }

    // If we've already got a select statement then just return a copy of the
    // source insert statement
    if (insertStatement.getSelectStatement() != null) {
      return copyInsertStatement(insertStatement);
    }

    Map<String, AliasedField> fieldDefaults = insertStatement.getFieldDefaults();

    // Expand the from table
    String sourceTableName = insertStatement.getFromTable().getName();
    String destinationTableName = insertStatement.getTable().getName();

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
      sourceColumns.put(currentColumn.getUpperCaseName(), currentColumn);
    }

    // Build up the select statement from field list
    SelectStatementBuilder selectStatementBuilder = SelectStatement.select();
    List<AliasedField> resultFields = new ArrayList<>();

    for (Column currentColumn : metadata.getTable(destinationTableName).columns()) {
      String currentColumnName = currentColumn.getName();

      // Add the destination column
      resultFields.add(new FieldReference(currentColumnName));

      // If there is a default for this column in the defaults list then use it
      if (fieldDefaults.containsKey(currentColumnName)) {
        selectStatementBuilder = selectStatementBuilder.fields(fieldDefaults.get(currentColumnName));
        continue;
      }

      // If there is a column in the source table with the same name then link
      // them
      // and move on to the next column
      if (sourceColumns.containsKey(currentColumn.getUpperCaseName())) {
        selectStatementBuilder = selectStatementBuilder.fields(new FieldReference(currentColumnName));
        continue;
      }
    }
    // Set the source table
    SelectStatement selectStatement = selectStatementBuilder
        .from(insertStatement.getFromTable())
        .build();

   return InsertStatement.insert()
               .into(insertStatement.getTable())
               .fields(resultFields)
               .from(selectStatement)
               .build();
  }


  /**
   * Copies an insert statement to a duplicate instance.
   *
   * @param statement the {@linkplain InsertStatement} to copy
   * @return a new instance of the {@linkplain InsertStatement}
   */
  protected InsertStatement copyInsertStatement(InsertStatement statement) {
    return statement.shallowCopy().build();
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
  private List<String> buildSimpleAutonumberUpdate(TableReference dataTable, String generatedFieldName, Table autoNumberTable, String nameColumn, String valueColumn) {
    String autoNumberName = getAutoNumberName(dataTable.getName());

    if (autoNumberName.equals("autonumber")) {
      return new ArrayList<>();
    }

    List<String> sql = new ArrayList<>();
    sql.add(String.format("DELETE FROM %s where %s = '%s'", schemaNamePrefix(autoNumberTable) + autoNumberTable.getName(), nameColumn,
      autoNumberName));
    sql.add(String.format("INSERT INTO %s (%s, %s) VALUES('%s', (%s))", schemaNamePrefix(autoNumberTable) + autoNumberTable.getName(),
      nameColumn, valueColumn, autoNumberName, getExistingMaxAutoNumberValue(dataTable, generatedFieldName)));

    return sql;
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
    return getSqlFrom(new SelectStatement(Function.coalesce(
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
  public AliasedField nextIdValue(TableReference sourceTable, TableReference sourceReference, Table autoNumberTable, String nameColumn, String valueColumn) {
    String autoNumberName = getAutoNumberName(sourceTable.getName());

    if (sourceReference == null) {
      return new FieldFromSelect(new SelectStatement(Function.coalesce(new FieldReference(valueColumn), new FieldLiteral(1))).from(
        new TableReference(autoNumberTable.getName(), autoNumberTable.isTemporary())).where(
        new Criterion(Operator.EQ, new FieldReference(nameColumn), autoNumberName)));
    } else {
      return new MathsField(new FieldFromSelect(new SelectStatement(Function.coalesce(new FieldReference(valueColumn),
        new FieldLiteral(0))).from(new TableReference(autoNumberTable.getName(), autoNumberTable.isTemporary())).where(
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
    String sql;
    StringBuilder suffix = new StringBuilder();
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
   * Generate the SQL to run analysis on a table.
   *
   * @param table The table to run the analysis on.
   * @return The SQL statements to analyse the table.
   */
  public Collection<String> getSqlForAnalyseTable(@SuppressWarnings("unused") Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


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
    return ImmutableList.of("DROP INDEX " + indexToBeRemoved.getName());
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
        SqlUtils.insert().into(SqlUtils.tableRef(table.getName())).from(selectStatement))
      )
      .build();
  }


  /**
   * Generates the SQL to create a table and insert the data specified in the {@link SelectStatement}.
   *
   * For supported dialects, this method casts each field in the provided select using the column definition of the provided table.
   *
   * Validation is performed to confirm that the fields included in the select statement correspond to the table columns.
   *
   * @param table The table to create.
   * @param selectStatement The {@link SelectStatement}
   * @return A collection of SQL statements
   */
  public Collection<String> addTableFromStatementsWithCasting(Table table, SelectStatement selectStatement) {
    return addTableFromStatements(table, selectStatement);
  }


  /**
   * This method:
   * - Uses the provided select statement to generate a CTAS statement using a temporary name
   * - Drops the original table
   * - Renames the new table using the original table's name
   * - Adds indexes from the original table
   *
   * @param originalTable the original table this method will replace
   * @param selectStatement the statement used to populate the replacement table via a CTAS
   * @return a list of statements for the operation
   */
  public List<String> replaceTableFromStatements(Table originalTable, SelectStatement selectStatement) {

    // Due to morf's oracle table length restrictions, our temporary table name cannot be longer than 27 characters
    final Table newTable = SchemaUtils.table("tmp_" + StringUtils.substring(originalTable.getName(), 0, 23))
        .columns(originalTable.columns());

    validateStatement(originalTable, selectStatement);

    // Generate the SQL for the CTAS and post-CTAS operations
    final List<String> createTableStatements = Lists.newArrayList();
    createTableStatements.addAll(addTableFromStatementsWithCasting(newTable, selectStatement));
    createTableStatements.addAll(dropTables(ImmutableList.of(originalTable), false, true));
    createTableStatements.addAll(renameTableStatements(newTable, originalTable));
    createTableStatements.addAll(createAllIndexStatements(originalTable));

    return createTableStatements;
  }


  private void validateStatement(Table table, SelectStatement selectStatement) {
    final String separator = "\n    ";

    List<String> tableColumns = table.columns().stream().map(Column::getName).collect(toList());
    List<String> selectColumns = selectStatement.getFields().stream().map(SqlDialect::getFieldName).collect(toList());

    if (tableColumns.size() != selectColumns.size()) {
     throw new IllegalArgumentException("Number of table columns [" + tableColumns.size() + "] does not match number of select columns [" + selectColumns.size() + "].");
    }

    ImmutableList.Builder<Pair<String,String>> differences = ImmutableList.builder();
    Iterator<String> tableColumnsIterator = tableColumns.iterator();
    Iterator<String> selectColumnsIterator = selectColumns.iterator();

    while (tableColumnsIterator.hasNext() && selectColumnsIterator.hasNext()) {
      String tableColumn = tableColumnsIterator.next();
      String selectColumn = selectColumnsIterator.next();
      if (!tableColumn.equalsIgnoreCase(selectColumn)) {
        differences.add(Pair.of(tableColumn, selectColumn));
      }
    }

    List<Pair<String, String>> diffs = differences.build();
    if (!diffs.isEmpty()) {
      throw new IllegalArgumentException("Table columns do not match select columns"
                      + "\nMismatching pairs:" + separator + diffs.stream().map(p -> p.getLeft() + " <> " + p.getRight()).collect(joining(separator)));
    }
  }


  private static String getFieldName(AliasedField field) {
    if (!StringUtils.isBlank(field.getAlias())) {
      return field.getAlias();
    }
    if (!StringUtils.isBlank(field.getImpliedName())) {
      return field.getImpliedName();
    }
    return field.toString();
  }


  /**
   * For some dialects, this casting is required, as the type may not be inferred for every field in the select statement.
   * @param table the table to add the casts.
   * @param selectStatement select statements which the casts need adding to.
   * @return SelectStatement with casts.
   */
  protected SelectStatement addCastsToSelect(Table table, SelectStatement selectStatement) {
    SelectStatement statementWithCasts = selectStatement.deepCopy();
    for (int i = 0; i < table.columns().size(); i++) {
      AliasedField field = statementWithCasts.getFields().get(i);
      Column column = table.columns().get(i);

      if (fieldRequiresCast(field, column)) {
        AliasedField fieldWithCast = field.cast().asType(column.getType(), column.getWidth(), column.getScale()).build().as(column.getName());
        statementWithCasts.getFields().set(i, fieldWithCast);
      }
    }
    return statementWithCasts;
  }


  private boolean fieldRequiresCast(AliasedField field, Column column) {
    if (!(field instanceof Cast)) {
      return true;
    }

    Cast cast = (Cast) field;
    return cast.getDataType() != column.getType() || cast.getWidth() != column.getWidth() || cast.getScale() != column.getScale();
  }


  /**
   * Generates the SQL to add an index to an existing table.
   *
   * @param table The existing table.
   * @param index The new index being added.
   * @return A collection of SQL statements.
   */
  public Collection<String> addIndexStatements(Table table, Index index) {
    return indexDeploymentStatements(table, index);
  }


  /**
   * Helper method to create all index statements defined for a table
   *
   * @param table the table to create indexes for
   * @return a list of index statements
   */
  protected List<String> createAllIndexStatements(Table table) {
    List<String> indexStatements = new ArrayList<>();
    for (Index index : table.indexes()) {
      indexStatements.addAll(addIndexStatements(table, index));
    }
    return indexStatements;
  }


  /**
   * Generate the SQL to deploy an index on a table.
   *
   * @param table The table to deploy the index on.
   * @param index The index to deploy on the table.
   * @return The SQL to deploy the index on the table.
   */
  protected Collection<String> indexDeploymentStatements(Table table, Index index) {
    StringBuilder statement = new StringBuilder();

    statement.append("CREATE ");
    if (index.isUnique()) {
      statement.append("UNIQUE ");
    }
    statement.append("INDEX ")
      .append(schemaNamePrefix(table))
      .append(index.getName())
      .append(" ON ")
      .append(schemaNamePrefix(table))
      .append(table.getName())
      .append(" (")
      .append(Joiner.on(", ").join(index.columnNames()))
      .append(')');

    return ImmutableList.of(statement.toString());
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
   * Sets up parameters on a {@link NamedParameterPreparedStatement} with a set of values.
   *
   * @param statement The {@link PreparedStatement} to set up
   * @param parameters The parameters.
   * @param values The values.
   * @throws RuntimeException if a data type is not supported or if a
   *         supplied string value cannot be converted to the column data type.
   */
  public void prepareStatementParameters(NamedParameterPreparedStatement statement, Iterable<SqlParameter> parameters, DataValueLookup values) {
    parameters.forEach(parameter -> {
      try {
        prepareStatementParameters(statement, values, parameter);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Error setting parameter value, column [%s], value [%s] on prepared statement",
          parameter.getMetadata().getName(), values.getObject(parameter.getMetadata())), e);
      }
    });
  }


  /**
   * Sets up a parameter on {@link NamedParameterPreparedStatement} with a value.

   * @param statement The {@link PreparedStatement} to set up
   * @param values The values.
   * @param parameter The parameters.
   * @throws RuntimeException if a data type is not supported or if a
   *         supplied string value cannot be converted to the column data type.
   * @throws SQLException for JDBC errors.
   */
  public void prepareStatementParameters(NamedParameterPreparedStatement statement, DataValueLookup values, SqlParameter parameter) throws SQLException {
    switch (parameter.getMetadata().getType()) {
      case BIG_INTEGER:
        Long longVal = values.getLong(parameter.getImpliedName());
        if (longVal == null) {
          statement.setObject(parameter, null);
        } else {
          statement.setLong(parameter, longVal);
        }
        break;
      case BLOB:
        byte[] blobVal = values.getByteArray(parameter.getImpliedName());
        if (blobVal == null) {
          statement.setBlob(parameter, new byte[] {});
        } else {
          statement.setBlob(parameter, blobVal);
        }
        break;
      case BOOLEAN:
        prepareBooleanParameter(statement, values.getBoolean(parameter.getImpliedName()), parameter);
        break;
      case DATE:
        Date dateVal = values.getDate(parameter.getImpliedName());
        if (dateVal == null) {
          statement.setObject(parameter, null);
        } else {
          statement.setDate(parameter, new java.sql.Date(dateVal.getTime()));
        }
        break;
      case DECIMAL:
        statement.setBigDecimal(parameter, values.getBigDecimal(parameter.getImpliedName()));
        break;
      case INTEGER:
        prepareIntegerParameter(statement, values.getInteger(parameter.getImpliedName()), parameter);
        break;
      case CLOB:
      case STRING:
        String stringVal = values.getString(parameter.getImpliedName());
        if (stringVal == null || stringVal.equals("")) {
          // since web-9161 for *ALL* databases
          // - we are using EmptyStringHQLAssistant
          // - and store empty strings as null
          statement.setString(parameter, null);
        } else {
          statement.setString(parameter, stringVal);
        }
        break;
      default:
        throw new RuntimeException(String.format("Unexpected DataType [%s]", parameter.getMetadata().getType()));
    }
  }


  /**
   * Overridable behaviour for mapping an integer parameter to a prepared statement.
   *
   * @param statement The statement.
   * @param integerVal The integer value.
   * @param parameter The parameter to map to.
   * @throws SQLException If an exception occurs setting the parameter.
   */
  protected void prepareIntegerParameter(NamedParameterPreparedStatement statement, Integer integerVal, SqlParameter parameter) throws SQLException {
    if (integerVal == null) {
      statement.setObject(parameter, null);
    } else {
      statement.setInt(parameter, integerVal);
    }
  }


  /**
   * Overridable behaviour for mapping a boolean parameter to a prepared statement.
   *
   * @param statement The statement.
   * @param boolVal The boolean value.
   * @param parameter The parameter to map to.
   * @throws SQLException If an exception occurs setting the parameter.
   */
  protected void prepareBooleanParameter(NamedParameterPreparedStatement statement, Boolean boolVal, SqlParameter parameter) throws SQLException {
    if (boolVal == null) {
      statement.setObject(parameter, null);
    } else {
      statement.setBoolean(parameter, boolVal);
    }
  }


  /**
   * Formats the SQL statement provided.
   *
   * @param sqlStatement The statement to format
   * @return the formatted SQL statement
   */
  public String formatSqlStatement(String sqlStatement) {
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
   * Given an ordered list of columns and a {@link ResultSet}, creates a
   * {@link Record} from the current row.
   *
   * @param resultSet The {@link ResultSet}. Must have been advanced (using
   *          {@link ResultSet#next()}) to the appropriate row.
   * @param columns The columns, ordered according to their appearance in the
   *          {@link ResultSet}. Use {@link ResultSetMetadataSorter} to pre-sort
   *          your columns according to the {@link ResultSetMetaData} if you
   *          can't be sure that the SQL will return the columns in the precise
   *          order that you are expecting.
   * @return A {@link Record} representation of the current {@link ResultSet}
   *         row.
   */
  public Record resultSetToRecord(ResultSet resultSet, Iterable<Column> columns) {

    // Provide initial sizing hint to the array. This potentially means double-traversal
    // of the columns if the column list is not a simple list, but it's almost certainly
    // worth it to minimise the array size and prevent resizing.
    RecordBuilder recordBuilder = DataSetUtils.record()
        .withInitialColumnCount(Iterables.size(columns));

    int idx = 1;
    for (Column column : columns) {
      try {
        switch (column.getType()) {
          case BIG_INTEGER:
            long longVal = resultSet.getLong(idx);
            if (resultSet.wasNull()) {
              recordBuilder.setObject(column.getName(), null);
            } else {
              recordBuilder.setLong(column.getName(), longVal);
            }
            break;
          case BOOLEAN:
            boolean boolVal = resultSet.getBoolean(idx);
            if (resultSet.wasNull()) {
              recordBuilder.setObject(column.getName(), null);
            } else {
              recordBuilder.setBoolean(column.getName(), boolVal);
            }
            break;
          case INTEGER:
            int intVal = resultSet.getInt(idx);
            if (resultSet.wasNull()) {
              recordBuilder.setObject(column.getName(), null);
            } else {
              recordBuilder.setInteger(column.getName(), intVal);
            }
            break;
          case DATE:
            Date date = resultSet.getDate(idx);
            if (date == null) {
              recordBuilder.setObject(column.getName(), null);
            } else {
              recordBuilder.setDate(column.getName(), date);
            }
            break;
          case DECIMAL:
            recordBuilder.setBigDecimal(column.getName(), resultSet.getBigDecimal(idx));
            break;
          case BLOB:
            recordBuilder.setByteArray(column.getName(), resultSet.getBytes(idx));
            break;
          case CLOB:
          case STRING:
            recordBuilder.setString(column.getName(), resultSet.getString(idx));
            break;
          default:
            recordBuilder.setObject(column.getName(), resultSet.getObject(idx));
            break;
        }
        idx++;
      } catch (SQLException e) {
        throw new RuntimeSqlException("Error retrieving value from result set with name [" + column.getName() + "]", e);
      }
    }
    return recordBuilder;
  }


  /**
   * Returns the non key fields from a merge statement.
   *
   * @param statement a merge statement
   * @return the non key fields
   */
  protected Iterable<AliasedField> getNonKeyFieldsFromMergeStatement(MergeStatement statement) {
    Set<String> tableUniqueKey = statement.getTableUniqueKey().stream()
        .map(AliasedField::getImpliedName)
        .collect(Collectors.toSet());

    return Iterables.filter(
      statement.getSelectStatement().getFields(),
      input -> !tableUniqueKey.contains(input.getImpliedName())
    );
  }


  /**
   * Creates matching conditions SQL for a list of fields used in the ON section
   * of a Merge Statement. For example:
   * "table1.fieldA = table2.fieldA AND table1.fieldB = table2.fieldB".
   *
   * @param statement the merge statement.
   * @param selectAlias the alias of the select statement of a merge statement.
   * @param targetTableName the name of the target table into which to merge.
   * @return The corresponding SQL
   */
  protected String matchConditionSqlForMergeFields(MergeStatement statement, String selectAlias, String targetTableName) {

    Iterable<String> expressions = Iterables.transform(statement.getTableUniqueKey(),
      field -> String.format("%s.%s = %s.%s", targetTableName, field.getImpliedName(), selectAlias, field.getImpliedName()));

    return Joiner.on(" AND ").join(expressions);
  }


  /**
   * Extracts updating expressions from the given merge statement and returns them as aliased fields,
   * similarly to how update expressions are provided to the update statement. Since updating expressions
   * are optional in merge statements, uses default expressions for any missing destination fields.
   *
   * @param statement a merge statement
   * @return the updating expressions aliased as destination fields
   */
  protected Iterable<AliasedField> getMergeStatementUpdateExpressions(MergeStatement statement) {
    final Map<String, AliasedField> onUpdateExpressions = Maps.uniqueIndex(statement.getIfUpdating(), AliasedField::getImpliedName);
    final Iterable<AliasedField> nonKeyFieldsFromMergeStatement = getNonKeyFieldsFromMergeStatement(statement);

    Set<String> keyFields = FluentIterable.from(statement.getTableUniqueKey())
      .transform(AliasedField::getImpliedName)
      .toSet();

    List<String> listOfKeyFieldsWithUpdateExpression = FluentIterable.from(onUpdateExpressions.keySet())
      .filter(a -> keyFields.contains(a))
      .toList();

    if (!listOfKeyFieldsWithUpdateExpression.isEmpty()) {
      throw new IllegalArgumentException("MergeStatement tries to update a key field via the update expressions " + listOfKeyFieldsWithUpdateExpression + " in " + statement);
    }

    // Note that we use the source select statement's fields here as we assume that they are
    // appropriately aliased to match the target table as part of the API contract
    return Iterables.transform(nonKeyFieldsFromMergeStatement,
      field -> onUpdateExpressions.getOrDefault(field.getImpliedName(), new MergeStatement.InputField(field.getImpliedName()).as(field.getImpliedName())));
  }


  /**
   * Returns the assignments for updating part of an SQL MERGE statement
   * based on the given {@link AliasedField}s.
   *
   * @param fields The {@link AliasedField}s to create the assignments from
   * @return the assignments for the updating part as a string
   */
  protected String getMergeStatementAssignmentsSql(Iterable<AliasedField> fields) {
    return getUpdateStatementAssignmentsSql(fields);
  }


  /**
   * Construct the old table for a change column
   * @param table The table to change
   * @param oldColumn The old column
   * @param newColumn The new column
   * @return The 'old' table
   *
   */
  protected Table oldTableForChangeColumn(Table table, Column oldColumn, Column newColumn) {
    return new ChangeColumn(table.getName(), oldColumn, newColumn).reverse(SchemaUtils.schema(table)).getTable(table.getName());
  }


  /**
   * Drops and recreates the triggers and supporting items for the target table.
   *
   * @param table the table for which to rebuild triggers
   * @return a collection of sql statements to execute
   */
  public Collection<String> rebuildTriggers(@SuppressWarnings("unused") Table table) {
    return SqlDialect.NO_STATEMENTS;
  }


  /**
   * Indicates whether the dialect uses NVARCHAR or VARCHAR to store string values.
   *
   * @return true if NVARCHAR is used, false is VARCHAR is used.
   */
  public boolean usesNVARCHARforStrings() {
    return false;
  }


  /**
   * Convert a {@link WindowFunction} into standards compliant SQL.
   * @param windowFunctionField The field to convert
   * @return The resulting SQL
   **/
  protected String getSqlFrom(WindowFunction windowFunctionField) {

    if (requiresOrderByForWindowFunction(windowFunctionField.getFunction()) && windowFunctionField.getOrderBys().isEmpty()) {
      throw new IllegalArgumentException("Window function " + windowFunctionField.getFunction().getType() + " requires an order by clause.");
    }

    StringBuilder statement = new StringBuilder().append(getSqlForWindowFunction(windowFunctionField.getFunction()));

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
   * Generates standards compliant SQL from the function within a window function
   * @param function The field to convert
   * @return The resulting SQL
   */
  protected String getSqlForWindowFunction(Function function) {
    return getSqlFrom(function);
  }


  /**
   * Returns the INSERT INTO statement.
   *
   * @param insertStatement he {@linkplain InsertStatement} object which can be used by the overriding methods to customize the INSERT statement.
   * @return the INSERT INTO statement.
   */
  protected String getSqlForInsertInto(@SuppressWarnings("unused") InsertStatement insertStatement) {
    return "INSERT INTO ";
  }


  /**
   * Converts the provided portable function into SQL. Each dialect will attempt to retrieve the applicable
   * function and arguments, throwing an unsupported operation exception if one is not found.
   *
   * @param portableSqlFunction the function to convert
   * @return the resulting SQL
   */
  protected abstract String getSqlFrom(PortableSqlFunction portableSqlFunction);


  /**
   * Common method used to convert portable functions, for dialects that share the same syntax.
   */
  protected String getSqlForPortableFunction(Pair<String, List<AliasedField>> functionWithArguments) {
    String functionName = functionWithArguments.getLeft();

    List<String> arguments = functionWithArguments.getRight()
        .stream()
        .map(this::getSqlFrom)
        .collect(toList());

    return functionName + "(" + Joiner.on(", ").join(arguments) + ")";
  }


  /**
   * Returns any statements needed to automatically heal the given schema.
   *
   * This healer is intended for automated database modifications not visible via the {@link Schema}.
   * For example the names of primary key indexes are not generally available via the {@link Schema},
   * and can therefore be healed via this method without disrupting the {@link Schema} contents.
   *
   * On the other hand, the names of indexes are available via the {@link Schema}, changing them
   * via this method would leave a database inconsistent with the contents of the {@link Schema},
   * and therefore those cannot be auto-healed via this method.
   *
   * See {@link SchemaAutoHealer}, another type of a database auto-healer, which is intended to heal
   * characteristics of the database visible in the {@link Schema}.
   *
   * Also note that this type of healer is dialect-specific, each dialect implements it separately.
   *
   * @param schemaResource Schema resource that can be examined.
   * @return List of statements to be run.
   */
  public List<String> getSchemaConsistencyStatements(SchemaResource schemaResource) {
    return ImmutableList.of();
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
     * True if this idTable should be create as a temporary table specific to
     * dialect
     */
    private final boolean isTemporary;


    /**
     * For testing only - the tableName might not be appropriate for your
     * dialect! The table will be a temporary table, specific to the dialect.
     *
     * @param tableName table name for the id table.
     * @return {@link IdTable}.
     */
    public static IdTable withDeterministicName(String tableName) {
      return new IdTable(tableName, true);
    }


    /**
     * Use this to create a temporary {@link IdTable} which is guaranteed to have a legal
     * name for the dialect.
     *
     * @param dialect {@link SqlDialect} that knows what temp table names are
     *          allowed.
     * @param prefix prefix for the unique name generated.
     * @return {@link IdTable}
     */
    public static IdTable withPrefix(SqlDialect dialect, String prefix) {
      return withPrefix(dialect, prefix, true);
    }


    /**
     * Use this to create a temporary or non-temporary {@link IdTable} which is
     * guaranteed to have a legal name for the dialect. The non-temporary idTables
     * are necessary to enable access from many sessions/connections in case of some
     * dialects.
     *
     * @param dialect     {@link SqlDialect} that knows what temp table names are
     *                      allowed.
     * @param prefix      prefix for the unique name generated.
     * @param isTemporary if set to true the table will be created as a temporary
     *                      table specific for the dialect.
     * @return {@link IdTable}
     */
    public static IdTable withPrefix(SqlDialect dialect, String prefix, boolean isTemporary) {
      return new IdTable(dialect.decorateTemporaryTableName(prefix + RandomStringUtils.randomAlphabetic(5)), isTemporary);
    }


    /**
     * Constructor used by tests for generating a predictable table name.
     *
     * @param tableName table name for the temporary table.
     * @param isTemporary if set to true, the table will be a temporary table specific to dialect.
     */
    private IdTable(String tableName, boolean isTemporary) {
      this.tableName = tableName;
      this.isTemporary = isTemporary;
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
      columns.add(SchemaUtils.column(ID_INCREMENTOR_TABLE_COLUMN_NAME, DataType.STRING, 132).primaryKey());
      columns.add(SchemaUtils.column(ID_INCREMENTOR_TABLE_COLUMN_VALUE, DataType.BIG_INTEGER));

      return columns;
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Table#isTemporary()
     */
    @Override
    public boolean isTemporary() {
      return isTemporary;
    }
  }
}
