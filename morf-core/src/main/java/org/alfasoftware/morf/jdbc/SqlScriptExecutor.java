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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement.ParseResult;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Executes an SQL script.
 * <p>
 * <b>WARNING:</b> This class is designed to NOT run in a transaction.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SqlScriptExecutor {
  /** Standard logger */
  private static final Log log = LogFactory.getLog(SqlScriptExecutor.class);

  /**
   * Visitor to be notified about SQL execution.
   */
  private final SqlScriptVisitor visitor;

  private final DataSource dataSource;

  private final SqlDialect sqlDialect;

  private int fetchSizeForBulkSelects;
  private int fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming;

  /**
   * Create an SQL executor with the given visitor, who will
   * be notified about events.
   *
   * @param visitor Visitor to notify about execution.
   * @param dataSource DataSource to use.
   */
  SqlScriptExecutor(SqlScriptVisitor visitor, DataSource dataSource, SqlDialect sqlDialect) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = sqlDialect;
    this.fetchSizeForBulkSelects = sqlDialect.fetchSizeForBulkSelects();
    this.fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming = sqlDialect.fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
    this.visitor = checkVisitor(visitor);
  }

  /**
   * Create an SQL executor with the given visitor, who will
   * be notified about events.
   *
   * @param visitor Visitor to notify about execution.
   * @param dataSource DataSource to use.
   * @param connectionResources The connection resources to use.
   */
  SqlScriptExecutor(SqlScriptVisitor visitor, DataSource dataSource, SqlDialect sqlDialect, ConnectionResources connectionResources) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = sqlDialect;
    this.fetchSizeForBulkSelects = connectionResources.getFetchSizeForBulkSelects() != null
        ? connectionResources.getFetchSizeForBulkSelects() : sqlDialect.fetchSizeForBulkSelects();
    this.fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming = connectionResources.getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming() != null
        ? connectionResources.getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming() : sqlDialect.fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
    this.visitor = checkVisitor(visitor);
  }

  private SqlScriptVisitor checkVisitor(SqlScriptVisitor visitor){
    if (visitor == null) {
      return new NullVisitor();
    } else {
      return visitor;
    }
  }

  /**
   * Receives notification about the execution of one or more SQL statements.
   *
   * @see SqlScriptExecutor
   * @author Copyright (c) Alfa Financial Software 2010
   */
  public static interface SqlScriptVisitor {

    /**
     * A batch of SQL is about to be executed.
     */
    public void executionStart();

    /**
     * Notify the visitor that the given SQL is about to be executed.
     *
     * @param sql SQL which is about to be executed.
     */
    public void beforeExecute(String sql);

    /**
     * Notify the visitor that the given SQL has been updated, and the
     * given number of rows were updated. This should always be preceded
     * by a call to {@link #beforeExecute(String)} with the same SQL.
     *
     * @param sql SQL which has just been executed.
     * @param numberOfRowsUpdated Number of rows updated by {@code sql}.
     */
    public void afterExecute(String sql, long numberOfRowsUpdated);

    /**
     * The batch of SQL statements has completed.
     */
    public void executionEnd();
  }


  /**
   * Performs some work using a JDBC connection derived from the injected data source.
   *
   * @param work a {@link Work} implementation.
   */
  public void doWork(Work work) {
    try {
      if (dataSource == null) {
        // Either initialise this executor with a DataSource or use the execute(Iterable<String>, Connection) method.
        throw new IllegalStateException("No data source found.");
      }
      try (Connection connection = dataSource.getConnection()) {
        autoCommitOff(connection, () -> {
          commitOrRollback(connection, () -> {
            work.execute(connection);
          });
        });
      }
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "Error with statement");
    }
  }


  private void commitOrRollback(Connection connection, SqlExceptionThrowingRunnable runnable) throws SQLException {
    try {
      runnable.run();
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } finally { //NOPMD
        throw e;
      }
    }
  }


  private void autoCommitOff(Connection connection, SqlExceptionThrowingRunnable runnable) throws SQLException {
    boolean wasAutoCommit = connection.getAutoCommit();
    if (wasAutoCommit) connection.setAutoCommit(false);
    try {
      runnable.run();
    } finally {
      if (wasAutoCommit) connection.setAutoCommit(true);
    }
  }


  private interface SqlExceptionThrowingRunnable {
    public void run() throws SQLException;
  }


  /**
   * Runs a script of SQL statements and commit.
   * The statements are executed in the order they are provided.
   *
   * @param sqlScript SQL statements to run.
   * @return The number of rows updated/affected by the statements in total.
   */
  public int execute(final Iterable<String> sqlScript) {
    final Holder<Integer> holder = new Holder<>(-1);
    doWork(new Work() {
      @Override
      public void execute(Connection connection) throws SQLException {
        holder.set(SqlScriptExecutor.this.executeAndCommit(sqlScript, connection));
      }
    });
    return holder.get();
  }


  /**
   * Runs a batch of SQL statements.
   *
   * @param sqlScript SQL statements to run.
   * @param connection Database against which to run SQL statements.
   * @return The number of rows updated/affected by the statements in total.
   */
  public int execute(Iterable<String> sqlScript, Connection connection) {
    int result = 0;
    try {
      visitor.executionStart();
      for (String sql : sqlScript) {
        result += executeInternal(sql, connection);
      }
      visitor.executionEnd();
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "Error with statement");
    }
    return result;
  }


  /**
   * Runs a batch of SQL statements.
   *
   * @param sqlScript SQL statements to run.
   * @param connection Database against which to run SQL statements.
   * @return The number of rows updated/affected by the statements in total.
   */
  public int executeAndCommit(Iterable<String> sqlScript, Connection connection) {
    int result = 0;
    try {
      visitor.executionStart();
      for (String sql : sqlScript) {
        result += executeInternal(sql, connection);
        connection.commit();
      }
      visitor.executionEnd();
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "Error with statement");
    }
    return result;
  }


  /**
   * Runs a single SQL statment.
   *
   * @param sqlStatement The single SQL statment to run
   * @param connection Database against which to run SQL statements.
   * @return The number of rows updated/affected by this statement
   */
  public int execute(String sqlStatement, Connection connection) {
    try {
      visitor.executionStart();
      int rowsUpdated = executeInternal(sqlStatement, connection);
      visitor.executionEnd();
      return rowsUpdated;
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "Error with statement");
    }
  }


  /**
   * Runs a single SQL statment.
   *
   * @param sqlStatement The single SQL statement to run
   * @return The number of rows updated/affected by this statement
   */
  public int execute(final String sqlStatement) {
    final Holder<Integer> holder = new Holder<>(-1);
    doWork(new Work() {
      @Override
      public void execute(Connection connection) throws SQLException {
        holder.set(SqlScriptExecutor.this.execute(sqlStatement, connection));
      }
    });
    return holder.get();
  }


  /**
   * Runs a single SQL statement with parameters.
   * <br>
   * <br>
   * <strong>Connection WILL NOT be committed in this method. Due to usage of
   * this method in JTA/non-JTA mode, it's the task of the caller to properly
   * commit (or not!) connection and close it</strong>
   *
   * @param sqlStatement The single SQL statement to run.
   * @param connection The connection to use.
   * @param parameterMetadata The metadata of the parameters being supplied.
   * @param parameterData The values of the parameters.
   * @return The number of rows updated/affected by this statement
   */
  public int execute(String sqlStatement, Connection connection, Iterable<SqlParameter> parameterMetadata, DataValueLookup parameterData) {
    visitor.beforeExecute(sqlStatement);
    int numberOfRowsUpdated = 0;
    try {
      try {
        try (NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parseSql(sqlStatement, sqlDialect).createFor(connection)) {
          sqlDialect.prepareStatementParameters(preparedStatement, parameterMetadata, parameterData);
          numberOfRowsUpdated = preparedStatement.executeUpdate();
        }
      } catch (SQLException e) {
        throw reclassifiedRuntimeException(e, "Error executing SQL [" + sqlStatement + "]");
      }
      return numberOfRowsUpdated;
    } finally {
      visitor.afterExecute(sqlStatement, numberOfRowsUpdated);
    }
  }


  /**
   * @param sqlStatement The SQL statement to execute
   * @param parameterMetadata The metadata of the parameters being supplied.
   * @param parameterData The values of the parameters.
   * @return The number of rows updated/affected by this statement
   * @see #execute(String, Connection, Iterable, DataValueLookup)
   */
  public int execute(final String sqlStatement, final Iterable<SqlParameter> parameterMetadata, final DataValueLookup parameterData) {
    final Holder<Integer> holder = new Holder<>();

    doWork(new Work() {
      @Override
      public void execute(Connection connection) throws SQLException {
        holder.set(SqlScriptExecutor.this.execute(sqlStatement, connection, parameterMetadata, parameterData));
      }
    });

    return holder.get();
  }


  /**
   * @param sql the sql statement to run.
   * @param connection Database against which to run SQL statements.
   * @return The number of rows updated/affected by this statement
   * @throws SQLException throws an exception for statement errors.
   */
  private int executeInternal(String sql, Connection connection) throws SQLException {
    visitor.beforeExecute(sql);
    int numberOfRowsUpdated = 0;
    try {
      // Skip comments
      if (sqlDialect.sqlIsComment(sql)) {
        return 0;
      }

      try (Statement statement = connection.createStatement()) {
        if (log.isDebugEnabled())
          log.debug("Executing SQL [" + sql + "]");

        boolean result = statement.execute(sql);
        if (!result) {
          numberOfRowsUpdated = statement.getUpdateCount();
        }

        if (log.isDebugEnabled())
          log.debug("SQL resulted in [" + numberOfRowsUpdated + "] rows updated");

      } catch (Exception e) {
        throw reclassifiedRuntimeException(e, "Error executing SQL [" + sql + "]");
      }

      return numberOfRowsUpdated;
    } finally {
      visitor.afterExecute(sql, numberOfRowsUpdated);
    }
  }


  /**
   * Reclassify an exception if it is dialect specific and wrap in a runtime exception.
   */
  private RuntimeException reclassifiedRuntimeException(Exception e, String message) {
    Exception reclassifiedException = sqlDialect.getDatabaseType().reclassifyException(e);
    return reclassifiedException instanceof SQLException ? new RuntimeSqlException(message, (SQLException) reclassifiedException) :
                                       new RuntimeException(message, reclassifiedException);
  }


  /**
   * Runs a select statement, with optional parameters, allowing its
   * {@link ResultSet} to be processed by the supplied implementation of
   * {@link ResultSetProcessor}. {@link ResultSetProcessor#process(ResultSet)}
   * can return a value of any type, which will form the return value of this
   * method.
   *
   * @param query The select statement to run.
   * @return a {@link QueryBuilder}, to continue specifying parameters.
   */
  public QueryBuilder executeQuery(SelectStatement query) {
    return new QueryBuilderImpl(query);
  }


  /**
   * Runs a select statement, with optional parameters, allowing its
   * {@link ResultSet} to be processed by the supplied implementation of
   * {@link ResultSetProcessor}. {@link ResultSetProcessor#process(ResultSet)}
   * can return a value of any type, which will form the return value of this
   * method.
   *
   * @param query The select statement to run.
   * @return a {@link QueryBuilder}, to continue specifying parameters.
   */
  public QueryBuilder executeQuery(String query) {
    return new QueryBuilderImpl(query);
  }


  /**
   * <p>
   * Runs a SQL query, allowing its {@link ResultSet} to be processed by the
   * supplied implementation of {@link ResultSetProcessor}.
   * {@link ResultSetProcessor#process(ResultSet)} can return a value of any
   * type, which will form the return value of this method.
   * </p>
   * <p>
   * A new connection is created and closed to run the query.
   * </p>
   * <p>
   * Usage example:
   * </p>
   * <blockquote>
   *
   * <pre>
   * boolean found = executor.executeQuery(sqlToCheckIfRecordExists, new ResultSetProcessor&lt;Boolean&gt;() {
   *   &#064;Override
   *   public Boolean process(ResultSet resultSet) throws SQLException {
   *     return resultSet.next();
   *   }
   * });
   * if (!found) {
   *   insertRecord();
   * }
   * </pre>
   *
   * </blockquote>
   *
   * @param sql the sql statement to run.
   * @param processor the code to be run to process the {@link ResultSet}.
   * @param <T> the type of results processed
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  public <T> T executeQuery(String sql, ResultSetProcessor<T> processor) {
    return executeQuery(sql).processWith(processor);
  }


  /**
   * <p>Runs a SQL query, allowing its {@link ResultSet} to be
   * processed by the supplied implementation of {@link ResultSetProcessor}.
   * {@link ResultSetProcessor#process(ResultSet)} can return a value
   * of any type, which will form the return value of this method.</p>
   *
   * <p>Usage example:</p>
   *
   * <blockquote><pre>
   *
   *   {@code boolean found = executor.executeQuery(sqlToCheckIfRecordExists, new ResultSetProcessor<Boolean>}() {
   *   &#064;Override
   *   public Boolean process(ResultSet resultSet) throws SQLException {
   *     return resultSet.next();
   *   }
   * });
   * if (!found) {
   *   insertRecord();
   * }
   *
   * </pre></blockquote>

   * @param sql the sql statement to run.
   * @param connection the connection to use.
   * @param processor the code to be run to process the {@link ResultSet}.
   * @param <T> the type of results processed
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  public <T> T executeQuery(String sql, Connection connection, ResultSetProcessor<T> processor) {
    return executeQuery(sql).withConnection(connection).processWith(processor);
  }


  /**
   * Runs a select statement (with parameters), allowing its {@link ResultSet}
   * to be processed by the supplied implementation of
   * {@link ResultSetProcessor}. {@link ResultSetProcessor#process(ResultSet)}
   * can return a value of any type, which will form the return value of this
   * method.
   *
   * @param query the select statement to run.
   * @param parameterMetadata the metadata describing the parameters.
   * @param parameterData the values to insert.
   * @param connection the connection to use.
   * @param resultSetProcessor the code to be run to process the
   *          {@link ResultSet}.
   * @param <T> the type of results processed
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  public <T> T executeQuery(SelectStatement query, Iterable<SqlParameter> parameterMetadata,
      DataValueLookup parameterData, Connection connection, ResultSetProcessor<T> resultSetProcessor) {
    return executeQuery(query).withParameterMetadata(parameterMetadata).withParameterData(parameterData).withConnection(connection)
        .processWith(resultSetProcessor);
  }


  /**
   * Runs a select statement (with parameters), allowing its {@link ResultSet}
   * to be processed by the supplied implementation of
   * {@link ResultSetProcessor}. {@link ResultSetProcessor#process(ResultSet)}
   * can return a value of any type, which will form the return value of this
   * method.
   *
   * @param sql the select statement to run.
   * @param parameterMetadata the metadata describing the parameters.
   * @param parameterData the values to insert.
   * @param connection the connection to use.
   * @param resultSetProcessor the code to be run to process the
   *          {@link ResultSet}.#
   * @param maxRows The maximum number of rows to be returned. Will inform the
   *          JDBC driver to tell the server not to return any more rows than
   *          this.
   * @param queryTimeout the timeout in <b>seconds</b> after which the query
   *          will time out on the database side
   * @param standalone whether the query being executed is stand-alone.
   * @param <T> the type of results processed
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  private <T> T executeQuery(String sql, Iterable<SqlParameter> parameterMetadata, DataValueLookup parameterData,
      Connection connection, ResultSetProcessor<T> resultSetProcessor, Optional<Integer> maxRows, Optional<Integer> queryTimeout,
      boolean standalone) {
    try {
      ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, sqlDialect);
      try (NamedParameterPreparedStatement preparedStatement = standalone ? parseResult.createForQueryOn(connection) : parseResult.createFor(connection)) {

        if (standalone) {
          preparedStatement.setFetchSize(fetchSizeForBulkSelects);
          log.debug("Executing query [" + sql + "] with standalone = [" + standalone + "] and fetch size: [" + fetchSizeForBulkSelects +"].");
        } else {
          preparedStatement.setFetchSize(fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming);
          log.debug("Executing query [" + sql + "] with standalone = [" + standalone + "] and fetch size: [" + fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming +"].");
        }
        return executeQuery(preparedStatement, parameterMetadata, parameterData, resultSetProcessor, maxRows, queryTimeout);
      }
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "SQL exception when executing query");
    }
  }


  /**
   * Runs a {@link NamedParameterPreparedStatement} (with parameters), allowing
   * its {@link ResultSet} to be processed by the supplied implementation of
   * {@link ResultSetProcessor}. {@link ResultSetProcessor#process(ResultSet)}
   * can return a value of any type, which will form the return value of this
   * method.
   *
   * @param preparedStatement Prepared statement to run.
   * @param parameterMetadata the metadata describing the parameters.
   * @param parameterData the values to insert.
   * @param connection the connection to use.
   * @param processor the code to be run to process the {@link ResultSet}.
   * @param maxRows The maximum number of rows to be returned. Will inform the
   *          JDBC driver to tell the server not to return any more rows than
   *          this.
   * @param queryTimeout the timeout in <b>seconds</b> after which the query
   *          will time out on the database side
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  private <T> T executeQuery(NamedParameterPreparedStatement preparedStatement, Iterable<SqlParameter> parameterMetadata,
      DataValueLookup parameterData, ResultSetProcessor<T> processor, Optional<Integer> maxRows, Optional<Integer> queryTimeout) {
    if (sqlDialect == null) {
      throw new IllegalStateException("Must construct with dialect");
    }
    try {
      sqlDialect.prepareStatementParameters(preparedStatement, parameterMetadata, parameterData);
      if (maxRows.isPresent()) {
        preparedStatement.setMaxRows(maxRows.get());
      }
      if (queryTimeout.isPresent()) {
        preparedStatement.setQueryTimeout(queryTimeout.get());
      }
      ResultSet resultSet = preparedStatement.executeQuery();
      try {
        T result = processor.process(resultSet);
        visitor.afterExecute(preparedStatement.toString(), 0);
        return result;
      } finally {
        resultSet.close();
      }

    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "SQL exception when executing query: [" + preparedStatement + "]");
    }
  }


  /**
   * Runs the specified SQL statement (which should contain parameters), repeatedly for
   * each record, mapping the contents of the records into the statement parameters in
   * their defined order.  Use to insert, merge or update a large batch of records
   * efficiently.
   *
   * @param sqlStatement the SQL statement.
   * @param parameterMetadata the metadata describing the parameters.
   * @param parameterData the values to insert.
   * @param connection the JDBC connection to use.
   * @param explicitCommit Determine if an explicit commit should be invoked after executing the supplied batch
   * @param statementsPerFlush the number of statements to execute between JDBC batch flushes. Higher numbers have higher memory cost
   *   but reduce the number of I/O round-trips to the database.
   */
  public void executeStatementBatch(String sqlStatement, Iterable<SqlParameter> parameterMetadata, Iterable<? extends DataValueLookup> parameterData, Connection connection, boolean explicitCommit, int statementsPerFlush) {
    try {
      try (NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parseSql(sqlStatement, sqlDialect).createFor(connection)) {
        executeStatementBatch(preparedStatement, parameterMetadata, parameterData, connection, explicitCommit, statementsPerFlush);
      } finally {
        if (explicitCommit) {
          connection.commit();
        }
      }
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "SQL exception executing batch");
    }
  }


  private void executeStatementBatch(NamedParameterPreparedStatement preparedStatement, Iterable<SqlParameter> parameterMetadata, Iterable<? extends DataValueLookup> parameterData, Connection connection, boolean explicitCommit, int statementsPerFlush) {
    if (sqlDialect == null) {
      throw new IllegalStateException("Must construct with dialect");
    }

    try {
      long count = 0;
      for (DataValueLookup data : parameterData) {

        sqlDialect.prepareStatementParameters(preparedStatement, parameterMetadata, data);

        // Use batching or just execute directly
        if (sqlDialect.useInsertBatching()) {
          preparedStatement.addBatch();
          count++;
          if (count % statementsPerFlush == 0) {
            try {
              preparedStatement.executeBatch();
              preparedStatement.clearBatch();
            } catch (SQLException e) {
              throw reclassifiedRuntimeException(e, "Error executing batch");
            }
            // commit each batch for performance reasons
            if (explicitCommit) {
              connection.commit();
            }
          }
        } else {
          try {
            preparedStatement.executeUpdate();
          } catch (SQLException e) {
            List<String> inserts = new ArrayList<>();
            for (SqlParameter parameter : parameterMetadata) {
              inserts.add(data.getString(parameter.getImpliedName()));
            }
            throw reclassifiedRuntimeException(e, "Error executing batch with values " + inserts);
          }
        }
      }

      // Clear up any remaining batch statements if in batch mode and
      // have un-executed statements
      if (sqlDialect.useInsertBatching() && count % statementsPerFlush > 0) {
        preparedStatement.executeBatch();
      }
    } catch (SQLException e) {
      throw reclassifiedRuntimeException(e, "SQLException executing batch. Prepared Statements: [" + preparedStatement + "]");
    }
  }


  /**
   * Specifies an object which can process {@link ResultSet}s.  Passed to
   * {@link SqlScriptExecutor#executeQuery(String, ResultSetProcessor)},
   * usually as an anonymous inner class. See
   * {@link SqlScriptExecutor#executeQuery(String, ResultSetProcessor)}
   * for usage examples.
   *
   * @author Copyright (c) Alfa Financial Software 2013
   */
  public interface ResultSetProcessor<T> {

    /**
     * Process a {@link ResultSet}, returning a result to be returned by a call
     * to {@link SqlScriptExecutor#executeQuery(String, ResultSetProcessor)}.
     *
     * @param resultSet The result set.
     * @return A value, which will be the return value of {@link SqlScriptExecutor#executeQuery(String, ResultSetProcessor)}
     * @throws SQLException when an error occurs when processing the supplied {@link ResultSet}
     */
    public T process(ResultSet resultSet) throws SQLException;
  }


  /**
   * Null (No-op) visitor.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  private static final class NullVisitor implements SqlScriptVisitor {

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionStart()
     */
    @Override
    public void executionStart() {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#beforeExecute(java.lang.String)
     */
    @Override
    public void beforeExecute(String sql) {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#afterExecute(java.lang.String,
     *      long)
     */
    @Override
    public void afterExecute(String sql, long numberOfRowsUpdated) {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionEnd()
     */
    @Override
    public void executionEnd() {
      // Defaults to no-op
    }
  }


  /**
   * Contract for performing a discrete piece of JDBC work.
   *
   * Based on the namesake in org.hibernate.jdbc
   *
   */
  public interface Work {
    /**
     * Execute the discrete work encapsulated by this work instance using the supplied connection.
     *
     * @param connection The connection on which to perform the work.
     * @throws SQLException Thrown during execution of the underlying JDBC interaction.
     */
    public void execute(Connection connection) throws SQLException;
  }


  /**
   * Execute query builder.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  public interface QueryBuilder {

    /**
     * Specifies optional parameter metadata for the query.
     *
     * @param parameterMetadata The parameter metadata.
     * @return this
     */
    QueryBuilder withParameterMetadata(Iterable<SqlParameter> parameterMetadata);


    /**
     * Specifies the values for the parameters. Required only if parameter
     * metadata has been specified in
     * {@link #withParameterData(DataValueLookup)}.
     *
     * @param parameterData The parameter data.
     * @return this
     */
    QueryBuilder withParameterData(DataValueLookup parameterData);


    /**
     * Specifies a connection to use. If not supplied, a connection will be
     * requested from the {@link DataSource}.
     *
     * @param connection The connection.
     * @return this
     */
    QueryBuilder withConnection(Connection connection);


    /**
     * Specifies the maximum number of rows to return. Optional.
     *
     * @param maxRows The maximum number of rows to return.
     * @return this
     */
    QueryBuilder withMaxRows(int maxRows);


    /**
     * Specifies the maximum number of rows to return. Optional.
     *
     * @param maxRows The maximum number of rows to return.
     * @return this
     */
    QueryBuilder withMaxRows(Optional<Integer> maxRows);


    /**
     * Specifies the time in <b>seconds</b> after which the query will time out.
     *
     * @param queryTimeout time out length in seconds
     * @return this
     */
    QueryBuilder withQueryTimeout(int queryTimeout);


    /**
     * Specifies that the query being built is stand-alone. This is used to
     * indicate that the connection used by the query won't be used while the
     * query results are being read.
     *
     * @return this
     */
    QueryBuilder standalone();


    /**
     * Executes the query, passing the results to the supplied result set
     * processor, and returns the result of the processor.
     *
     * @param resultSetProcessor The result set processor
     * @param <T> the type of results processed
     * @return The result of the processor.
     */
    <T> T processWith(ResultSetProcessor<T> resultSetProcessor);
  }


  /**
   * Implementation of {@link QueryBuilder}.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  private final class QueryBuilderImpl implements QueryBuilder {

    private final String query;
    private Iterable<SqlParameter> parameterMetadata = Collections.emptyList();
    private DataValueLookup parameterData = DataSetUtils.record();
    private Connection connection;
    private Optional<Integer> maxRows = Optional.empty();
    private Optional<Integer> queryTimeout = Optional.empty();
    private boolean standalone;


    QueryBuilderImpl(String query) {
      this.query = query;
    }

    QueryBuilderImpl(SelectStatement query) {
      this.query = sqlDialect.convertStatementToSQL(query);
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withParameterMetadata(java.lang.Iterable)
     */
    @Override
    public QueryBuilder withParameterMetadata(Iterable<SqlParameter> parameterMetadata) {
      this.parameterMetadata = parameterMetadata;
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withParameterData(org.alfasoftware.morf.metadata.DataValueLookup)
     */
    @Override
    public QueryBuilder withParameterData(DataValueLookup parameterData) {
      this.parameterData = parameterData;
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withConnection(java.sql.Connection)
     */
    @Override
    public QueryBuilder withConnection(Connection connection) {
      this.connection = connection;
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withMaxRows(int)
     */
    @Override
    public QueryBuilder withMaxRows(int maxRows) {
      this.maxRows = Optional.of(maxRows);
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withMaxRows(java.util.Optional)
     */
    @Override
    public QueryBuilder withMaxRows(Optional<Integer> maxRows) {
      this.maxRows = maxRows;
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withQueryTimeout(int)
     */
    @Override
    public QueryBuilder withQueryTimeout(int queryTimeout) {
      this.queryTimeout  = Optional.of(queryTimeout);
      return this;
    }

    @Override
    public QueryBuilder standalone() {
      this.standalone = true;
      return this;
    }

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#processWith(org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor)
     */
    @Override
    public <T> T processWith(final ResultSetProcessor<T> resultSetProcessor) {
      try {
        final Holder<T> holder = new Holder<>();

        Work work = new Work() {
          @Override
          public void execute(Connection innerConnection) throws SQLException {
            holder.set(executeQuery(query, parameterMetadata, parameterData, innerConnection, resultSetProcessor, maxRows, queryTimeout, standalone));
          }
        };

        if (connection == null) {
          // Get a new connection, and use that...
          doWork(work);
        } else {
          // Get out own connection, and use that...
          work.execute(connection);
        }

        return holder.get();
      } catch (SQLException e) {
        throw reclassifiedRuntimeException(e, "Error with statement");
      }
    }
  }


  /**
   * A simple mutable "holder" for arbitrary objects.
   *
   * @author Copyright (c) Alfa Financial Software 2012
   */
  private static final class Holder<T> {

    private T value;

    /**
     * Constructor.
     */
    Holder() {
      this(null);
    }


    /**
     * Constructor.
     * @param value The value with which to initialise this instance.
     */
    Holder(T value) {
      this.value = value;
    }


    /**
     * @return the value
     */
    T get() {
      return value;
    }


    /**
     * @param value the value to set
     */
    void set(T value) {
      this.value = value;
    }
  }
}