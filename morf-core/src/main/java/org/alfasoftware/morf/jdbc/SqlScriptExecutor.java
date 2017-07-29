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

import javax.sql.DataSource;

import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Optional;

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
   * Size of batches used within
   * {@link #executeStatementBatch(String, Iterable, Iterable, Connection, boolean)}
   * to bundle up insert statements.
   */
  private static final int BATCH_SIZE = 1000;

  /**
   * Visitor to be notified about SQL execution.
   */
  private final SqlScriptVisitor visitor;

  private final DataSource dataSource;

  private final SqlDialect sqlDialect;

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
    if (visitor == null) {
      this.visitor = new NullVisitor();
    } else {
      this.visitor = visitor;
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

      Connection connection = dataSource.getConnection();
      boolean wasAutoCommit = connection.getAutoCommit();
      try {
        if (wasAutoCommit) connection.setAutoCommit(false);

        work.execute(connection);

        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      } finally {
        if (wasAutoCommit) connection.setAutoCommit(true);
        connection.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException(e);
    }
  }


  /**
   * Runs a script of SQL statements and commit.
   *
   * The statements are executed in the order they are provided
   *
   * @param sqlScript SQL statements to run.
   */
  public void execute(final Iterable<String> sqlScript) {
    doWork(new Work() {
      @Override
      public void execute(Connection connection) throws SQLException {
        SqlScriptExecutor.this.executeAndCommit(sqlScript, connection);
      }
    });
  }


  /**
   * Runs a batch of SQL statements.
   *
   * @param sqlScript SQL statements to run.
   * @param connection Database against which to run SQL statements.
   */
  public void execute(Iterable<String> sqlScript, Connection connection) {
    try {
      visitor.executionStart();
      for (String sql : sqlScript) {
        executeInternal(sql, connection);
      }
      visitor.executionEnd();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error with statement", e);
    }
  }


  /**
   * Runs a batch of SQL statements.
   *
   * @param sqlScript SQL statements to run.
   * @param connection Database against which to run SQL statements.
   */
  public void executeAndCommit(Iterable<String> sqlScript, Connection connection) {
    try {
      visitor.executionStart();
      for (String sql : sqlScript) {
        executeInternal(sql, connection);
        connection.commit();
      }
      visitor.executionEnd();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error with statement", e);
    }
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
      throw new RuntimeSqlException("Error with statement", e);
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
        NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parse(sqlStatement).createFor(
          connection);
        try {
          prepareParameters(preparedStatement, parameterMetadata, parameterData);
          numberOfRowsUpdated = preparedStatement.executeUpdate();
        } finally {
          preparedStatement.close();
        }
      } catch (SQLException e) {
        throw new RuntimeSqlException(e);
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

      Statement statement = connection.createStatement();
      try {
        if (log.isDebugEnabled())
          log.debug("Executing SQL [" + sql + "]");

        boolean result = statement.execute(sql);
        if (!result) {
          numberOfRowsUpdated = statement.getUpdateCount();
        }

        if (log.isDebugEnabled())
          log.debug("SQL resulted in [" + numberOfRowsUpdated + "] rows updated");

      } catch (SQLException e) {
        throw new RuntimeSqlException("Error executing SQL [" + sql + "]", e);
      } catch (Exception e) {
        throw new RuntimeException("Error executing SQL [" + sql + "]", e);
      } finally {
        statement.close();
      }

      return numberOfRowsUpdated;
    } finally {
      visitor.afterExecute(sql, numberOfRowsUpdated);
    }
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
   * @param <T> the type of results processed
   * @return the result from {@link ResultSetProcessor#process(ResultSet)}.
   */
  private <T> T executeQuery(String sql, Iterable<SqlParameter> parameterMetadata,
      DataValueLookup parameterData, Connection connection, ResultSetProcessor<T> resultSetProcessor, Optional<Integer> maxRows, Optional<Integer> queryTimeout) {
    try {
      NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parse(sql).createFor(connection);
      try {
        return executeQuery(preparedStatement, parameterMetadata, parameterData, resultSetProcessor, maxRows, queryTimeout);
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("SQL exception when executing query", e);
    }
  }


  private <T> T executeQuery(NamedParameterPreparedStatement preparedStatement, Iterable<SqlParameter> parameterMetadata,
      DataValueLookup parameterData, ResultSetProcessor<T> processor, Optional<Integer> maxRows, Optional<Integer> queryTimeout) {
    if (sqlDialect == null) {
      throw new IllegalStateException("Must construct with dialect");
    }
    try {
      prepareParameters(preparedStatement, parameterMetadata, parameterData);
      if (maxRows.isPresent()) {
        preparedStatement.setMaxRows(maxRows.get());
      }
      if(queryTimeout.isPresent()) {
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
      throw new RuntimeSqlException("SQL exception when executing query: [" + preparedStatement + "]", e);
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
   */
  public void executeStatementBatch(String sqlStatement, Iterable<SqlParameter> parameterMetadata, Iterable<? extends DataValueLookup> parameterData, Connection connection, boolean explicitCommit) {
    try {
      NamedParameterPreparedStatement preparedStatement = NamedParameterPreparedStatement.parse(sqlStatement).createFor(connection);
      try {
        executeStatementBatch(preparedStatement, parameterMetadata, parameterData, connection, explicitCommit);
      } finally {
        if (explicitCommit) {
          connection.commit();
        }
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("SQL exception executing batch", e);
    }
  }


  /**
   * Runs the specified prepared statement (which should contain parameters), repeatedly for
   * each record, mapping the contents of the records into the statement parameters in
   * their defined order.  Use to insert, merge or update a large batch of records
   * efficiently.
   *
   * @param preparedStatement the prepared statement.
   * @param parameterMetadata the metadata describing the parameters.
   * @param parameterData the values to insert.
   * @param connection the JDBC connection to use.
   * @param explicitCommit Determine if an explicit commit should be invoked after executing the supplied batch
   */
  public void executeStatementBatch(NamedParameterPreparedStatement preparedStatement, Iterable<SqlParameter> parameterMetadata, Iterable<? extends DataValueLookup> parameterData, Connection connection, boolean explicitCommit) {
    if (sqlDialect == null) {
      throw new IllegalStateException("Must construct with dialect");
    }

    try {
      long count = 0;
      for (DataValueLookup data : parameterData) {

        prepareParameters(preparedStatement, parameterMetadata, data);

        // Use batching or just execute directly
        if (sqlDialect.useInsertBatching()) {
          preparedStatement.addBatch();
          count++;
          if (count % BATCH_SIZE == 0) {
            try {
              preparedStatement.executeBatch();
              preparedStatement.clearBatch();
            } catch (SQLException e) {
              throw new RuntimeSqlException("Error executing batch", e);
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
              inserts.add(data.getValue(parameter.getImpliedName()));
            }
            throw new RuntimeSqlException("Error executing batch with values " + inserts, e);
          }
        }
      }

      // Clear up any remaining batch statements if in batch mode and
      // have un-executed statements
      if (sqlDialect.useInsertBatching() && count % BATCH_SIZE > 0) {
        preparedStatement.executeBatch();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("SQLException executing batch. Prepared Statements: [" + preparedStatement + "]", e);
    }
  }


  /**
   * Prepare the statement parameters by parsing them into the right type and
   * assigning to preparedStatement
   *
   * @param preparedStatement
   * @param parameterMetadata
   * @param data
   */
  private void prepareParameters(NamedParameterPreparedStatement preparedStatement, Iterable<SqlParameter> parameterMetadata,
      DataValueLookup data) {
    for (SqlParameter parameter : parameterMetadata) {
      String value = null;
      try {
        value = data.getValue(parameter.getImpliedName());
        sqlDialect.prepareStatementParameter(preparedStatement, parameter, value);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to parse value [%s] for column [%s]", value, parameter.getImpliedName()), e);
      }
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
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#beforeExecute(java.lang.String)
     */
    @Override
    public void beforeExecute(String sql) {
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#afterExecute(java.lang.String,
     *      long)
     */
    @Override
    public void afterExecute(String sql, long numberOfRowsUpdated) {
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionEnd()
     */
    @Override
    public void executionEnd() {
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
    private Optional<Integer> maxRows = Optional.absent();
    private Optional<Integer> queryTimeout = Optional.absent();


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
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.QueryBuilder#withMaxRows(com.google.common.base.Optional)
     */
    @Override
    public QueryBuilder withMaxRows(Optional<Integer> maxRows) {
      this.maxRows = maxRows;
      return this;
    }


    @Override
    public QueryBuilder withQueryTimeout(int queryTimeout) {
      this.queryTimeout  = Optional.of(queryTimeout);
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
            holder.set(executeQuery(query, parameterMetadata, parameterData, innerConnection, resultSetProcessor, maxRows, queryTimeout));
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
        throw new RuntimeSqlException("Error with statement", e);
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