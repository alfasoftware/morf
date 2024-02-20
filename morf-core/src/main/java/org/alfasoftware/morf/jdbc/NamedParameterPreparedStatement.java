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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.alfasoftware.morf.sql.SqlTokenizer;
import org.alfasoftware.morf.sql.element.SqlParameter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

/**
 * A wrapped around {@link PreparedStatement} which allows for named parameters.  Parser is the
 * one published by Adam Crume, modified to play nicely with our existing database code.
 *
 * @see <a href="http://www.javaworld.com/article/2077706/core-java/named-parameters-for-preparedstatement.html">
 *     Named parameters for prepared statements</a>
 *
 */
public class NamedParameterPreparedStatement implements AutoCloseable {

  /** The statement this object is wrapping. */
  private final PreparedStatement statement;

  /** Maps parameter names to the list of index positions at which this parameter appears. */
  private final Map<String, Collection<Integer>> indexMap;

  /** The statement - for logging and exceptions. */
  private final ParseResult sql;


  /**
   * Parses the SQL string containing named parameters in such a form that
   * can be cached, so that prepared statements using the parsed result
   * can be created rapidly.
   *
   * @param sql the SQL
   * @param sqlDialect Dialect of the SQL.
   * @return the parsed result
   */
  public static ParseResult parseSql(String sql, SqlDialect sqlDialect) {
    return new ParseResult(sql, sqlDialect);
  }


  /**
   * @deprecated Use the {@link #parseSql(String, SqlDialect)} method.
   * @param sql the SQL
   * @return the parsed result
   */
  @Deprecated
  public static ParseResult parse(String sql) {
    return new ParseResult(sql, null);
  }


  /**
   * Creates a NamedParameterStatement. Wraps a call to
   * {@link Connection#prepareStatement(java.lang.String) prepareStatement}.
   *
   * @param connection the database connection
   * @param query the parameterized query
   * @param indexMap
   * @param queryOnly Set up the statement for bulk queries.
   * @throws SQLException if the statement could not be created
   */
  NamedParameterPreparedStatement(Connection connection, String query, Map<String, Collection<Integer>> indexMap, boolean queryOnly, ParseResult sql) throws SQLException {
    this.indexMap = indexMap;
    this.sql = sql;
    if (queryOnly) {
      this.statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    } else {
      this.statement = connection.prepareStatement(query);
    }
  }


  /**
   * @see PreparedStatement#execute()
   * <P>
   * Executes the SQL statement in this <code>PreparedStatement</code> object,
   * which may be any kind of SQL statement.
   * Some prepared statements return multiple results; the <code>execute</code>
   * method handles these complex statements as well as the simpler
   * form of statements handled by the methods <code>executeQuery</code>
   * and <code>executeUpdate</code>.
   * </P><P>
   * The <code>execute</code> method returns a <code>boolean</code> to
   * indicate the form of the first result.  You must call either the method
   * <code>getResultSet</code> or <code>getUpdateCount</code>
   * to retrieve the result; you must call <code>getMoreResults</code> to
   * move to any subsequent result(s).</P>
   *
   * @return <code>true</code> if the first result is a <code>ResultSet</code>
   *         object; <code>false</code> if the first result is an update
   *         count or there is no result
   * @exception SQLException if a database access error occurs;
   * this method is called on a closed <code>PreparedStatement</code>
   * or an argument is supplied to this method
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   */
  public boolean execute() throws SQLException {
    return statement.execute();
  }


  /**
   * @see PreparedStatement#executeQuery()
   * @return a <code>ResultSet</code> object that contains the data produced by the
   *         query; never <code>null</code>
   * @exception SQLException if a database access error occurs;
   * this method is called on a closed  <code>PreparedStatement</code> or the SQL
   *            statement does not return a <code>ResultSet</code> object
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   */
  public ResultSet executeQuery() throws SQLException {
    this.statement.setFetchDirection(ResultSet.FETCH_FORWARD);
    return statement.executeQuery();
  }


  /**
   * @see PreparedStatement#executeUpdate()
   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
   *         or (2) 0 for SQL statements that return nothing
   * @exception SQLException if a database access error occurs;
   * this method is called on a closed  <code>PreparedStatement</code>
   * or the SQL statement returns a <code>ResultSet</code> object
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   */
  public int executeUpdate() throws SQLException {
    return statement.executeUpdate();
  }


  /**
   * @see PreparedStatement#close()
   * @exception SQLException if a database access error occurs
   */
  @Override
  public void close() throws SQLException {
    statement.close();
  }


  /**
   * @see PreparedStatement#addBatch()
   * @exception SQLException if a database access error occurs,
   * this method is called on a closed <code>Statement</code> or the
   * driver does not support batch statements.
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   */
  public void addBatch() throws SQLException {
    statement.addBatch();
  }


  /**
   * @see PreparedStatement#executeBatch()
   * @return an array of update counts containing one element for each
   * command in the batch.  The elements of the array are ordered according
   * to the order in which commands were added to the batch.
   * @exception SQLException if a database access error occurs,
   * this method is called on a closed <code>Statement</code> or the
   * driver does not support batch statements.
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   */
  public int[] executeBatch() throws SQLException {
    return statement.executeBatch();
  }


  /**
   * @see PreparedStatement#clearBatch()
   * @exception SQLException if a database access error occurs,
   *  this method is called on a closed <code>Statement</code> or the
   * driver does not support batch updates
   */
  public void clearBatch() throws SQLException {
    statement.clearBatch();
  }


  /**
   * @param rows the number of rows to fetch
   * @see PreparedStatement#setFetchSize(int)
   * @exception SQLException if a database access error occurs,
   * this method is called on a closed <code>Statement</code> or the
   *        condition {@code rows >= 0} is not satisfied.
   */
  public void setFetchSize(int rows) throws SQLException {
    statement.setFetchSize(rows);
  }


  /**
   * Allows arbitrary code to be executed for each index position at which the named parameter can be found.
   */
  private void forEachOccurrenceOfParameter(SqlParameter parameter, Operation operation) throws SQLException {
    Collection<Integer> indexes = indexMap.get(parameter.getImpliedName());
    if (indexes == null) {
      throw new IllegalArgumentException("Parameter not found: " + parameter.getImpliedName());
    }
    for (int i : indexes) {
      operation.apply(i);
    }
  }


  /**
   * Closure for use by callers of forEachOccurrenceOfParameter.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  private interface Operation {
    public void apply(int parameterIndex) throws SQLException;
  }


  /**
   * Sets the value of a named boolean parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setBoolean(SqlParameter parameter, final boolean value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setBoolean(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named object parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setObject(SqlParameter parameter, final Object value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setObject(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named date parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setDate(SqlParameter parameter, final Date value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setDate(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named big decimal parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setBigDecimal(SqlParameter parameter, final BigDecimal value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setBigDecimal(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named string parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setString(SqlParameter parameter, final String value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        // TODO: dialect nullability is deprecated, and should be ousted asap
        if (sql.dialect != null && sql.dialect.usesNVARCHARforStrings()) {
          statement.setNString(parameterIndex, value);
        }
        else {
          statement.setString(parameterIndex, value);
        }
      }
    });
    return this;
  }


  /**
   * Sets the value of a named integer parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setInt(SqlParameter parameter, final int value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setInt(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named long parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setLong(SqlParameter parameter, final long value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setLong(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named binary stream parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setBinaryStream(SqlParameter parameter, final InputStream value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        statement.setBinaryStream(parameterIndex, value);
      }
    });
    return this;
  }


  /**
   * Sets the value of a named blob parameter.
   *
   * @param parameter the parameter metadata.
   * @param value the parameter value.
   * @return this, for method chaining
   * @exception SQLException if an error occurs when setting the parameter
   */
  public NamedParameterPreparedStatement setBlob(SqlParameter parameter, final byte[] value) throws SQLException {
    forEachOccurrenceOfParameter(parameter, new Operation() {
      @Override
      public void apply(int parameterIndex) throws SQLException {
        Blob blob = statement.getConnection().createBlob();
        int written = blob.setBytes(1 /* odd position thing */, value);
        if (written != value.length) throw new IllegalStateException("Failed to write all bytes to BLOB (written = " + written + ", actual = " + value.length + ")");
        statement.setBlob(parameterIndex, blob);
      }
    });
    return this;
  }


  /**
   * Sets the limit for the maximum number of rows that any
   * <code>ResultSet</code> object  generated by this <code>Statement</code>
   * object can contain to the given number.
   * If the limit is exceeded, the excess
   * rows are silently dropped.
   *
   * @param maxRows the new max rows limit; zero means there is no limit
   * @exception SQLException if a database access error occurs,
   * this method is called on a closed <code>Statement</code>
   *            or the condition maxRows &gt;= 0 is not satisfied
   * @see Statement#setMaxRows(int)
   */
  public void setMaxRows(Integer maxRows) throws SQLException {
    statement.setMaxRows(maxRows);
  }


  /**
   * Sets the timeout in <b>seconds</b> after which the query will time out on
   * database side. In such case JDBC driver will throw an exception which is
   * specific to database implementation but is likely to extend
   * {@link SQLTimeoutException}.
   *
   * @param queryTimeout timeout in <b>seconds</b>
   * @exception SQLException if an error occurs when setting the timeout
   */
  public void setQueryTimeout(Integer queryTimeout) throws SQLException {
    statement.setQueryTimeout(queryTimeout);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return sql + " " + indexMap;
  }


  /**
   * Cacheable parse result, encapsulating the JDBC-format SQL string and the list of positions
   * at which a named parameter appears.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  public static final class ParseResult {

    private final String query;
    private final SqlDialect dialect;
    private final Multimap<String, Integer> indexMap = HashMultimap.create();

    /**
     * Private constructor.
     */
    private ParseResult(String query, SqlDialect sqlDialect) {
      this.query = parse(query);
      this.dialect = sqlDialect;
    }


    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return String.format("[%s]:%s", query, indexMap.toString());
    };


    /**
     * For testing only.
     *
     * @return the list of indexes at which the named
     * parameter can be found.
     */
    List<Integer> getIndexesForParameter(String parameterName) {
      return ImmutableList.copyOf(indexMap.get(parameterName));
    }


    /**
     * For testing only.
     *
     * @return the parsed SQL.
     */
    public String getParsedSql() {
      return query;
    }


    /**
     * Create the prepared statement against the specified connection.
     *
     * @param connection the connection
     * @return the prepared statement.
     * @throws SQLException if the statement could not be created
     */
    public NamedParameterPreparedStatement createFor(Connection connection) throws SQLException {
      return new NamedParameterPreparedStatement(connection, query, indexMap.asMap(), false, this);
    }


    /**
     * Create the prepared statement against the specified connection.  The prepared statement
     * can only be used to run queries (not updates/inserts etc) and is strictly read only
     * and forward only.  For use in bulk data loads.
     *
     * @param connection the connection
     * @return the prepared statement.
     * @throws SQLException if the statement could not be created
     */
    public NamedParameterPreparedStatement createForQueryOn(Connection connection) throws SQLException {
      return new NamedParameterPreparedStatement(connection, query, indexMap.asMap(), true, this);
    }


    /**
     * Parses a SQL query with named parameters. The parameter-index mappings are extracted
     * and stored in a map, and the parsed query with parameter placeholders is returned.
     *
     * @param query The SQL query to parse, which may contain named parameters.
     * @return The parsed SQL query with named parameters replaced by placeholders.
     */
    private String parse(String query) {
      final String databaseType = dialect != null ? dialect.getDatabaseType().identifier() : null;
      final StringBuilder parsedQuery = new StringBuilder();
      final AtomicBoolean foundColon = new AtomicBoolean(false);
      final AtomicInteger index = new AtomicInteger(0);
      final String colon = ":";

      SqlTokenizer.tokenizeSqlQuery(databaseType, query, token -> {
        switch(token.getType()) {
          case OPERATOR:
            if (foundColon.get()) {
              parsedQuery.append(colon);
              foundColon.set(false);
            }
            if (colon.equals(token.getString())) {
              foundColon.set(true);
            } else {
              parsedQuery.append(token.getString());
            }
            break;

          case STRING:
          case DSTRING:
          case WHITESPACE:
          case MCOMMENT:
          case SCOMMENT:
          case DQUOTED:
          case NUMBER:
            if (foundColon.get()) {
              parsedQuery.append(colon);
              foundColon.set(false);
            }
            parsedQuery.append(token.getString());
            break;

          case IDENTIFIER:
            if (foundColon.get()) { // colon followed by identifier
              foundColon.set(false);
              index.incrementAndGet();
              parsedQuery.append("?"); // Replace the parameter with question mark
              indexMap.put(token.getString(), index.get()); // remember the index
            } else {
              parsedQuery.append(token.getString());
            }
            break;

          default:
          case ILLEGAL:
            throw new IllegalStateException(token.getType() + " token at position " + token.getPosition() + " found: " + token.getString());
        }
      });

      return parsedQuery.toString();
    }
  }
}