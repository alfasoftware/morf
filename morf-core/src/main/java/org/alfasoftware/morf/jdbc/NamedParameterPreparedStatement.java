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
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.sql.element.SqlParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A wrapped around {@link PreparedStatement} which allows for named parameters.  Parser is the
 * one published by Adam Crume, modified to play nicely with our existing database code.
 *
 * @see <a href="http://www.javaworld.com/article/2077706/core-java/named-parameters-for-preparedstatement.html">
 *     Named parameters for prepared statements</a>
 *
 */
public class NamedParameterPreparedStatement {

  /** The statement this object is wrapping. */
  private final PreparedStatement statement;

  /** Maps parameter names to the list of index positions at which this parameter appears. */
  private final Map<String, List<Integer>> indexMap;

  /** The statement - for logging and exceptions. */
  private final ParseResult sql;


  /**
   * Parses the SQL string containing named parameters in such a form that
   * can be cached, so that prepared statements using the parsed result
   * can be created rapidly.
   *
   * @param sql the SQL
   * @return the parsed result
   */
  public static ParseResult parse(String sql) {
    return new ParseResult(sql);
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
  NamedParameterPreparedStatement(Connection connection, String query, Map<String, List<Integer>> indexMap, boolean queryOnly, ParseResult sql) throws SQLException {
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
   *
   * @return <code>true</code> if the first result is a <code>ResultSet</code>
   *         object; <code>false</code> if the first result is an update
   *         count or there is no result
   * @throws SQLTimeoutException when the driver has determined that the
   * timeout value that was specified by the {@code setQueryTimeout}
   * method has been exceeded and has at least attempted to cancel
   * the currently running {@code Statement}
   * @see Statement#execute
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
    List<Integer> indexes = indexMap.get(parameter.getImpliedName());
    if (indexes == null) {
      throw new IllegalArgumentException("Parameter not found: " + parameter.getImpliedName());
    }
    for (int i = 0; i < indexes.size(); i++) {
      operation.apply(indexes.get(i));
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
        statement.setString(parameterIndex, value);
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
        if (written != value.length) throw new IllegalStateException("Failed to write all bytes to BLOB");
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
    private final Map<String, List<Integer>> indexMap = Maps.newHashMap();

    /**
     * Private constructor.
     */
    private ParseResult(String query) {
      this.query = parse(query);
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
      return indexMap.get(parameterName) == null
          ? ImmutableList.<Integer>of()
          : ImmutableList.copyOf(indexMap.get(parameterName));
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
      return new NamedParameterPreparedStatement(connection, query, indexMap, false, this);
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
      return new NamedParameterPreparedStatement(connection, query, indexMap, true, this);
    }


    /**
     * Parses a query with named parameters. The parameter-index mappings are put
     * into the map, and the parsed query is returned.
     *
     * @param query query to parse
     * @return the parsed query
     */
    private String parse(String query) {
      // I was originally using regular expressions, but they didn't work well for
      // ignoring
      // parameter-like strings inside quotes.
      int length = query.length();
      StringBuffer parsedQuery = new StringBuffer(length);
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      int index = 1;

      for (int i = 0; i < length; i++) {
        char c = query.charAt(i);
        if (inSingleQuote) {
          if (c == '\'') {
            inSingleQuote = false;
          }
        } else if (inDoubleQuote) {
          if (c == '"') {
            inDoubleQuote = false;
          }
        } else {
          if (c == '\'') {
            inSingleQuote = true;
          } else if (c == '"') {
            inDoubleQuote = true;
          } else if (c == ':' && i + 1 < length && Character.isJavaIdentifierStart(query.charAt(i + 1))) {
            int j = i + 2;
            while (j < length && Character.isJavaIdentifierPart(query.charAt(j))) {
              j++;
            }
            String name = query.substring(i + 1, j);
            c = '?'; // replace the parameter with a question mark

            //CHECKSTYLE:OFF ModifiedControlVariableCheck
            i += name.length(); // skip past the end if the parameter
            //CHECKSTYLE:ON:

            List<Integer> indexList = indexMap.get(name);
            if (indexList == null) {
              indexList = Lists.newArrayList();
              indexMap.put(name, indexList);
            }
            indexList.add(index);

            index++;
          }
        }
        parsedQuery.append(c);
      }

      return parsedQuery.toString();
    }
  }
}