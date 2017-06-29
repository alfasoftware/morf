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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

/**
 * Implementation of {@linkplain DataSetProducer} that provides data sets from
 * a JDBC database connection.
 *
 * <p>Note that calling {@link #close()} will close any resources created by the
 * {@linkplain DatabaseDataSetProducer} but will not close the database connection
 * supplied on construction</p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class DatabaseDataSetProducer implements DataSetProducer {

  private static final Log log = LogFactory.getLog(DatabaseDataSetProducer.class);


  /**
   * Dialect used to generate SQL statements.
   */
  private final SqlDialect sqlDialect;

  /**
   * Database connection from which tables should be extracted.
   */
  private Connection connection;

  /**
   * Store the state of the auto-commit flag.
   */
  private boolean wasAutoCommit;

  /**
   * Provides database meta data.
   */
  private Schema schema;

  /**
   * An array of names of the domain classes that should have a total order applied
   * rather than the default id based sort
   */
  private final Map<String, List<String>> orderingOverrides;

  private final DataSource dataSource;

  private final ConnectionResources connectionResources;

  private final Set<ResultSetIterator> openResultSets = Sets.newHashSet();


  /**
   * Creates an instance based on the specified table list.
   *
   * @param connectionResources Database from which to provide data.
   */
  public DatabaseDataSetProducer(ConnectionResources connectionResources) {
    this(connectionResources, connectionResources.getDataSource());
  }


  /**
   * Creates an instance based on the specified table list.
   *
   * <p>Can also be injected from Guice</p>
   *
   * @param connectionResources Database from which to provide data.
   * @param dataSource Data source to provide database connections.
   */
  @Inject
  public DatabaseDataSetProducer(ConnectionResources connectionResources, DataSource dataSource) {
    this(connectionResources, new HashMap<String, List<String>>(), dataSource);
  }


  /**
   * Creates an instance based on the specified table list.
   *
   * @param connectionResources Database from which to provide data.
   * @param tablesForOverriddenSortOrder An array of names of the tables that should have a total order applied rather than the default id based sort
   * @param dataSource Datasource provided database connections.
   */
  public DatabaseDataSetProducer(ConnectionResources connectionResources, Map<String, List<String>> tablesForOverriddenSortOrder, DataSource dataSource) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = connectionResources.sqlDialect();
    this.orderingOverrides = tablesForOverriddenSortOrder;
    this.connectionResources = connectionResources;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    try {
      this.connection = dataSource.getConnection();
      this.wasAutoCommit = connection.getAutoCommit();

      // disable auto-commit on this connection for HSQLDB performance
      wasAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error opening connection", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {

    if (connection == null) {
      return;
    }

    try {
      for (ResultSetIterator resultSetIterator : openResultSets) {
        log.warn("Result set over [" + resultSetIterator.table.getName() + "] is not closed. Forcing closure.");
        resultSetIterator.resultSet.close();
      }
      openResultSets.clear();

      // restore the auto-commit flag.
      connection.setAutoCommit(wasAutoCommit);
      connection.close();
      connection = null;
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error closing result set", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(String tableName) {
    final Table table = getSchema().getTable(tableName);

    return new Iterable<Record>() {
      @Override
      public Iterator<Record> iterator() {
        List<String> columnOrdering = null;

        for (Map.Entry<String, List<String>> entry : orderingOverrides.entrySet()) {
          if (entry.getKey().equalsIgnoreCase(table.getName())) {
            columnOrdering = entry.getValue();
            break;
          }
        }

        return new ResultSetIterator(table, columnOrdering);
      }
    };
  }



  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {

    SelectStatement countQuery = new SelectStatement().from(new TableReference(tableName));
    String sql = sqlDialect.convertStatementToSQL(countQuery);

    if (connection == null) {
      throw new IllegalStateException("Dataset has not been opened");
    }

    try {
      Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      try {
        ResultSet resultSet = statement.executeQuery(sql);
        try {
          // the table is empty if there are no rows returned.
          return !resultSet.next();
        } finally {
          resultSet.close();
        }
      } finally {
        statement.close();
      }

    } catch(SQLException sqlException) {
      throw new RuntimeSqlException("Failed to execute count of rows in table [" + tableName + "]: [" + sql + "]", sqlException);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {

    if (connection == null) {
      throw new IllegalStateException("Dataset has not been opened");
    }

    if (schema == null) {
      // we use the same connection as this provider, so there is no (extra) clean-up to do.
      schema = DatabaseType.Registry.findByIdentifier(connectionResources.getDatabaseType()).openSchema(connection, connectionResources.getDatabaseName(), connectionResources.getSchemaName());
    }

    return schema;
  }


  /**
   * Provides data set iterator functionality based on a jdbc result set.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private class ResultSetIterator implements Iterator<Record> {

    /**
     * The underlying result set to iterate over.
     */
    private final ResultSet resultSet;

    /**
     * A record implementation based on the result set.
     */
    private final ResultSetRecord resultSetRecord;

    /**
     * The jdbc statement opened to supply the result set.
     */
    private final Statement statement;

    /**
     * Indicates if there are more records.
     */
    private boolean hasNext;

    private final Table table;


    /**
     * @param table Meta data for the table we want to iterate over.
     * @param columnOrdering The columns to order by.
     */
    public ResultSetIterator(Table table, List<String> columnOrdering){
      super();
      this.table = table;

      if (connection == null) {
        throw new IllegalStateException("Dataset has not been opened");
      }

      try {
        this.statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        this.statement.setFetchSize(sqlDialect.fetchSizeForBulkSelects());

      } catch (SQLException e) {
        throw new RuntimeSqlException("Error creating table extract statement for table [" + table.getName() + "]", e);
      }

      SelectStatement selectStatement = new SelectStatement();

      if (columnOrdering == null || columnOrdering.isEmpty()) {
        List<AliasedField> orderByPrimaryKey = new ArrayList<>();
        for (Column column : table.columns()) {
          if (column.isPrimaryKey()) {
            orderByPrimaryKey.add(new FieldReference(column.getName(), Direction.ASCENDING));
          }
        }
        if (!orderByPrimaryKey.isEmpty()) {
          selectStatement.orderBy(orderByPrimaryKey.toArray(new AliasedField[orderByPrimaryKey.size()]));
        }
      } else {
        List<AliasedField> orderByList = new ArrayList<>();
        for (String column : columnOrdering) {
          orderByList.add(new FieldReference(column, Direction.ASCENDING));
        }

        selectStatement.orderBy(orderByList.toArray(new AliasedField[orderByList.size()]));
      }

      selectStatement.from(new TableReference(table.getName()));
      openResultSets.add(this);
      String sql = sqlDialect.convertStatementToSQL(selectStatement);
      try {
        this.resultSet = statement.executeQuery(sql);
        this.resultSetRecord = new ResultSetRecord(table, resultSet, sqlDialect);
        advanceResultSet();
      } catch (SQLException e) {
        throw new RuntimeSqlException("Error running table extract statement for table [" + table.getName() + "]: " + sql, e);
      }
    }


    /**
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      return hasNext;
    }

    /**
     * @see java.util.Iterator#next()
     */
    @Override
    public Record next() {
      if (hasNext) {
        resultSetRecord.cacheValues();

        // Attempt to advance
        advanceResultSet();
      }
      return resultSetRecord;
    }

    /**
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove items from a result set iterator");
    }

    /**
     * Advances the underlying result set and releases resources if the end is reached.
     */
    private void advanceResultSet() {
      try {
        hasNext = resultSet.next();
        if (!hasNext) {
          // Also closes the result set
          resultSet.close();
          statement.close();
          openResultSets.remove(this);
        }
      } catch (SQLException e) {
        throw new RuntimeSqlException("Error advancing result set", e);
      }
    }
  }
}
