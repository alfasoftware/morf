package org.alfasoftware.morf.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.TableSetSchema;

import com.google.common.collect.Sets;

/**
 * Implementation of {@linkplain DataSetProducer} that provides a data set from
 * a single SQL query.
 *
 * The metadata is described by a {@link Table} and the data is described by
 * the {@link ResultSet}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class SqlQueryDataSetProducer implements DataSetProducer {

  private final Schema schema;
  private final SqlDialect sqlDialect;
  private final DataSource dataSource;
  private Connection connection;
  private final String query;

  /**
   * Store the state of the auto-commit flag.
   */
  private boolean wasAutoCommit;

  private final Set<ResultSetIterator> openResultSets = Sets.newHashSet();


  /**
   * Constructs an instance for the provided {@link Table}.
   *
   * @param table The metadata returned by the query.
   * @param query The query to execute.
   * @param dataSource The data source.
   * @param sqlDialect The SQL dialect.
   */
  public SqlQueryDataSetProducer(Table table, String query, DataSource dataSource, SqlDialect sqlDialect) {
    this.schema = new TableSetSchema(Collections.singleton(table));
    this.dataSource = dataSource;
    this.query = query;
    this.sqlDialect = sqlDialect;
  }


  /**
   * Opens a connection and prepares it.
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
   * Closes the connection and any active result sets.
   */
  @Override
  public void close() {
    if (connection == null) {
      return;
    }

    try {
      for (ResultSetIterator resultSetIterator : openResultSets) {
        resultSetIterator.close();
      }
      openResultSets.clear();

      // restore the auto-commit flag.
      connection.setAutoCommit(wasAutoCommit);
      connection.close();
      connection = null;
    } catch (SQLException e) {
      throw new RuntimeException("Error closing result set", e);
    }
  }


  /**
   * Returns a {@link Schema} containing information about the {@link Table}
   * associated with this {@link ResultSet}.
   */
  @Override
  public Schema getSchema() {
    return schema;
  }


  /**
   * Returns an iterable of records contained in the {@link ResultSet}.
   */
  @Override
  public Iterable<Record> records(String tableName) {
    Table table = getSchema().getTable(tableName);

    return new Iterable<Record>() {
      @Override
      public Iterator<Record> iterator() {
        ResultSetIterator resultSetIterator = new ResultSetIterator(table, query, connection, sqlDialect);
        openResultSets.add(resultSetIterator);
        return resultSetIterator;
      }
    };
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    return records(tableName).iterator().hasNext();
  }
}