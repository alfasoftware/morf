package org.alfasoftware.morf.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Provides data set iterator functionality based on a jdbc result set.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class ResultSetIterator implements Iterator<Record>, AutoCloseable {

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

  /**
   * Meta data for the query's result.
   */
  private final Table table;


  /**
   * Creates a {@link ResultSetIterator} using the supplied SQL query,
   * applying no further result sorting beyond that contained in the SQL query.
   *
   * @param table Meta data for the query's result.
   * @param query The query to run.
   * @param connection The database connection to use.
   * @param sqlDialect The vendor-specific dialect for the database connection.
   */
  public ResultSetIterator(Table table, String query, Connection connection, SqlDialect sqlDialect) {
    super();
    this.table = table;

    if (connection == null) {
      throw new IllegalStateException("Dataset has not been opened");
    }

    try {
      this.statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      this.statement.setFetchDirection(ResultSet.FETCH_FORWARD);
      this.statement.setFetchSize(sqlDialect.fetchSizeForBulkSelects());
      this.resultSet = statement.executeQuery(query);
      this.resultSetRecord = new ResultSetRecord(table, this.resultSet, sqlDialect);
      advanceResultSet();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error running statement for table [" + table.getName() + "]: " + query, e);
    }
  }


  /**
   * Creates a {@link ResultSetIterator} by selecting all records from a table,
   * ordering the results using the supplied column ordering.
   *
   * @param table Meta data for the table we want to iterate over.
   * @param columnOrdering The columns to order by.
   * @param connection The database connection to use.
   * @param sqlDialect The vendor-specific dialect for the database connection.
   */
  public ResultSetIterator(Table table, List<String> columnOrdering, Connection connection, SqlDialect sqlDialect) {
    this(table, buildSqlQuery(table, columnOrdering, sqlDialect), connection, sqlDialect);
  }


  private static String buildSqlQuery(Table table, List<String> columnOrdering, SqlDialect sqlDialect) {
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
    return sqlDialect.convertStatementToSQL(selectStatement);
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
   * Advances the underlying result set.
   */
  private void advanceResultSet() {
    try {
      hasNext = this.resultSet.next();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error advancing result set", e);
    }
  }


  /**
   * @return the table
   */
  public Table getTable() {
    return table;
  }


  /**
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws SQLException {
    resultSet.close();
    statement.close();
  }
}
