package org.alfasoftware.morf.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.NoSuchElementException;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

/**
 * Tests for {@link ResultSetIterator}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestResultSetIterator {

  @Mock private Connection connection;
  @Mock private SqlDialect sqlDialect;
  @Mock private Statement statement;

  @Before
  public void setup() throws SQLException {
    MockitoAnnotations.initMocks(this);
    given(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
      .willReturn(statement);
  }


  /**
   * Tests building a ResultSetIterator with a query that produces an empty result set.
   */
  @Test
  public void testQueryWithEmptyResultSet() throws SQLException {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    ResultSet resultSet = mock(ResultSet.class);
    given(statement.executeQuery(query)).willReturn(resultSet);
    given(resultSet.findColumn("Column")).willReturn(1);

    // When
    @SuppressWarnings("resource") /* Resources are closed in ResultSetIterator.close()
                                     automatically when caller attempts to advance past
                                     the last row in the result set. */
    ResultSetIterator resultSetIterator = new ResultSetIterator(table, query, connection, sqlDialect);

    // Then
    assertFalse(resultSetIterator.hasNext());
    verify(statement).close();
    verify(resultSet).close();
  }


  /**
   * Tests building a ResultSetIterator with a query that produces a result set.
   * @throws Exception
   */
  @Test
  public void testQueryWithResultSet() throws Exception {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    ResultSet resultSet = mock(ResultSet.class);
    given(statement.executeQuery(query)).willReturn(resultSet);
    given(resultSet.findColumn("Column")).willReturn(1);
    given(resultSet.next()).willReturn(true).willReturn(true).willReturn(false);

    // When
    @SuppressWarnings("resource") /* Resources are closed in ResultSetIterator.close()
                                     automatically when caller attempts to advance past
                                     the last row in the result set. */
    ResultSetIterator resultSetIterator = new ResultSetIterator(table, query, connection, sqlDialect);

    // Then
    assertTrue(resultSetIterator.hasNext());

    resultSetIterator.next();
    resultSetIterator.next();

    assertFalse(resultSetIterator.hasNext());

    verify(resultSet).close();
    verify(statement).close();

    boolean gotException = false;
    try {
      resultSetIterator.next();
    } catch (NoSuchElementException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }


  /**
   * Ensures that removing a record is unsupported.
   * @throws Exception
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testRemoveWhenBuiltWithQuery() throws Exception {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    ResultSet resultSet = mock(ResultSet.class);
    given(statement.executeQuery(query)).willReturn(resultSet);
    given(resultSet.findColumn("Column")).willReturn(1);

    // When
    ResultSetIterator resultSetIterator = new ResultSetIterator(table, query, connection, sqlDialect);

    // Then
    resultSetIterator.remove();

    // When
    resultSetIterator.close();

    // Then
    verify(resultSet).close();
    verify(statement).close();
  }


  /**
   * Tests building a ResultSetIterator with an empty column ordering that produces an empty result set.
   * @throws Exception
   */
  @Test
  public void testBuildEmptyResultSetWithEmptyColumnOrdering() throws Exception {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    ResultSet resultSet = mock(ResultSet.class);
    given(resultSet.findColumn("Column")).willReturn(1);
    given(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).willReturn(query);
    given(statement.executeQuery(query)).willReturn(resultSet);

    // When
    @SuppressWarnings("resource") /* Resources are closed in ResultSetIterator.close()
                                     automatically when caller attempts to advance past
                                     the last row in the result set. */
    ResultSetIterator resultSetIterator = new ResultSetIterator(table, Lists.newArrayList(), connection, sqlDialect);

    // Then
    assertFalse(resultSetIterator.hasNext());
    verify(resultSet).close();
    verify(statement).close();
  }


  /**
   * Tests building a ResultSetIterator with an empty column ordering that produces a result set.
   * @throws Exception
   */
  @Test
  public void testBuildWithEmptyColumnOrdering() throws Exception {
    // Given
    Table table = buildTable();
    String query = "select column from table";
    ResultSet resultSet = mock(ResultSet.class);
    given(resultSet.findColumn("Column")).willReturn(1);
    given(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).willReturn(query);
    given(statement.executeQuery(query)).willReturn(resultSet);
    given(resultSet.next()).willReturn(true).willReturn(true).willReturn(false);

    // When
    @SuppressWarnings("resource") /* Resources are closed in ResultSetIterator.close()
                                     automatically when caller attempts to advance past
                                     the last row in the result set. */
    ResultSetIterator resultSetIterator = new ResultSetIterator(table, Lists.newArrayList(), connection, sqlDialect);

    // Then
    assertTrue(resultSetIterator.hasNext());
    resultSetIterator.next();
    resultSetIterator.next();

    assertFalse(resultSetIterator.hasNext());

    verify(resultSet).close();
    verify(statement).close();

    boolean gotException = false;
    try {
      resultSetIterator.next();
    } catch (NoSuchElementException e) {
      gotException = true;
    }
    assertTrue(gotException);
  }


  private static Table buildTable() {
    return new Table() {

      @Override
      public boolean isTemporary() {
        return false;
      }


      @Override
      public List<Index> indexes() {
        return Lists.newArrayList();
      }


      @Override
      public String getName() {
        return "Table";
      }


      @Override
      public List<Column> columns() {
        return Lists.newArrayList(SchemaUtils.column("Column", DataType.STRING, 20).nullable());
      }
    };
  }
}
