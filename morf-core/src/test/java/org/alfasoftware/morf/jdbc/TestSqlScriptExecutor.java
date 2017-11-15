package org.alfasoftware.morf.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Test for {@link SqlScriptExecutor}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestSqlScriptExecutor {

  @Mock private Connection connection;
  @Mock private Statement  statement;
  @Mock private DataSource dataSource;
  @Mock private SqlDialect sqlDialect;

  private SqlScriptExecutor sqlScriptExecutor;


  /**
   * Set up mocks.
   *
   * @throws SQLException if something goes wrong.
   */
  @Before
  public void setUp() throws SQLException {
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    dataSource = mock(DataSource.class);
    sqlDialect = mock(SqlDialect.class);
    sqlScriptExecutor = new SqlScriptExecutorProvider(dataSource, sqlDialect).get();

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
  }


  /**
   * Verify that {@link SqlScriptExecutor#execute(Iterable)} returns the number of rows updated.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testExecute() throws SQLException {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");

    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.execute(sqlScript);
    assertEquals("Return value", 7, result);
  }


  /**
   * Verify that exception handling works when a statement cannot be executed
   *
   * @throws Exception if something goes wrong during mocking.
   */
  @Test
  public void testExecuteFailure() throws Exception {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");
    when(statement.execute(sqlScript.get(0))).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.execute(sqlScript);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScript.get(0) + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }


  /**
   * Verify that {@link SqlScriptExecutor#execute(Iterable, Connection) returns the number of rows updated.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testExecuteWithScriptAndConnectionParameters() throws SQLException {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");

    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.execute(sqlScript, connection);
    assertEquals("Return value", 7, result);
  }


  /**
   * Verify that exception handling works when a statement cannot be executed.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testExecuteWithScriptAndConnectionParamatersFailure() throws Exception {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");

    when(statement.execute(sqlScript.get(0))).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.execute(sqlScript, connection);
      fail("Expected RuntimeSqlException");
    } catch(RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScript.get(0) + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }


  /**
   * Verify that {@link SqlScriptExecutor#executeAndCommit(Iterable, Connection)} a list of scripts will return the number of rows updated
   * and commit all statements.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testExecuteAndCommit() throws SQLException {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");

    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.executeAndCommit(sqlScript, connection);
    assertEquals("Return value", 7, result);
    verify(connection, times(sqlScript.size())).commit();
  }


  /**
   * Verify that exception handling works when a statement cannot be executed.
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testExecuteAndCommitFailure() throws Exception {
    List<String> sqlScript = Arrays.asList("update table set column = 1;", "update table2 set column = 2;");

    when(statement.execute(sqlScript.get(0))).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.executeAndCommit(sqlScript, connection);
      fail("Expected RuntimeSqlException");
    } catch(RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScript.get(0) + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }
}

