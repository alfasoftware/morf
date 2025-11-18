package org.alfasoftware.morf.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.time.Clock;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor;
import org.alfasoftware.morf.metadata.DataValueLookup;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test for {@link SqlScriptExecutor}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestSqlScriptExecutor {

  private final String              sqlScriptOne         = "update table set column = 1;";
  private final String              sqlScriptTwo         = "update table2 set column = 2;";
  private final List<String>        sqlScripts           = ImmutableList.of(sqlScriptOne, sqlScriptTwo);

  private final Connection          connection           = mock(Connection.class);
  private final Statement           statement            = mock(Statement.class);
  private final DataSource          dataSource           = mock(DataSource.class);
  private final SqlDialect          sqlDialect           = mock(SqlDialect.class);
  private final DatabaseType        databaseType         = mock(DatabaseType.class);
  private final SqlScriptVisitor    SqlScriptVisitor     = mock(SqlScriptVisitor.class);
  private final ConnectionResources connectionResources  = mock(ConnectionResources.class);

  private final SqlScriptExecutor sqlScriptExecutor = new SqlScriptExecutorProvider(dataSource, sqlDialect).get();


  /**
   * Set up mocks.
   */
  @Before
  public void setUp() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(sqlDialect.getDatabaseType()).thenReturn(databaseType);
    when(databaseType.reclassifyException(any(Exception.class))).thenAnswer(invoc -> (Exception) invoc.getArguments()[0]);
  }

  /**
   * Verify that a {@link SqlScriptExecutor} instantiated without ConnectionResources sets the legacy fetch sizes on {@link java.sql.PreparedStatement}.
   */
  @Test
  public void testLegacyFetchSizesForSqlScriptExecutor() throws SQLException {
    verify(sqlDialect, times(0)).fetchSizeForBulkSelects();
    verify(sqlDialect, times(0)).fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
    verify(sqlDialect, times(1)).legacyFetchSizeForBulkSelects();
    verify(sqlDialect, times(1)).legacyFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
  }


  /**
   * Verify that a {@link SqlScriptExecutor} instanstiated with fetch sizes configured with ConnectionResources sets the configured fetch sizes {@link java.sql.PreparedStatement}.
   */
  @Test
  public void testConnectionResourcesWithFetchSizesForSqlScriptExecutor() throws SQLException {

    ConnectionResources connectionResourcesWithFetchSizes = mock(ConnectionResources.class);
    DataSource testDataSource = mock(DataSource.class);
    SqlDialect testSqlDialect = mock(SqlDialect.class);
    when(connectionResourcesWithFetchSizes.getDataSource()).thenReturn(testDataSource);
    when(connectionResourcesWithFetchSizes.sqlDialect()).thenReturn(testSqlDialect);
    when(connectionResourcesWithFetchSizes.getFetchSizeForBulkSelects()).thenReturn(1000);
    when(connectionResourcesWithFetchSizes.getFetchSizeForBulkSelects()).thenReturn(1);

    new SqlScriptExecutorProvider(connectionResourcesWithFetchSizes).get();

    verify(testSqlDialect, times(0)).fetchSizeForBulkSelects();
    verify(testSqlDialect, times(0)).fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();

    verify(connectionResourcesWithFetchSizes, times(2)).getFetchSizeForBulkSelects();
    verify(connectionResourcesWithFetchSizes, times(2)).getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
  }


  /**
   * Verify that a {@link SqlScriptExecutor} constructed with no fetch sizes configured on ConnectionResources sets the default fetch sizes on {@link java.sql.PreparedStatement}.
   */
  @Test
  public void testConnectionResourcesWithoutFetchSizesForSqlScriptExecutor() throws SQLException {

    ConnectionResources connectionResourcesWithoutFetchSizes = mock(ConnectionResources.class);
    DataSource testDataSource = mock(DataSource.class);
    SqlDialect testSqlDialect = mock(SqlDialect.class);
    when(connectionResourcesWithoutFetchSizes.getDataSource()).thenReturn(testDataSource);
    when(connectionResourcesWithoutFetchSizes.sqlDialect()).thenReturn(testSqlDialect);
    when(connectionResourcesWithoutFetchSizes.getFetchSizeForBulkSelects()).thenReturn(null);
    when(connectionResourcesWithoutFetchSizes.getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()).thenReturn(null);
    when(testSqlDialect.fetchSizeForBulkSelects()).thenReturn(2000);
    when(testSqlDialect.fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()).thenReturn(2000);

    new SqlScriptExecutorProvider(connectionResourcesWithoutFetchSizes).get();

    verify(testSqlDialect, times(1)).fetchSizeForBulkSelects();
    verify(testSqlDialect, times(1)).fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();

    verify(connectionResourcesWithoutFetchSizes, times(1)).getFetchSizeForBulkSelects();
    verify(connectionResourcesWithoutFetchSizes, times(1)).getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();
  }


  /**
   * Verify that {@link SqlScriptExecutor#execute(Iterable)} returns the number of rows updated.
   */
  @Test
  public void testExecute() throws SQLException {
    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.execute(sqlScripts);
    assertEquals("Return value", 7, result);
  }


  /**
   * Verify that exception handling works when a statement cannot be executed
   */
  @Test
  public void testExecuteFailure() throws Exception {
    when(statement.execute(sqlScriptOne)).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.execute(sqlScripts);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScriptOne + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }


  /**
   * Verify that {@link SqlScriptExecutor#execute(Iterable, Connection) returns the number of rows updated.
   */
  @Test
  public void testExecuteWithScriptAndConnectionParameters() throws SQLException {
    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.execute(sqlScripts, connection);
    assertEquals("Return value", 7, result);
  }


  /**
   * Verify that exception handling works when a statement cannot be executed.
   */
  @Test
  public void testExecuteWithScriptAndConnectionParamatersFailure() throws Exception {
    when(statement.execute(sqlScriptOne)).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.execute(sqlScripts, connection);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScriptOne + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }


  /**
   * Verify that {@link SqlScriptExecutor#executeAndCommit(Iterable, Connection)} a list of scripts will return the number of rows updated and commit all
   * statements.
   */
  @Test
  public void testExecuteAndCommit() throws SQLException {
    when(statement.getUpdateCount()).thenReturn(5).thenReturn(2);

    int result = sqlScriptExecutor.executeAndCommit(sqlScripts, connection);
    assertEquals("Return value", 7, result);
    verify(connection, times(sqlScripts.size())).commit();
  }


  /**
   * Verify that exception handling works when a statement cannot be executed.
   */
  @Test
  public void testExecuteAndCommitFailure() throws Exception {
    when(statement.execute(sqlScriptOne)).thenThrow(new SQLException());

    try {
      sqlScriptExecutor.executeAndCommit(sqlScripts, connection);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScriptOne + "]"));
      assertEquals("Cause", SQLException.class, e.getCause().getClass());
    }
  }


  /**
   * Test that exceptions are reclassified by the DatabaseType and wrapped
   */
  @Test
  public void testExceptionReclassification() throws Exception {
    RuntimeException originalException = new RuntimeException();
    SQLException transformedException = new SQLException();
    when(statement.execute(sqlScriptOne)).thenThrow(originalException);
    when(databaseType.reclassifyException(originalException)).thenReturn(transformedException);

    try {
      sqlScriptExecutor.executeAndCommit(sqlScripts, connection);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      assertTrue("Message", e.getMessage().startsWith("Error executing SQL [" + sqlScriptOne + "]"));
      assertEquals("Cause", transformedException, e.getCause());
    }
  }


  /**
   * Tests the execution timing for {@link SqlScriptExecutor#execute(String)}.
   */
  @Test
  public void testSqlExecutionTiming1() {
    // GIVEN:
    Clock clock = mock(Clock.class);
    Long startTimeInMs = 1000L;
    Long endTimeInMs = 2000L;
    Long expectedDurationInMs = endTimeInMs - startTimeInMs;
    when(clock.millis()).thenReturn(startTimeInMs, endTimeInMs);

    var sqlScriptExecutor = new SqlScriptExecutor(SqlScriptVisitor, dataSource, sqlDialect, connectionResources, clock);

    // WHEN:
    sqlScriptExecutor.execute(ImmutableList.of(sqlScriptOne));

    // THEN:
    verify(SqlScriptVisitor).afterExecute(eq(sqlScriptOne), anyLong(), eq(expectedDurationInMs));
  }


  /**
   * Tests the execution timing for {@link SqlScriptExecutor#executeQuery(String, SqlScriptExecutor.ResultSetProcessor)}.
   */
  @Test
  public void testSqlExecutionTiming2() throws SQLException {
    // GIVEN:
    Clock clock = mock(Clock.class);
    Long startTimeInMs = 1000L;
    Long endTimeInMs = 2000L;
    Long expectedDurationInMs = endTimeInMs - startTimeInMs;
    when(clock.millis()).thenReturn(startTimeInMs, endTimeInMs);

    when(connectionResources.getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()).thenReturn(1000);
    var resultSetProcessor = mock(SqlScriptExecutor.ResultSetProcessor.class);

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(eq(sqlScriptOne))).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    var sqlScriptExecutor = new SqlScriptExecutor(SqlScriptVisitor, dataSource, sqlDialect, connectionResources, clock);

    // WHEN:
    sqlScriptExecutor.executeQuery(sqlScriptOne, resultSetProcessor);

    // THEN:
    verify(SqlScriptVisitor).afterExecute(anyString(), anyLong(), eq(expectedDurationInMs));
  }


  /**
   * Tests the execution timing for {@link SqlScriptExecutor#execute(String, Connection, Iterable, DataValueLookup)}.
   */
  @Test
  public void testSqlExecutionTiming3() throws SQLException {
    // GIVEN:
    Clock clock = mock(Clock.class);
    Long startTimeInMs = 1000L;
    Long endTimeInMs = 2000L;
    Long expectedDurationInMs = endTimeInMs - startTimeInMs;
    when(clock.millis()).thenReturn(startTimeInMs, endTimeInMs);

    when(connectionResources.getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()).thenReturn(1000);

    SqlParameter sqlParameter = mock(SqlParameter.class);

    DataValueLookup dataValueLookup = mock(DataValueLookup.class);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(connection.prepareStatement(eq(sqlScriptOne))).thenReturn(preparedStatement);
    ResultSet resultSet = mock(ResultSet.class);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);

    var sqlScriptExecutor = new SqlScriptExecutor(SqlScriptVisitor, dataSource, sqlDialect, connectionResources, clock);

    // WHEN:
    sqlScriptExecutor.execute(sqlScriptOne, connection, List.of(sqlParameter), dataValueLookup);

    // THEN:
    verify(SqlScriptVisitor).afterExecute(anyString(), anyLong(), eq(expectedDurationInMs));
  }
}