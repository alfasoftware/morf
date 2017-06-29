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

package org.alfasoftware.morf.jdbc.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Tests for {@link OracleMetaDataProvider}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestOracleMetaDataProvider {

  private final DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private DatabaseType oracle;

  @Before
  public void setup() {
    oracle = DatabaseType.Registry.findByIdentifier(Oracle.IDENTIFIER);
  }


  @Before
  public void before() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
  }


  /**
   * Checks the sql run for an {@link DatabaseMetaDataProvider#isEmptyDatabase()} check.
   *
   * @throws SQLException exception
   */
  @Test
  public void testIsEmptyDatabase() throws SQLException {

    String query1 = "SELECT A.TABLE_NAME, A.COLUMN_NAME FROM ALL_CONS_COLUMNS A JOIN ALL_CONSTRAINTS C  ON A.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND A.OWNER = C.OWNER and A.TABLE_NAME = C.TABLE_NAME WHERE C.TABLE_NAME not like 'BIN$%' AND "
    + "C.OWNER=? AND C.CONSTRAINT_TYPE = 'P' ORDER BY A.TABLE_NAME, A.POSITION";

    final PreparedStatement statement1 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);

    String query2 = "SELECT A.TABLE_NAME, A.COLUMN_NAME FROM ALL_CONS_COLUMNS A JOIN ALL_CONSTRAINTS C  ON A.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND A.OWNER = C.OWNER and A.TABLE_NAME = C.TABLE_NAME WHERE C.TABLE_NAME not like 'BIN$%' AND "
    + "C.OWNER=? AND C.CONSTRAINT_TYPE = 'P' ORDER BY A.TABLE_NAME, A.POSITION";

    final PreparedStatement statement2 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);

    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");

    when(connection.prepareStatement(query1)).thenReturn(statement1);
    when(statement1.executeQuery()).thenAnswer(new ReturnMockResultSet(0));
    assertTrue("Database should be reported empty", oracleMetaDataProvider.isEmptyDatabase());

    when(connection.prepareStatement(query2)).thenReturn(statement2);
    when(statement2.executeQuery()).thenAnswer(new ReturnMockResultSet(1));
    assertFalse("Database should not be reported empty", oracleMetaDataProvider.isEmptyDatabase());

    verify(statement1).setString(1, "TESTSCHEMA");
    verify(statement2).setString(1, "TESTSCHEMA");
  }


  /**
   * Checks the SQL run for retrieving view information
   *
   * @throws SQLException exception
   */
  @Test
  public void testLoadViews() throws SQLException {
    // Given
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement("SELECT view_name FROM ALL_VIEWS WHERE owner=?")).thenReturn(statement);
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSet(1));

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("View names", "[VIEW1]", oracleMetaDataProvider.viewNames().toString());
    View view = oracleMetaDataProvider.views().iterator().next();
    assertEquals("View name", "VIEW1", view.getName());

    try {
      SelectStatement selectStatement = view.getSelectStatement();
      fail("Expected UnsupportedOperationException, got " + selectStatement);
    } catch (UnsupportedOperationException e) {
      assertEquals("Message", "Cannot return SelectStatement as [VIEW1] has been loaded from the database", e.getMessage());
    }

    try {
      String[] dependencies = view.getDependencies();
      fail("Expected UnsupportedOperationException, got " + dependencies);
    } catch (UnsupportedOperationException e) {
      assertEquals("Message", "Cannot return dependencies as [VIEW1] has been loaded from the database", e.getMessage());
    }

    verify(statement).setString(1, "TESTSCHEMA");
  }

  /**
   * Checks that if an exception is thrown by the {@link OracleMetaDataProvider} while the connection is open that the statement being used is correctly closed.
   *
   * @throws SQLException exception
   */
  @Test
  public void testCloseStatementOnException() throws SQLException {
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);

    doThrow(new SQLException("Test")).when(statement).setFetchSize(anyInt());
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSet(1));


    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");

    try {
      oracleMetaDataProvider.isEmptyDatabase();
      fail("Exception expected");
    } catch (RuntimeSqlException e) {
      // We expected the exception - now verify that the statement was closed
      verify(statement).close();
    }
  }


  /**
   * Verify that certain oracle system tables are ignored
   */
  @Test
  public void testIgnoreSystemTables() throws SQLException {
    // Given
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement(anyString())).thenReturn(statement);

    // This is the list of tables that's returned.
    when(statement.executeQuery()).thenAnswer(new ReturnTablesMockResultSet(1)).thenAnswer(new ReturnTablesMockResultSet(8));

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("Table names", "[AREALTABLE]", oracleMetaDataProvider.tableNames().toString());
    assertFalse("Table names", oracleMetaDataProvider.tableNames().toString().contains("DBMS"));
  }


  /**
   * Verify that the data type mapping is correct for date columns.
   */
  @Test
  public void testCorrectDataTypeMappingDate() throws SQLException {
    // Given
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement(anyString())).thenReturn(statement);

    // This is the list of tables that's returned.
    when(statement.executeQuery()).thenAnswer(new ReturnTablesMockResultSet(1)).thenAnswer(new ReturnTablesWithDateColumnMockResultSet(2));

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("Table names", "[AREALTABLE]", oracleMetaDataProvider.tableNames().toString());
    Column dateColumn = Iterables.find(oracleMetaDataProvider.getTable("AREALTABLE").columns(), new Predicate<Column>() {

      @Override
      public boolean apply(Column input) {
        return "dateColumn".equalsIgnoreCase(input.getName());
      }
    });
    assertEquals("Date column type", dateColumn.getType(), DataType.DATE);
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnTablesMockResultSet implements Answer<ResultSet> {

    private final int numberOfResultRows;
    private int counter;

    /**
     * @param numberOfResultRows
     */
    private ReturnTablesMockResultSet(int numberOfResultRows) {
      super();
      this.numberOfResultRows = numberOfResultRows;
    }


    @Override
    public ResultSet answer(final InvocationOnMock invocation) throws Throwable {
      final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return counter++ < numberOfResultRows;
        }
      });

      //
      when(resultSet.getString(1)).thenReturn("AREALTABLE").thenReturn("DBMS_TEST");
      when(resultSet.getString(2)).thenReturn("TableComment");
      when(resultSet.getString(3)).thenReturn("ID");
      when(resultSet.getString(4)).thenReturn("IDComment");
      when(resultSet.getString(5)).thenReturn("VARCHAR2");
      when(resultSet.getString(6)).thenReturn("10");
      return resultSet;
    }
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnTablesWithDateColumnMockResultSet implements Answer<ResultSet> {

    private final int numberOfResultRows;
    private int counter;

    /**
     * @param numberOfResultRows
     */
    private ReturnTablesWithDateColumnMockResultSet(int numberOfResultRows) {
      super();
      this.numberOfResultRows = numberOfResultRows;
    }


    @Override
    public ResultSet answer(final InvocationOnMock invocation) throws Throwable {
      final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return counter++ < numberOfResultRows;
        }
      });

      //
      when(resultSet.getString(1)).thenReturn("AREALTABLE");
      when(resultSet.getString(2)).thenReturn("TableComment");
      when(resultSet.getString(3)).thenReturn("dateColumn");
      when(resultSet.getString(4)).thenReturn("");
      when(resultSet.getString(5)).thenReturn("DATE");
      when(resultSet.getString(6)).thenReturn("");
      return resultSet;
    }
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnMockResultSet implements Answer<ResultSet> {

    private final int numberOfResultRows;


    /**
     * @param numberOfResultRows
     */
    private ReturnMockResultSet(int numberOfResultRows) {
      super();
      this.numberOfResultRows = numberOfResultRows;
    }


    @Override
    public ResultSet answer(final InvocationOnMock invocation) throws Throwable {
      final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenAnswer(new Answer<Boolean>() {
        private int counter;

        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          return counter++ < numberOfResultRows;
        }
      });

      when(resultSet.getString(1)).thenReturn("VIEW1");
      when(resultSet.getString(3)).thenReturn("SOMEPRIMARYKEYCOLUMN");
      return resultSet;
    }
  }
}
