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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.metadata.*;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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


    final PreparedStatement statement1 = mockGetTableKeysQuery(0, false, false);

    final Schema oracleMetaDataProvider1 = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertTrue("Database should be reported empty", oracleMetaDataProvider1.isEmptyDatabase());


    final PreparedStatement statement2= mockGetTableKeysQuery(1, false, false);

    final Schema oracleMetaDataProvider2 = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertFalse("Database should not be reported empty", oracleMetaDataProvider2.isEmptyDatabase());

    verify(statement1).setString(1, "TESTSCHEMA");
    verify(statement2).setString(1, "TESTSCHEMA");
  }

  @Test
  public void testIfCatchesWronglyNamedPrimaryKeyIndex() throws SQLException {
    mockGetTableKeysQuery(1, true, false);

    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");

    try {
      oracleMetaDataProvider.isEmptyDatabase();
      fail("Exception expected");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(),
        "Primary Key on table [AREALTABLE] column [dateColumn] backed with an index whose name does not end in _PK [PRIMARY_INDEX_NK]" + System.lineSeparator());
    }
  }

  @Test
  public void testIfCatchesWronglyNamedPrimaryKeyIndexNull() throws SQLException {
    mockGetTableKeysQuery(1, false, true);

    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");

    try {
      oracleMetaDataProvider.isEmptyDatabase();
      fail("Exception expected");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(),
          "Primary Key on table [AREALTABLE] column [dateColumn] backed with an index whose name does not end in _PK [null]" + System.lineSeparator());
    }
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
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSet(1, false, false, false));

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
   * Checks the SQL run for retrieving sequences information
   *
   * @throws SQLException exception
   */
  @Test
  public void testLoadSequences() throws SQLException {
    // Given
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement("SELECT sequence_name FROM ALL_SEQUENCES WHERE cache_size != 2000 AND sequence_owner=?")).thenReturn(statement);
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSetWithSequence(1));

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("Sequence names", "[SEQUENCE1]", oracleMetaDataProvider.sequenceNames().toString());
    Sequence sequence = oracleMetaDataProvider.sequences().iterator().next();
    assertEquals("Sequence name", "SEQUENCE1", sequence.getName());

    try {
      Integer startsWith = sequence.getStartsWith();
      fail("Expected UnsupportedOperationException, got " + startsWith);
    } catch (UnsupportedOperationException e) {
      assertEquals("Message", "Cannot return startsWith as [SEQUENCE1] has been loaded from the database",
        e.getMessage());
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
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSet(1, false, false, false));


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
    mockGetTableKeysQuery(0, false, false);



    // This is the list of tables that's returned.
    when(statement.executeQuery()).thenAnswer(new ReturnTablesMockResultSet(1)).thenAnswer(new ReturnTablesMockResultSet(8));

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("Table names", "[AREALTABLE]", oracleMetaDataProvider.tableNames().toString());
    assertFalse("Table names", oracleMetaDataProvider.tableNames().toString().contains("DBMS"));
  }


  /**
   * Tests that regular and partitioned indexes not in a usable state have the <UNUSABLE> string appended to their name.
   */
  @Test
  public void testInvalidIndexHandling() throws Exception {
    // Given
    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement(anyString())).thenReturn(statement);

    mockGetTableKeysQuery(0, false, false);
    // This is the list of tables that's returned.
    when(statement.executeQuery()).thenAnswer(new ReturnTablesMockResultSet(1)).thenAnswer(new ReturnTablesMockResultSet(8));

    PreparedStatement statement1 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement("select table_name, index_name, uniqueness, status from ALL_INDEXES where owner=? order by table_name, index_name")).thenReturn(statement1);

    // four indexes, two regular, two partitioned of which one is valid and the other is not
    when(statement1.executeQuery()).thenAnswer(answer -> {
      ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenReturn(true, true, true, true, false);
      when(resultSet.getString(1)).thenReturn("AREALTABLE");
      when(resultSet.getString(2)).thenReturn("AREALTABLE_1", "AREALTABLE_2", "AREALTABLE_3", "AREALTABLE_4");
      when(resultSet.getString(4)).thenReturn("VALID", "UNUSABLE", "N/A", "N/A");
      return resultSet;
    });


    PreparedStatement statement2 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement("select index_name, status from ALL_IND_PARTITIONS where index_owner=?")).thenReturn(statement2);
    when(statement2.executeQuery()).thenAnswer(answer -> {
      ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenReturn(true, true, true, true, false);
      when(resultSet.getString(1)).thenReturn("AREALTABLE_3", "AREALTABLE_3", "AREALTABLE_4", "AREALTABLE_4");
      when(resultSet.getString(2)).thenReturn("USABLE", "USABLE", "UNUSABLE", "USABLE");
      return resultSet;
    });

    // When
    final Schema oracleMetaDataProvider = oracle.openSchema(connection, "TESTDATABASE", "TESTSCHEMA");
    assertEquals("Table names", "[AREALTABLE]", oracleMetaDataProvider.tableNames().toString());

    List<Index> indexes = oracleMetaDataProvider.getTable("AREALTABLE").indexes();
    assertEquals("AREALTABLE_1", indexes.get(0).getName());
    assertEquals("AREALTABLE_2<UNUSABLE>", indexes.get(1).getName());
    assertEquals("AREALTABLE_3", indexes.get(2).getName());
    assertEquals("AREALTABLE_4<UNUSABLE>", indexes.get(3).getName());
  }


  /**
   * Verify that the data type mapping is correct for date columns.
   */
  @Test
  public void testCorrectDataTypeMappingDate() throws SQLException {
    // Given


    final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    mockGetTableKeysQuery(1, false, false);


    // This is the list of tables that's returned.
    when(statement.executeQuery()).thenAnswer(new ReturnTablesWithDateColumnMockResultSet(2));

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
   * Mocks the constraint validation query in OracleMetaDataProvider.readTableKeys() which throws an exception if the primary key index does not end in _PK
   *
   * @param numberOfResultRows
   * @param failPKConstraintCheck
   * @return
   * @throws SQLException
   */
  private final PreparedStatement mockGetTableKeysQuery(int numberOfResultRows, boolean failPKConstraintCheck, boolean failNullPKConstraintCheck) throws SQLException {
    String query1 ="SELECT A.TABLE_NAME, A.COLUMN_NAME, C.INDEX_NAME FROM ALL_CONS_COLUMNS A "
            + "JOIN ALL_CONSTRAINTS C  ON A.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND A.OWNER = C.OWNER and A.TABLE_NAME = C.TABLE_NAME "
            + "WHERE C.TABLE_NAME not like 'BIN$%' AND C.OWNER=? AND C.CONSTRAINT_TYPE = 'P' ORDER BY A.TABLE_NAME, A.POSITION";

    final PreparedStatement statement1 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

    when(connection.prepareStatement(query1)).thenReturn(statement1);
    when(statement1.executeQuery()).thenAnswer(new ReturnMockResultSet(numberOfResultRows, true, failPKConstraintCheck, failNullPKConstraintCheck));
    return statement1;
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnMockResultSet implements Answer<ResultSet> {

    private final int numberOfResultRows;
    private final boolean isConstraintQuery;
    private final boolean failPKConstraintCheck;
    private final boolean failNullPKConstraintCheck;


    /**
     * @param numberOfResultRows
     * @param isConstraintQuery
     * @param failPKConstraintCheck
     * @param failNullPKConstraintCheck
     */
    private ReturnMockResultSet(int numberOfResultRows, boolean isConstraintQuery, boolean failPKConstraintCheck, boolean failNullPKConstraintCheck) {
      super();
      this.numberOfResultRows = numberOfResultRows;
      this.isConstraintQuery = isConstraintQuery;
      this.failPKConstraintCheck = failPKConstraintCheck;
      this.failNullPKConstraintCheck = failNullPKConstraintCheck;
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

      if (isConstraintQuery) {
        when(resultSet.getString(1)).thenReturn("AREALTABLE");
        when(resultSet.getString(2)).thenReturn("dateColumn");

        if (failNullPKConstraintCheck) {
          when(resultSet.getString(3)).thenReturn(null);
        } else {
          if (failPKConstraintCheck) {
            when(resultSet.getString(3)).thenReturn("PRIMARY_INDEX_NK");
          } else {
            when(resultSet.getString(3)).thenReturn("PRIMARY_INDEX_PK");
          }
        }

      } else {
        when(resultSet.getString(1)).thenReturn("VIEW1");
        when(resultSet.getString(3)).thenReturn("SOMEPRIMARYKEYCOLUMN");
      }
      return resultSet;
    }
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnMockResultSetWithSequence implements Answer<ResultSet> {

    private final int numberOfResultRows;


    /**
     * @param numberOfResultRows
     */
    private ReturnMockResultSetWithSequence(int numberOfResultRows) {
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

      when(resultSet.getString(1)).thenReturn("SEQUENCE1");

      return resultSet;
    }
  }
}