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

package org.alfasoftware.morf.jdbc.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * Test class for {@link PostgreSQLMetaDataProvider}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestPostgreSqlMetaDataProvider {

  private final DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private DatabaseType postgres;

  @Before
  public void setup() {
    postgres = DatabaseType.Registry.findByIdentifier(PostgreSQL.IDENTIFIER);
  }


  @Before
  public void before() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
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
    when(connection.prepareStatement("SELECT S.relname FROM pg_class S LEFT JOIN pg_depend D ON " +
      "(S.oid = D.objid AND D.deptype = 'a') LEFT JOIN pg_namespace N on (N.oid = S.relnamespace) WHERE S.relkind = " +
      "'S' AND D.objid IS NULL AND N.nspname=?")).thenReturn(statement);
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSetWithSequence(1));

    // When
    final Schema postgresMetaDataProvider = postgres.openSchema(connection, "TestDatabase", "TestSchema");
    assertEquals("Sequence names", "[Sequence1]", postgresMetaDataProvider.sequenceNames().toString());
    Sequence sequence = postgresMetaDataProvider.sequences().iterator().next();
    assertEquals("Sequence name", "Sequence1", sequence.getName());

    verify(statement).setString(1, "TestSchema");
  }


  /**
   * Checks the SQL run for retrieving partitioned tables information
   *
   * @throws SQLException exception
   */
  @Test
  public void testLoadPartitionedTables() throws SQLException {
    // Given
    final Statement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("select relname from pg_class where not relispartition and relkind = 'p'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition"));
    when(statement.executeQuery("select relname from pg_class where relispartition and relkind = 'r'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition_p0"));

    // When
    final AdditionalMetadata postgresMetaDataProvider = (AdditionalMetadata)postgres.openSchema(connection, "TestDatabase", "TestSchema");
    assertEquals("Partition Table name", "[partition]", postgresMetaDataProvider.partitionedTableNames().toString());
    String partitionTable = postgresMetaDataProvider.partitionedTableNames().iterator().next();
    assertEquals("Partition Table name", "partition", partitionTable);
  }


  /**
   * Checks the SQL run for retrieving partition table information
   *
   * @throws SQLException exception
   */
  @Test
  public void testLoadPartitionTables() throws SQLException {
    // Given
    final Statement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("select relname from pg_class where not relispartition and relkind = 'p'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition"));
    when(statement.executeQuery("select relname from pg_class where relispartition and relkind = 'r'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition_p0"));

    // When
    final AdditionalMetadata postgresMetaDataProvider = (AdditionalMetadata)postgres.openSchema(connection, "TestDatabase", "TestSchema");

    assertEquals("Partition Table name", "[partition_p0]", postgresMetaDataProvider.partitionTableNames().toString());
    String partitionTable = postgresMetaDataProvider.partitionTableNames().iterator().next();
    assertEquals("Partition Table name", "partition_p0", partitionTable);
  }


  /**
   * Checks the SQL run for retrieving partition table information
   *
   * @throws SQLException exception
   */
  @Test
  public void testIgnoredTables() throws SQLException {
    // Given
    final Statement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);

    final PreparedStatement statement1 = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
    when(connection.prepareStatement(anyString())).thenReturn(statement1);

    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("select relname from pg_class where not relispartition and relkind = 'p'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition"));
    when(statement.executeQuery("select relname from pg_class where relispartition and relkind = 'r'"))
      .thenAnswer(new ReturnMockResultSetWithPartitionTables(1, "partition_p0"));
    DatabaseMetaData postgreSQLMetaDataMock = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(postgreSQLMetaDataMock);
    when(postgreSQLMetaDataMock.getTables(any(), any(), any(), any()))
      .thenAnswer(new ReturnMockResultSetWithSequence(0));

    // When
    final Schema postgresMetaDataProvider = postgres.openSchema(connection, "TestDatabase", "TestSchema");
    // Then
    assertEquals("Partition Table name", "[partition]", postgresMetaDataProvider.tableNames().toString());
    assertFalse("Table names", postgresMetaDataProvider.tableNames().toString().contains("partition_p0"));
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

      when(resultSet.getString(1)).thenReturn("Sequence1");

      return resultSet;
    }
  }

  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows for partition tables.
   */
  private static final class ReturnMockResultSetWithPartitionTables implements Answer<ResultSet> {

    private final int numberOfResultRows;
    private final String partitionResult;


    /**
     * @param numberOfResultRows
     */
    private ReturnMockResultSetWithPartitionTables(int numberOfResultRows, String partitionResult) {
      super();
      this.numberOfResultRows = numberOfResultRows;
      // class is rigged for just one value
      this.partitionResult = partitionResult;
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

      when(resultSet.getString(1)).thenReturn(partitionResult);

      return resultSet;
    }
  }
}
