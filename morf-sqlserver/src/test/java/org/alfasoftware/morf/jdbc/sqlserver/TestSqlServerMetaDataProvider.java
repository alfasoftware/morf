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

package org.alfasoftware.morf.jdbc.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;


/**
 * Test class for {@link SqlServerMetaDataProvider}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestSqlServerMetaDataProvider {

  private final DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private DatabaseType sqlServer;

  @Before
  public void setup() {
    sqlServer = DatabaseType.Registry.findByIdentifier(SqlServer.IDENTIFIER);
  }


  @Before
  public void before() throws SQLException {
    final PreparedStatement createStatement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(createStatement);
    when(createStatement.executeQuery(any())).thenAnswer(new ReturnMockResultSetWithSequence(1));
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

    when(connection.prepareStatement("SELECT name FROM sys.all_objects WHERE type='SO' AND SCHEMA_NAME(schema_id)=?")).thenReturn(statement);

    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSetWithSequence(1));

    // When
    final Schema sqlServerMetaDataProvider = sqlServer.openSchema(connection, "TestDatabase", "TestSchema");
    assertEquals("Sequence names", "[Sequence1]", sqlServerMetaDataProvider.sequenceNames().toString());
    Sequence sequence = sqlServerMetaDataProvider.sequences().iterator().next();
    assertEquals("Sequence name", "Sequence1", sequence.getName());
    assertEquals("Partitioned table names", Lists.newArrayList(), sqlServerMetaDataProvider.partitionedTableNames());
    assertEquals("Partition table names", Lists.newArrayList(), sqlServerMetaDataProvider.partitionTableNames());

    verify(statement).setString(1, "TestSchema");
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

}
