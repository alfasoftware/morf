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

package org.alfasoftware.morf.jdbc.h2;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;


/**
 * Test class for {@link H2MetaDataProvider}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestH2MetaDataProvider {

  private final DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private DatabaseType h2;

  @Before
  public void setup() {
    h2 = DatabaseType.Registry.findByIdentifier(H2.IDENTIFIER);
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
    when(connection.prepareStatement("SELECT SEQUENCE_NAME FROM INFORMATION_SCHEMA.SEQUENCES")).thenReturn(statement);
    when(statement.executeQuery()).thenAnswer(new ReturnMockResultSetWithSequence(1, false, false, false));

    // When
    final Schema h2MetaDataProvider = h2.openSchema(connection, "TestDatabase", "TestSchema");
    assertEquals("Sequence names", "[Sequence1]", h2MetaDataProvider.sequenceNames().toString());
    Sequence sequence = h2MetaDataProvider.sequences().iterator().next();
    assertEquals("Sequence name", "Sequence1", sequence.getName());

    verify(statement, never()).setString(1, "TestSchema");
  }


  /**
   * Mockito {@link Answer} that returns a mock result set with a given number of resultRows.
   */
  private static final class ReturnMockResultSetWithSequence implements Answer<ResultSet> {

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
    private ReturnMockResultSetWithSequence(int numberOfResultRows, boolean isConstraintQuery, boolean failPKConstraintCheck, boolean failNullPKConstraintCheck) {
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
        when(resultSet.getString(1)).thenReturn("Sequence1");
      }
      return resultSet;
    }
  }

}
