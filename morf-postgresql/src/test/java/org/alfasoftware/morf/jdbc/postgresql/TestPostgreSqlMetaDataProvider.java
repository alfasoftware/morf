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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Index;
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
  private static final String TABLE_NAME = "AREALTABLE";
  private static final String TEST_SCHEMA = "TestSchema";

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
    final Schema postgresMetaDataProvider = postgres.openSchema(connection, "TestDatabase", TEST_SCHEMA);
    assertEquals("Sequence names", "[Sequence1]", postgresMetaDataProvider.sequenceNames().toString());
    Sequence sequence = postgresMetaDataProvider.sequences().iterator().next();
    assertEquals("Sequence name", "Sequence1", sequence.getName());

    verify(statement).setString(1, TEST_SCHEMA);
  }

  /**
   * Checks the SQL run for retrieving sequences information
   *
   * @throws SQLException exception
   */
  @Test
  public void testLoadAllIgnoredIndexes() throws SQLException {
    // Given
    Statement statement = mock(Statement.class, RETURNS_SMART_NULLS);
    when(connection.createStatement(eq(ResultSet.TYPE_FORWARD_ONLY), eq(ResultSet.CONCUR_READ_ONLY))).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenAnswer(answer -> {
      ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
      when(resultSet.next()).thenReturn(true, true, true, false);
      when(resultSet.getString(1)).thenReturn("AREALTABLE_1", "AREALTABLE_PRF1", "AREALTABLE_PRF2");
      when(resultSet.getString(2)).thenReturn("REALNAME:[AREALTABLE_1]", "REALNAME:[AREALTABLE_PRF1]", "REALNAME:[AREALTABLE_PRF2]");
      return resultSet;
    });

    DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class, RETURNS_SMART_NULLS);
    when(connection.getMetaData()).thenReturn(databaseMetaData);

    // mock getTables
    when(databaseMetaData.getTables(null, TEST_SCHEMA, null, new String[] { "TABLE" }))
      .thenAnswer(answer -> {
        ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(3)).thenReturn(TABLE_NAME); // // 3 - TABLE_NAME
        when(resultSet.getString(2)).thenReturn(TEST_SCHEMA); // 2 - TABLE_SCHEM
        when(resultSet.getString(5)).thenReturn("REALNAME:[AREALTABLE]"); // 5 - TABLE_REMARKS
        when(resultSet.getString(4)).thenReturn("REGULAR"); // 4 - TABLE_TYPE

        return resultSet;
      });

    // mock getColumns
    when(databaseMetaData.getColumns(null, TEST_SCHEMA, null, null))
      .thenAnswer(answer -> {
        ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(3)).thenReturn(TABLE_NAME); // 3 - COLUMN_TABLE_NAME
        when(resultSet.getString(4)).thenReturn("column1"); // 4 - COLUMN_NAME
        when(resultSet.getString(6)).thenReturn("VARCHAR"); // 6 - COLUMN_TYPE_NAME
        when(resultSet.getInt(5)).thenReturn(12);  // 5 - COLUMN_DATA_TYPE - VARCHAR 12
        when(resultSet.getInt(7)).thenReturn(1);  // 7 - COLUMN_SIZE - width
        when(resultSet.getInt(9)).thenReturn(0);  // 9 - COLUMN_DECIMAL_DIGITS - scale
        when(resultSet.getString(12)).thenReturn("REALNAME:[column1]/TYPE:[STRING]");  // 12 - TABLE_REMARKS
        when(resultSet.getString(18)).thenReturn("NO"); // 18 - COLUMN_IS_NULLABLE
        when(resultSet.getString(23)).thenReturn("NO"); // 23 - COLUMN_IS_AUTOINCREMENT
        when(resultSet.getString(23)).thenReturn(null); // 13 - COLUMN_DEFAULT_EXPR

        return resultSet;
      });

    // mock getIndexInfo
    when(databaseMetaData.getIndexInfo(null, TEST_SCHEMA, "arealtable", false, false))
      .thenAnswer(answer -> {
        ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getString(6)).thenReturn("AREALTABLE_1", "AREALTABLE_PRF1", "AREALTABLE_PRF2"); // 6 - INDEX_NAME
        when(resultSet.getString(9)).thenReturn("column1", "column1", "column1");
        when(resultSet.getBoolean(4)).thenReturn(true, true, true); // 4 - INDEX_NON_UNIQUE
        return resultSet;
      });

    // When
    final Schema postgresMetaDataProvider = postgres.openSchema(connection, "TestDatabase", TEST_SCHEMA);
    Map<String, List<Index>> ignoredIndexesMap = ((AdditionalMetadata)postgresMetaDataProvider).ignoredIndexes();
    // test loading the cached version:
    Map<String, List<Index>> ignoredIndexesMap1 = ((AdditionalMetadata)postgresMetaDataProvider).ignoredIndexes();

    // Then
    assertEquals("map size must match", 1, ignoredIndexesMap.size());
    assertEquals("map size must match", 1, ignoredIndexesMap1.size());
    assertEquals("table ignored indexes size must match", 2, ignoredIndexesMap.get(TABLE_NAME).size());
    Index indexPrf1 = ignoredIndexesMap.get(TABLE_NAME).get(0);
    Index indexPrf2 = ignoredIndexesMap.get(TABLE_NAME).get(1);
    assertEquals("index prf1 name", "AREALTABLE_PRF1", indexPrf1.getName());
    assertThat("index prf1 columns", indexPrf1.columnNames(), contains("column1"));
    assertEquals("index prf2 name", "AREALTABLE_PRF2", indexPrf2.getName());
    assertThat("index prf2 columns", indexPrf2.columnNames(), contains("column1"));
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
