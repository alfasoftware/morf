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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link ExistingViewHashLoader}.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestExistingViewHashLoader {

  private ExistingViewHashLoader onTest;

  private final DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
  private final SqlDialect dialect = mock(SqlDialect.class, RETURNS_SMART_NULLS);
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private final Statement statement = mock(Statement.class, RETURNS_SMART_NULLS);
  private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

  private int recordIndex;


  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    onTest = new ExistingViewHashLoader(dataSource, dialect);
  }


  /**
   * Tests that we return nothing if there is no deployed views table in the schema.
   */
  @Test
  public void testNoDeployedViewsTable() {
    Schema schema = schema(table("SomeTable"));
    assertFalse(onTest.loadViewHashes(schema).isPresent());
  }


  /**
   * Tests that we return what we've mocked.
   * @throws SQLException
   */
  @Test
  public void testFetch() throws SQLException {
    Schema schema = schema(table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME));

    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(resultSet);
    when(resultSet.next()).thenAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        return recordIndex++ < 5;
      }
    });
    when(resultSet.getString(anyInt())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        if (((Integer)invocation.getArguments()[0]).equals(1)) {
          switch (recordIndex) {
            case 1: return "VIEW1";
            case 2: return "view2"; // we should ignore this one as it's overridden by the closer match (capitalised) below
            case 3: return "VIEW2";
            case 4: return "View1"; // we should ignore this one as it's overridden by the closer match (capitalised) above
            case 5: return "view3"; // lowercase but nothing better so we use it
            default: throw new IllegalStateException("Unexpected index");
          }
        } else {
          switch (recordIndex) {
            case 1: return "hash1";
            case 2: return "HASH2"; // ignore
            case 3: return "hash2";
            case 4: return "HASH1"; // ignore
            case 5: return "hash3";
            default: throw new IllegalStateException("Unexpected index");
          }
        }
      }
    });

    Map<String, String> result = onTest.loadViewHashes(schema).get();

    assertEquals("Result count", 3, result.size());
    assertEquals("View 1 hash", "hash1", result.get("VIEW1"));
    assertEquals("View 2 hash", "hash2", result.get("VIEW2"));
  }
}
