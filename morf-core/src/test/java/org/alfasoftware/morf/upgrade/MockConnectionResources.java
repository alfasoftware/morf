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

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import org.alfasoftware.morf.dataset.SchemaAdapter;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.MockDialect;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import com.google.common.collect.Maps;

/**
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class MockConnectionResources  {


  /**
   * @return A mock of {@link ConnectionResources} for upgrade testing.
   */
  public static ConnectionResources build() {
    return new MockConnectionResources().create();
  }

  private SqlDialect dialect;
  private Schema schema;
  private final Map<String, ResultSet> resultSets = Maps.newLinkedHashMap();


  /**
   * Allow specification of the SQL dialect to use.
   *
   * @param dialect Dialect to use.
   * @return this.
   */
  public MockConnectionResources withDialect(SqlDialect dialect) {
    this.dialect = dialect;
    return this;
  }


  /**
   * Allow specification of a backing schema.
   *
   * @param schema Schema to return.
   * @return this.
   */
  public MockConnectionResources withSchema(Schema schema) {
    this.schema = schema;
    return this;
  }


  /**
   * Allow specification of the results of a query.
   *
   * @param query SQL
   * @param result Results.
   * @return this.
   */
  public MockConnectionResources withResultSet(String query, ResultSet resultSet) {
    resultSets.put(query, resultSet);
    return this;
  }


  /**
   * @return A mock of {@link ConnectionResources} for upgrade testing.
   */
  public ConnectionResources create() {
    try {
      if (dialect == null) {
        dialect = spy(new MockDialect());
      }

      ConnectionResources mockConnectionResources = mock(ConnectionResources.class, RETURNS_SMART_NULLS);
      when(mockConnectionResources.sqlDialect()).thenReturn(dialect);

      DataSource dataSource = mock(DataSource.class, RETURNS_SMART_NULLS);
      when(mockConnectionResources.getDataSource()).thenReturn(dataSource);

      if (schema != null) {
        when(mockConnectionResources.openSchemaResource()).thenReturn(new StubSchemaResource(schema));
        when(mockConnectionResources.openSchemaResource(same(dataSource))).thenReturn(new StubSchemaResource(schema));
      }

      Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
      when(dataSource.getConnection()).thenReturn(connection);

      java.sql.Statement statement = mock(java.sql.Statement.class, RETURNS_SMART_NULLS);
      when(connection.createStatement()).thenReturn(statement);
      
      for (String query : resultSets.keySet()) {
        ResultSet resultSet = resultSets.get(query);
        
        when(statement.executeQuery(StringUtils.isEmpty(query) ? anyString() : eq(query))).thenReturn(resultSet);
      }

      return mockConnectionResources;
    } catch(SQLException sqle) {
      throw new RuntimeSqlException("err", sqle);
    }
  }


  /**
   * An implementation of {@link SchemaResource}.
   *
   * @author Copyright (c) Alfa Financial Software 2012
   */
  static final class StubSchemaResource extends SchemaAdapter implements SchemaResource {

    /**
     * @param delegate
     */
    public StubSchemaResource(Schema delegate) {
      super(delegate);
    }


    /**
     * @see org.alfasoftware.morf.metadata.SchemaResource#close()
     */
    @Override
    public void close() {
      // No-op
    }
  }
}
