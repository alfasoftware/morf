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

package org.alfasoftware.morf.jdbc;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.MockRecord;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.inject.util.Providers;

/**
 * Tests for {@link DatabaseDataSetConsumer}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestDatabaseDataSetConsumer {

  /**
   * Tests that blob fields are added to the the prepared insert statement in correct decoded form.
   *
   * @throws SQLException not really thrown
   */
  @Test
  public void testBlobInsertion() throws SQLException {

    // Mock all the resources - we're only interested in the PreparedStatement
    // for this test.
    final ConnectionResources connectionResources = Mockito.mock(ConnectionResources.class);
    final DataSource dataSource = Mockito.mock(DataSource.class);
    final Connection connection = Mockito.mock(Connection.class);
    final SqlDialect dialect = Mockito.mock(SqlDialect.class);
    final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    Mockito.when(connectionResources.getDataSource()).thenReturn(dataSource);
    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connectionResources.sqlDialect()).thenReturn(dialect);
    Mockito.when(dialect.convertStatementToSQL(Mockito.any(InsertStatement.class), Mockito.any(Schema.class))).thenReturn("");
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);

    // Create our consumer
    final DatabaseDataSetConsumer consumer = new DatabaseDataSetConsumer(connectionResources, new SqlScriptExecutorProvider(dataSource, Providers.of(dialect)));
    consumer.open();

    // Create a mock schema and records
    final Table table = table("DatabaseTest")
      .columns(
        idColumn(),
        versionColumn(),
        column("blob", DataType.BLOB)
      );
    final List<Record> records = new ArrayList<>();
    records.add(new MockRecord(table,"1","2","QUJD"));

    // consume the records
    consumer.table(table, records);

    // Verify dialect is requested to write the values to the statement

    ArgumentCaptor<SqlParameter> parameterCaptor = ArgumentCaptor.forClass(SqlParameter.class);
    Mockito.verify(dialect).prepareStatementParameter(any(NamedParameterPreparedStatement.class), parameterCaptor.capture(), eq("1"));
    assertEquals("Name of parameter 0", idColumn().getName(), parameterCaptor.getValue().getImpliedName());

    parameterCaptor = ArgumentCaptor.forClass(SqlParameter.class);
    Mockito.verify(dialect).prepareStatementParameter(any(NamedParameterPreparedStatement.class), parameterCaptor.capture(), eq("2"));
    assertEquals("Name of parameter 1", versionColumn().getName(), parameterCaptor.getValue().getImpliedName());

    parameterCaptor = ArgumentCaptor.forClass(SqlParameter.class);
    Mockito.verify(dialect).prepareStatementParameter(any(NamedParameterPreparedStatement.class), parameterCaptor.capture(), eq("QUJD"));
    assertEquals("Name of parameter 2", "blob", parameterCaptor.getValue().getImpliedName());
  }


  /**
   * Check original autocommit value is returned to the connection.
   *
   * @throws SQLException exception
   */
  @Test
  public void testAutoCommitFlag() throws SQLException {
    ConnectionResources connectionResources = mock(ConnectionResources.class, Mockito.RETURNS_SMART_NULLS);
    SqlScriptExecutorProvider sqlScriptExecutorProvider = mock(SqlScriptExecutorProvider.class, Mockito.RETURNS_SMART_NULLS);
    DataSource dataSource = mock(DataSource.class, Mockito.RETURNS_SMART_NULLS);
    Connection connection = mock(Connection.class, Mockito.RETURNS_SMART_NULLS);
    SqlScriptExecutor sqlScriptExecutor = mock(SqlScriptExecutor.class, Mockito.RETURNS_SMART_NULLS);

    when(dataSource.getConnection()).thenReturn(connection);
    when(connectionResources.sqlDialect()).thenReturn(mock(SqlDialect.class));
    when(sqlScriptExecutorProvider.get()).thenReturn(sqlScriptExecutor);

    when(connection.getAutoCommit()).thenReturn(true);

    DatabaseDataSetConsumer consumer = new DatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider, dataSource);
    consumer.open();
    consumer.close(CloseState.COMPLETE);

    verify(connection).commit();
    verify(connection).close();
    verify(connection).setAutoCommit(false);
    verify(connection).setAutoCommit(true);
  }
}
