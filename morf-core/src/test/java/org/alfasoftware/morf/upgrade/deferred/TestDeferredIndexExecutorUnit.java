/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link DeferredIndexExecutorImpl} covering the
 * comments-based schema scanning approach: the executor finds indexes
 * with {@code isDeferred()=true} in the SchemaResource and builds them.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexExecutorUnit {

  @Mock private ConnectionResources connectionResources;
  @Mock private SqlDialect sqlDialect;
  @Mock private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Mock private DataSource dataSource;
  @Mock private Connection connection;
  @Mock private java.sql.DatabaseMetaData databaseMetaData;

  private UpgradeConfigAndContext config;
  private AutoCloseable mocks;


  /** Set up mocks and config before each test. */
  @Before
  public void setUp() throws SQLException {
    mocks = MockitoAnnotations.openMocks(this);
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);
    when(connectionResources.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    // Default: no physical indexes exist (empty result set for getIndexInfo)
    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(databaseMetaData.getIndexInfo(any(), any(), any(), anyBoolean(), anyBoolean())).thenReturn(emptyRs);
  }


  /** Close mocks after each test. */
  @After
  public void tearDown() throws Exception {
    mocks.close();
  }


  /** execute with no deferred indexes should return an already-completed future. */
  @Test
  public void testExecuteNoDeferredIndexes() {
    // given -- schema with no deferred indexes
    SchemaResource sr = mockSchemaResource();
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    CompletableFuture<Void> future = executor.execute();

    // then
    assertTrue("Future should be completed immediately", future.isDone());
  }


  /** execute with a single deferred index should build it. */
  @Test
  public void testExecuteSingleDeferredIndex() throws SQLException {
    // given -- one deferred index on TestTable
    mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIdx ON TestTable(col1, col2)"));

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute().join();

    // then
    verify(sqlDialect).deferredIndexDeploymentStatements(any(Table.class), any(Index.class));
    verify(scriptExecutor).execute(any(Collection.class), any(Connection.class));
  }


  /** execute should skip non-deferred indexes. */
  @Test
  public void testExecuteSkipsNonDeferredIndexes() {
    // given -- table with only non-deferred indexes
    Table tableWithNonDeferred = table("TestTable")
        .columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("col1", DataType.STRING, 50)
        )
        .indexes(index("TestIdx").columns("id", "col1"));
    SchemaResource sr = mockSchemaResource(tableWithNonDeferred);
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    CompletableFuture<Void> future = executor.execute();

    // then
    assertTrue("Future should be completed immediately (no deferred indexes)", future.isDone());
    verify(sqlDialect, never()).deferredIndexDeploymentStatements(any(Table.class), any(Index.class));
  }


  /** execute should retry on failure and succeed on a subsequent attempt. */
  @SuppressWarnings("unchecked")
  @Test
  public void testExecuteRetryThenSuccess() {
    // given -- deferred index that fails on first attempt, succeeds on second
    config.setDeferredIndexMaxRetries(2);
    mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("temporary failure"))
        .thenReturn(List.of("CREATE INDEX TestIdx ON TestTable(col1, col2)"));

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute().join();

    // then
    verify(scriptExecutor).execute(any(Collection.class), any(Connection.class));
  }


  /** execute should give up after exhausting retries. */
  @Test
  public void testExecutePermanentFailure() {
    // given -- deferred index that always fails
    config.setDeferredIndexMaxRetries(1);
    SchemaResource sr = mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    when(connectionResources.openSchemaResource()).thenReturn(sr);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("persistent failure"));

    // when -- should not throw; the future completes and the error is logged
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute().join();
  }


  /** execute should handle a SQLException from getConnection as a failure. */
  @Test
  public void testExecuteSqlExceptionFromConnection() throws SQLException {
    // given -- deferred index exists but connection fails during build
    config.setDeferredIndexMaxRetries(0);
    SchemaResource sr = mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    when(connectionResources.openSchemaResource()).thenReturn(sr);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIdx ON TestTable(col1, col2)"));
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute().join();
  }


  /** buildIndex should restore autocommit to its original value after execution. */
  @Test
  public void testAutoCommitRestoredAfterBuildIndex() throws SQLException {
    // given -- connection with autocommit=false and a deferred index to build
    when(connection.getAutoCommit()).thenReturn(false);
    mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIdx ON TestTable(col1, col2)"));

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute().join();

    // then -- autocommit was set to true for the build, then restored
    verify(connection, org.mockito.Mockito.atLeastOnce()).setAutoCommit(true);
    verify(connection).setAutoCommit(false);
  }


  /** execute should be a no-op when deferred index creation is disabled. */
  @Test
  public void testExecuteDisabled() {
    // given
    config.setDeferredIndexCreationEnabled(false);

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    CompletableFuture<Void> future = executor.execute();

    // then
    assertTrue("Future should be completed immediately", future.isDone());
    verify(connectionResources, never()).openSchemaResource();
  }


  // -------------------------------------------------------------------------
  // getMissingDeferredIndexStatements
  // -------------------------------------------------------------------------

  /** getMissingDeferredIndexStatements should return SQL for unbuilt deferred indexes. */
  @Test
  public void testGetMissingDeferredIndexStatements() {
    // given
    mockSchemaResourceWithDeferredIndex("TestTable", "TestIdx", "col1", "col2");
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIdx ON TestTable(col1, col2)"));

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    List<String> statements = executor.getMissingDeferredIndexStatements();

    // then
    assertEquals(1, statements.size());
    assertEquals("CREATE INDEX TestIdx ON TestTable(col1, col2)", statements.get(0));
  }


  /** getMissingDeferredIndexStatements should return empty when disabled. */
  @Test
  public void testGetMissingDeferredIndexStatementsDisabled() {
    // given
    config.setDeferredIndexCreationEnabled(false);

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    List<String> statements = executor.getMissingDeferredIndexStatements();

    // then
    assertTrue("Should return empty list when disabled", statements.isEmpty());
  }


  /** getMissingDeferredIndexStatements should return empty when no deferred indexes. */
  @Test
  public void testGetMissingDeferredIndexStatementsNone() {
    // given
    SchemaResource sr = mockSchemaResource();
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    // when
    DeferredIndexExecutorImpl executor = createExecutor();
    List<String> statements = executor.getMissingDeferredIndexStatements();

    // then
    assertTrue("Should return empty list", statements.isEmpty());
  }


  // -------------------------------------------------------------------------
  // Config validation
  // -------------------------------------------------------------------------

  /** threadPoolSize less than 1 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadPoolSize() {
    // given
    config.setDeferredIndexThreadPoolSize(0);
    SchemaResource sr = mockSchemaResourceWithDeferredIndex("T", "Idx", "c1", "c2");
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    // when -- should throw
    createExecutor().execute();
  }


  /** maxRetries less than 0 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMaxRetries() {
    // given
    config.setDeferredIndexMaxRetries(-1);
    SchemaResource sr = mockSchemaResourceWithDeferredIndex("T", "Idx", "c1", "c2");
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    // when -- should throw
    createExecutor().execute();
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexExecutorImpl createExecutor() {
    return new DeferredIndexExecutorImpl(connectionResources, sqlScriptExecutorProvider,
        config, new DeferredIndexExecutorServiceFactory.Default());
  }


  /**
   * Creates a mock SchemaResource with no tables.
   */
  private SchemaResource mockSchemaResource() {
    SchemaResource sr = mock(SchemaResource.class);
    when(sr.tables()).thenReturn(Collections.emptyList());
    when(sr.tableNames()).thenReturn(Collections.emptySet());
    return sr;
  }


  /**
   * Creates a mock SchemaResource with a single table containing only
   * non-deferred indexes.
   */
  private SchemaResource mockSchemaResource(Table table) {
    SchemaResource sr = mock(SchemaResource.class);
    when(sr.tables()).thenReturn(List.of(table));
    when(sr.tableNames()).thenReturn(Set.of(table.getName()));
    when(sr.tableExists(table.getName())).thenReturn(true);
    when(sr.getTable(table.getName())).thenReturn(table);
    return sr;
  }


  /**
   * Creates a mock SchemaResource with a single table that has one
   * deferred index. The deferred index has {@code isDeferred()=true}.
   * The table name lookups are set up for the targeted scan.
   */
  private SchemaResource mockSchemaResourceWithDeferredIndex(String tableName, String indexName,
                                                              String col1, String col2) {
    Index deferredIndex = mock(Index.class);
    when(deferredIndex.getName()).thenReturn(indexName);
    when(deferredIndex.isDeferred()).thenReturn(true);
    when(deferredIndex.isUnique()).thenReturn(false);
    when(deferredIndex.columnNames()).thenReturn(List.of(col1, col2));

    Table table = mock(Table.class);
    when(table.getName()).thenReturn(tableName);
    when(table.indexes()).thenReturn(List.of(deferredIndex));

    SchemaResource sr = mock(SchemaResource.class);
    when(sr.tables()).thenReturn(List.of(table));
    when(sr.tableNames()).thenReturn(Set.of(tableName));
    when(sr.tableExists(tableName)).thenReturn(true);
    when(sr.getTable(tableName)).thenReturn(table);

    // indexExistsPhysically now uses JDBC DatabaseMetaData (not SchemaResource),
    // and the default mock returns an empty ResultSet (no physical indexes).
    when(connectionResources.openSchemaResource()).thenReturn(sr);

    return sr;
  }
}
