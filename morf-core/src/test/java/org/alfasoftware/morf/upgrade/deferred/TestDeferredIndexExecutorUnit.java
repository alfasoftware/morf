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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link DeferredIndexExecutorImpl} covering progress tracking,
 * retry behaviour, and connection handling.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexExecutorUnit {

  @Mock private ConnectionResources connectionResources;
  @Mock private SqlDialect sqlDialect;
  @Mock private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Mock private DataSource dataSource;
  @Mock private Connection connection;

  private DeferredIndexExecutionConfig config;
  private AutoCloseable mocks;


  /** Set up mocks and a fast-retry config before each test. */
  @Before
  public void setUp() throws SQLException {
    mocks = MockitoAnnotations.openMocks(this);
    config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);
    when(connectionResources.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);

    // Default: openSchemaResource returns a mock that says table does not exist
    // (post-failure index-exists check will return false)
    SchemaResource mockSr = mock(SchemaResource.class);
    when(mockSr.tableExists(anyString())).thenReturn(false);
    when(connectionResources.openSchemaResource()).thenReturn(mockSr);
  }


  /** Close mocks after each test. */
  @After
  public void tearDown() throws Exception {
    mocks.close();
  }


  /** logProgress should run without error when no operations have been submitted. */
  @Test
  public void testLogProgressOnFreshExecutor() {
    DeferredIndexExecutorImpl executor = createExecutor();
    executor.logProgress();

    assertEquals(0, executor.getCompletedCount());
    assertEquals(0, executor.getFailedCount());
    assertEquals(0, executor.getTotalCount());
  }


  /** Executing with an empty list should return an already-completed future. */
  @Test
  public void testEmptyListReturnsImmediately() {
    DeferredIndexExecutorImpl executor = createExecutor();
    CompletableFuture<Void> future = executor.execute(Collections.emptyList());

    assertTrue("Future should be completed immediately", future.isDone());
    assertEquals(0, executor.getCompletedCount());
    assertEquals(0, executor.getFailedCount());
  }


  /** A single index should be built via the dialect and marked as completed. */
  @SuppressWarnings("unchecked")
  @Test
  public void testBuildsSingleIndex() {
    DeferredAddIndex dai = buildDeferredAddIndex("TestIndex", false);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIndex ON TestTable(col1)"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    verify(scriptExecutor).execute(any(Collection.class), any(Connection.class));
    assertEquals(1, executor.getCompletedCount());
    assertEquals(0, executor.getFailedCount());
    assertEquals(1, executor.getTotalCount());
  }


  /** A transient failure should be retried and succeed on the next attempt. */
  @SuppressWarnings("unchecked")
  @Test
  public void testRetryOnFailure() {
    config.setMaxRetries(2);
    config.setRetryBaseDelayMs(1L);
    config.setRetryMaxDelayMs(1L);

    DeferredAddIndex dai = buildDeferredAddIndex("TestIndex", false);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);

    // First call throws, second call succeeds
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("temporary failure"))
        .thenReturn(List.of("CREATE INDEX TestIndex ON TestTable(col1)"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    assertEquals(1, executor.getCompletedCount());
    assertEquals(0, executor.getFailedCount());
  }


  /** An index that fails on every attempt should be counted as failed. */
  @Test
  public void testFailedAfterMaxRetriesWithNoRetries() {
    config.setMaxRetries(0);
    config.setRetryBaseDelayMs(1L);
    config.setRetryMaxDelayMs(1L);

    DeferredAddIndex dai = buildDeferredAddIndex("TestIndex", false);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);

    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("persistent failure"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    assertEquals(0, executor.getCompletedCount());
    assertEquals(1, executor.getFailedCount());
  }


  /** A unique index should be built via the dialect and marked as completed. */
  @SuppressWarnings("unchecked")
  @Test
  public void testUniqueIndexCreated() {
    DeferredAddIndex dai = buildDeferredAddIndex("TestUniqueIndex", true);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE UNIQUE INDEX TestUniqueIndex ON TestTable(col1)"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    verify(scriptExecutor).execute(any(Collection.class), any(Connection.class));
    assertEquals(1, executor.getCompletedCount());
    assertEquals(0, executor.getFailedCount());
  }


  /** A SQLException from getConnection should result in a failed index. */
  @Test
  public void testExecuteSqlExceptionFromConnection() throws SQLException {
    config.setMaxRetries(0);
    DeferredAddIndex dai = buildDeferredAddIndex("TestIndex", false);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIndex ON TestTable(col1)"));
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    assertEquals(0, executor.getCompletedCount());
    assertEquals(1, executor.getFailedCount());
  }


  /** buildIndex should restore autocommit to its original value after execution. */
  @SuppressWarnings("unchecked")
  @Test
  public void testAutoCommitRestoredAfterBuildIndex() throws SQLException {
    when(connection.getAutoCommit()).thenReturn(false);
    DeferredAddIndex dai = buildDeferredAddIndex("TestIndex", false);
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX TestIndex ON TestTable(col1)"));

    DeferredIndexExecutorImpl executor = createExecutor();
    executor.execute(List.of(dai)).join();

    InOrder order = inOrder(connection);
    order.verify(connection).setAutoCommit(true);
    order.verify(connection).setAutoCommit(false);
  }


  /**
   * Creates a {@link DeferredIndexExecutorImpl} with the current test mocks and config.
   */
  private DeferredIndexExecutorImpl createExecutor() {
    return new DeferredIndexExecutorImpl(connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
  }


  /**
   * Builds a {@link DeferredAddIndex} for the given index name and uniqueness flag.
   */
  private DeferredAddIndex buildDeferredAddIndex(String indexName, boolean unique) {
    Index idx = unique
        ? index(indexName).columns("col1").unique()
        : index(indexName).columns("col1");
    return new DeferredAddIndex("TestTable", idx, "test-uuid");
  }
}
