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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link DeferredIndexExecutorImpl} covering edge cases
 * that are difficult to exercise in integration tests: shutdown lifecycle,
 * progress logging, string truncation, and async execution behaviour.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexExecutorUnit {

  @Mock private DeferredIndexOperationDAO dao;
  @Mock private ConnectionResources connectionResources;
  @Mock private SqlDialect sqlDialect;
  @Mock private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Mock private DataSource dataSource;
  @Mock private Connection connection;

  private DeferredIndexConfig config;


  /** Set up mocks and a fast-retry config before each test. */
  @Before
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10L);
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);
    when(connectionResources.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
  }


  /** Calling shutdown before any execution should be a safe no-op. */
  @Test
  public void testShutdownBeforeExecutionIsNoOp() {
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.shutdown();
  }


  /** Calling shutdown after execute should be idempotent. */
  @Test
  public void testShutdownAfterNonEmptyExecution() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();
    executor.shutdown();
  }


  /** logProgress should run without error when no operations have been submitted. */
  @Test
  public void testLogProgressOnFreshExecutor() {
    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.logProgress();
  }


  /** truncate should return an empty string when the input is null. */
  @Test
  public void testTruncateReturnsEmptyForNull() {
    assertEquals("", DeferredIndexExecutorImpl.truncate(null, 100));
  }


  /** truncate should return the original string when it is within the limit. */
  @Test
  public void testTruncateReturnsOriginalWhenWithinLimit() {
    assertEquals("short", DeferredIndexExecutorImpl.truncate("short", 100));
  }


  /** truncate should cut the string at maxLength when it exceeds the limit. */
  @Test
  public void testTruncateCutsAtMaxLength() {
    assertEquals("abcdefghij", DeferredIndexExecutorImpl.truncate("abcdefghij-extra", 10));
  }


  /** execute with an empty pending queue should return an already-completed future. */
  @Test
  public void testExecuteEmptyQueue() {
    when(dao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    CompletableFuture<Void> future = executor.execute();

    assertTrue("Future should be completed immediately", future.isDone());
    verify(dao, never()).markStarted(any(Long.class), any(Long.class));
  }


  /** execute with a single successful operation should mark it completed. */
  @Test
  public void testExecuteSingleSuccess() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    verify(dao).markCompleted(eq(1001L), any(Long.class));
  }


  /** execute should retry on failure and succeed on a subsequent attempt. */
  @SuppressWarnings("unchecked")
  @Test
  public void testExecuteRetryThenSuccess() {
    config.setMaxRetries(2);
    config.setRetryBaseDelayMs(1L);
    config.setRetryMaxDelayMs(1L);

    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);

    // First call throws, second call succeeds
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("temporary failure"))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    verify(dao).markCompleted(eq(1001L), any(Long.class));
  }


  /** execute should mark an operation as permanently failed after exhausting retries. */
  @Test
  public void testExecutePermanentFailure() {
    config.setMaxRetries(1);
    config.setRetryBaseDelayMs(1L);
    config.setRetryMaxDelayMs(1L);

    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);

    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("persistent failure"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    // Should be called twice (initial + 1 retry), each time with markFailed
    verify(dao, org.mockito.Mockito.times(2)).markFailed(eq(1001L), any(String.class), any(Integer.class));
  }


  /** execute should correctly reconstruct and build a unique index. */
  @Test
  public void testExecuteWithUniqueIndex() {
    DeferredIndexOperation op = buildOp(1001L);
    op.setIndexUnique(true);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE UNIQUE INDEX idx ON t(c)"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    verify(dao).markCompleted(eq(1001L), any(Long.class));
  }


  /** execute should handle a SQLException from getConnection as a failure. */
  @Test
  public void testExecuteSqlExceptionFromConnection() throws SQLException {
    config.setMaxRetries(0);
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, connectionResources, sqlScriptExecutorProvider, config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();

    verify(dao).markFailed(eq(1001L), any(String.class), eq(1));
  }


  private DeferredIndexOperation buildOp(long id) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(id);
    op.setUpgradeUUID("test-uuid");
    op.setTableName("TestTable");
    op.setIndexName("TestIndex");
    op.setOperationType(DeferredIndexOperationType.ADD);
    op.setIndexUnique(false);
    op.setStatus(DeferredIndexStatus.PENDING);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setColumnNames(List.of("col1"));
    return op;
  }
}
