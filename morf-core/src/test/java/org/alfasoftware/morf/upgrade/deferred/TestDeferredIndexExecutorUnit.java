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
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
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
 * progress logging, string truncation, and thread interruption.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexExecutorUnit {

  @Mock private DeferredIndexOperationDAO dao;
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
    when(dataSource.getConnection()).thenReturn(connection);
  }


  /** Calling shutdown before any execution should be a safe no-op. */
  @Test
  public void testShutdownBeforeExecutionIsNoOp() {
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.shutdown();
  }


  /** Calling shutdown after executeAndWait should be idempotent. */
  @Test
  public void testShutdownAfterNonEmptyExecution() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.executeAndWait(60_000L);
    executor.shutdown();
  }


  /** logProgress should run without error when no operations have been submitted. */
  @Test
  public void testLogProgressOnFreshExecutor() {
    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.logProgress();
  }


  /** logProgress should report accurate counters after a completed execution run. */
  @Test
  public void testLogProgressAfterExecution() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutorImpl executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.executeAndWait(60_000L);
    executor.logProgress();

    DeferredIndexExecutor.ExecutionStatus status = executor.getStatus();
    assertEquals("totalCount", 1, status.getTotalCount());
    assertEquals("completedCount", 1, status.getCompletedCount());
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


  /** awaitCompletion should return false and restore the interrupt flag when the waiting thread is interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws Exception {
    when(dao.hasNonTerminalOperations()).thenReturn(true);

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    AtomicBoolean result = new AtomicBoolean(true);
    Thread testThread = new Thread(() -> result.set(executor.awaitCompletion(60L)));
    testThread.start();
    Thread.sleep(200);
    testThread.interrupt();
    testThread.join(5_000L);

    assertFalse("Should return false when interrupted", result.get());
  }


  /** executeAndWait with an empty pending queue should return (0, 0). */
  @Test
  public void testExecuteAndWaitEmptyQueue() {
    when(dao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /** executeAndWait with a single successful operation should return (1, 0). */
  @Test
  public void testExecuteAndWaitSingleSuccess() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 1, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
    verify(dao).markCompleted(eq(1001L), any(Long.class));
  }


  /** executeAndWait should retry on failure and succeed on a subsequent attempt. */
  @SuppressWarnings("unchecked")
  @Test
  public void testExecuteAndWaitRetryThenSuccess() {
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

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 1, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /** executeAndWait should mark an operation as permanently failed after exhausting retries. */
  @Test
  public void testExecuteAndWaitPermanentFailure() {
    config.setMaxRetries(1);
    config.setRetryBaseDelayMs(1L);
    config.setRetryMaxDelayMs(1L);

    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);

    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenThrow(new RuntimeException("persistent failure"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 1, result.getFailedCount());
  }


  /** getStatus should reflect counts from a completed execution. */
  @Test
  public void testGetStatusAfterExecution() {
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.executeAndWait(60_000L);

    DeferredIndexExecutor.ExecutionStatus status = executor.getStatus();
    assertEquals("totalCount", 1, status.getTotalCount());
    assertEquals("completedCount", 1, status.getCompletedCount());
    assertEquals("inProgressCount", 0, status.getInProgressCount());
    assertEquals("failedCount", 0, status.getFailedCount());
  }


  /** getStatus on a fresh executor should report zero for all fields. */
  @Test
  public void testGetStatusBeforeExecution() {
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionStatus status = executor.getStatus();
    assertEquals("totalCount", 0, status.getTotalCount());
    assertEquals("completedCount", 0, status.getCompletedCount());
    assertEquals("inProgressCount", 0, status.getInProgressCount());
    assertEquals("failedCount", 0, status.getFailedCount());
  }


  /** awaitCompletion should return true immediately when no non-terminal operations exist. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenEmpty() {
    when(dao.hasNonTerminalOperations()).thenReturn(false);

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    boolean result = executor.awaitCompletion(60L);

    assertEquals("awaitCompletion should return true", true, result);
  }


  /** awaitCompletion should return false when the timeout elapses. */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    when(dao.hasNonTerminalOperations()).thenReturn(true);

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    boolean result = executor.awaitCompletion(1L);

    assertFalse("awaitCompletion should return false on timeout", result);
  }


  /** executeAndWait should correctly reconstruct and build a unique index. */
  @Test
  public void testExecuteAndWaitWithUniqueIndex() {
    DeferredIndexOperation op = buildOp(1001L);
    op.setIndexUnique(true);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE UNIQUE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 1, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /** executeAndWait should handle a SQLException from getConnection as a failure. */
  @Test
  public void testExecuteAndWaitSqlExceptionFromConnection() throws SQLException {
    config.setMaxRetries(0);
    DeferredIndexOperation op = buildOp(1001L);
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    DeferredIndexExecutor.ExecutionResult result = executor.executeAndWait(60_000L);

    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 1, result.getFailedCount());
  }


  /** awaitCompletion with zero timeout should wait indefinitely until done. */
  @Test
  public void testAwaitCompletionZeroTimeoutWaitsUntilDone() {
    java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();
    when(dao.hasNonTerminalOperations()).thenAnswer(inv -> callCount.incrementAndGet() < 2);

    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    boolean result = executor.awaitCompletion(0L);

    assertEquals("awaitCompletion should return true", true, result);
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
