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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

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
 * Unit tests for {@link DeferredIndexExecutor} covering edge cases
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
    DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.shutdown();
  }


  /** Calling shutdown after executeAndWait should be idempotent. */
  @Test
  public void testShutdownAfterNonEmptyExecution() {
    DeferredIndexOperation op = buildOp("op1");
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.executeAndWait(60_000L);
    executor.shutdown();
  }


  /** logProgress should run without error when no operations have been submitted. */
  @Test
  public void testLogProgressOnFreshExecutor() {
    DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.logProgress();
  }


  /** logProgress should report accurate counters after a completed execution run. */
  @Test
  public void testLogProgressAfterExecution() {
    DeferredIndexOperation op = buildOp("op1");
    when(dao.findPendingOperations()).thenReturn(List.of(op));
    SqlScriptExecutor scriptExecutor = mock(SqlScriptExecutor.class);
    when(sqlScriptExecutorProvider.get()).thenReturn(scriptExecutor);
    when(sqlDialect.deferredIndexDeploymentStatements(any(Table.class), any(Index.class)))
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));

    DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    executor.executeAndWait(60_000L);
    executor.logProgress();

    DeferredIndexExecutor.ExecutionStatus status = executor.getStatus();
    assertEquals("totalCount", 1, status.getTotalCount());
    assertEquals("completedCount", 1, status.getCompletedCount());
  }


  /** truncate should return an empty string when the input is null. */
  @Test
  public void testTruncateReturnsEmptyForNull() {
    assertEquals("", DeferredIndexExecutor.truncate(null, 100));
  }


  /** truncate should return the original string when it is within the limit. */
  @Test
  public void testTruncateReturnsOriginalWhenWithinLimit() {
    assertEquals("short", DeferredIndexExecutor.truncate("short", 100));
  }


  /** truncate should cut the string at maxLength when it exceeds the limit. */
  @Test
  public void testTruncateCutsAtMaxLength() {
    assertEquals("abcdefghij", DeferredIndexExecutor.truncate("abcdefghij-extra", 10));
  }


  /** awaitCompletion should return false and restore the interrupt flag when the waiting thread is interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws Exception {
    when(dao.hasNonTerminalOperations()).thenReturn(true);

    DeferredIndexExecutor executor = new DeferredIndexExecutor(dao, sqlDialect, sqlScriptExecutorProvider, dataSource, config);
    AtomicBoolean result = new AtomicBoolean(true);
    Thread testThread = new Thread(() -> result.set(executor.awaitCompletion(60L)));
    testThread.start();
    Thread.sleep(200);
    testThread.interrupt();
    testThread.join(5_000L);

    assertFalse("Should return false when interrupted", result.get());
  }


  private DeferredIndexOperation buildOp(String operationId) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setOperationId(operationId);
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
