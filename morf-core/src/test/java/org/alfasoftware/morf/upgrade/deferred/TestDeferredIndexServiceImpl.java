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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexServiceImpl} covering config validation,
 * the {@link DeferredIndexService.ExecutionResult} value type, and the
 * {@code execute()} / {@code awaitCompletion()} orchestration logic.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexServiceImpl {

  // -------------------------------------------------------------------------
  // Config validation
  // -------------------------------------------------------------------------

  /** Construction with valid default config should succeed. */
  @Test
  public void testConstructionWithDefaultConfig() {
    new DeferredIndexServiceImpl(null, new DeferredIndexConfig());
  }


  /** threadPoolSize less than 1 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadPoolSize() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setThreadPoolSize(0);
    new DeferredIndexServiceImpl(null, config);
  }


  /** maxRetries less than 0 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMaxRetries() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setMaxRetries(-1);
    new DeferredIndexServiceImpl(null, config);
  }


  /** retryBaseDelayMs less than 0 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryBaseDelayMs() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(-1L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** retryMaxDelayMs less than retryBaseDelayMs should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryMaxDelayMs() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setRetryBaseDelayMs(10_000L);
    config.setRetryMaxDelayMs(5_000L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** staleThresholdSeconds of 0 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidStaleThresholdSeconds() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setStaleThresholdSeconds(0L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** operationTimeoutSeconds of 0 should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOperationTimeoutSeconds() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setOperationTimeoutSeconds(0L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** Validate the error message when threadPoolSize is invalid. */
  @Test
  public void testInvalidThreadPoolSizeMessage() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setThreadPoolSize(0);
    try {
      new DeferredIndexServiceImpl(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue("Message should mention threadPoolSize", e.getMessage().contains("threadPoolSize"));
    }
  }


  /** Config validation should accept edge-case valid values. */
  @Test
  public void testEdgeCaseValidConfig() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setThreadPoolSize(1);
    config.setMaxRetries(0);
    config.setRetryBaseDelayMs(0L);
    config.setRetryMaxDelayMs(0L);
    config.setStaleThresholdSeconds(1L);
    config.setOperationTimeoutSeconds(1L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** Negative staleThresholdSeconds should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeStaleThresholdSeconds() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setStaleThresholdSeconds(-5L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** Negative operationTimeoutSeconds should be rejected. */
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeOperationTimeoutSeconds() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    config.setOperationTimeoutSeconds(-1L);
    new DeferredIndexServiceImpl(null, config);
  }


  /** Default config should pass all validation checks. */
  @Test
  public void testDefaultConfigPassesAllValidation() {
    DeferredIndexConfig config = new DeferredIndexConfig();
    assertFalse("Default maxRetries should be >= 0", config.getMaxRetries() < 0);
    assertTrue("Default threadPoolSize should be >= 1", config.getThreadPoolSize() >= 1);
    assertTrue("Default staleThresholdSeconds should be > 0", config.getStaleThresholdSeconds() > 0);
    assertTrue("Default operationTimeoutSeconds should be > 0", config.getOperationTimeoutSeconds() > 0);
    assertTrue("Default retryBaseDelayMs should be >= 0", config.getRetryBaseDelayMs() >= 0);
    assertTrue("Default retryMaxDelayMs >= retryBaseDelayMs",
        config.getRetryMaxDelayMs() >= config.getRetryBaseDelayMs());
  }


  // -------------------------------------------------------------------------
  // ExecutionResult
  // -------------------------------------------------------------------------

  /** ExecutionResult should faithfully report completed and failed counts. */
  @Test
  public void testExecutionResultCounts() {
    DeferredIndexService.ExecutionResult result = new DeferredIndexService.ExecutionResult(5, 2);
    assertEquals("completedCount", 5, result.getCompletedCount());
    assertEquals("failedCount", 2, result.getFailedCount());
  }


  /** ExecutionResult with zero counts should work correctly. */
  @Test
  public void testExecutionResultZeroCounts() {
    DeferredIndexService.ExecutionResult result = new DeferredIndexService.ExecutionResult(0, 0);
    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  // -------------------------------------------------------------------------
  // execute() orchestration
  // -------------------------------------------------------------------------

  /** execute() should call recovery then executor and return success result. */
  @Test
  public void testExecuteSuccessfulRun() {
    DeferredIndexRecoveryService mockRecovery = mock(DeferredIndexRecoveryService.class);
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.executeAndWait(14_400_000L))
        .thenReturn(new DeferredIndexExecutor.ExecutionResult(3, 0));

    DeferredIndexServiceImpl service = serviceWithMocks(mockRecovery, mockExecutor, null);
    DeferredIndexService.ExecutionResult result = service.execute();

    verify(mockRecovery).recoverStaleOperations();
    verify(mockExecutor).executeAndWait(14_400_000L);
    assertEquals("completedCount", 3, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /** execute() should throw IllegalStateException when any operations fail. */
  @Test(expected = IllegalStateException.class)
  public void testExecuteThrowsOnFailure() {
    DeferredIndexRecoveryService mockRecovery = mock(DeferredIndexRecoveryService.class);
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.executeAndWait(14_400_000L))
        .thenReturn(new DeferredIndexExecutor.ExecutionResult(2, 1));

    DeferredIndexServiceImpl service = serviceWithMocks(mockRecovery, mockExecutor, null);
    service.execute();
  }


  /** execute() with zero pending operations should return zero counts. */
  @Test
  public void testExecuteWithNoPendingOperations() {
    DeferredIndexRecoveryService mockRecovery = mock(DeferredIndexRecoveryService.class);
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.executeAndWait(14_400_000L))
        .thenReturn(new DeferredIndexExecutor.ExecutionResult(0, 0));

    DeferredIndexServiceImpl service = serviceWithMocks(mockRecovery, mockExecutor, null);
    DeferredIndexService.ExecutionResult result = service.execute();

    assertEquals("completedCount", 0, result.getCompletedCount());
    assertEquals("failedCount", 0, result.getFailedCount());
  }


  /** execute() should propagate exceptions from recovery service. */
  @Test(expected = RuntimeException.class)
  public void testExecutePropagatesRecoveryException() {
    DeferredIndexRecoveryService mockRecovery = mock(DeferredIndexRecoveryService.class);
    doThrow(new RuntimeException("recovery failed")).when(mockRecovery).recoverStaleOperations();

    DeferredIndexServiceImpl service = serviceWithMocks(mockRecovery, null, null);
    service.execute();
  }


  /** The failure exception message should include the failed count. */
  @Test
  public void testExecuteFailureMessageIncludesCount() {
    DeferredIndexRecoveryService mockRecovery = mock(DeferredIndexRecoveryService.class);
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.executeAndWait(14_400_000L))
        .thenReturn(new DeferredIndexExecutor.ExecutionResult(5, 3));

    DeferredIndexServiceImpl service = serviceWithMocks(mockRecovery, mockExecutor, null);
    try {
      service.execute();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should include count", e.getMessage().contains("3"));
    }
  }


  // -------------------------------------------------------------------------
  // awaitCompletion() orchestration
  // -------------------------------------------------------------------------

  /** awaitCompletion() should return true immediately when no non-terminal operations exist. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenAllDone() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.hasNonTerminalOperations()).thenReturn(false);

    DeferredIndexServiceImpl service = serviceWithMocks(null, null, mockDao);
    assertTrue("Should return true when queue is empty", service.awaitCompletion(60L));
  }


  /** awaitCompletion() should return false when the timeout elapses with operations still pending. */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.hasNonTerminalOperations()).thenReturn(true);

    DeferredIndexServiceImpl service = serviceWithMocks(null, null, mockDao);
    assertFalse("Should return false on timeout", service.awaitCompletion(1L));
  }


  /** awaitCompletion() should return true once operations transition to terminal. */
  @Test
  public void testAwaitCompletionPollsUntilDone() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    AtomicInteger callCount = new AtomicInteger();
    when(mockDao.hasNonTerminalOperations()).thenAnswer(inv -> callCount.incrementAndGet() < 3);

    DeferredIndexServiceImpl service = serviceWithMocks(null, null, mockDao);
    assertTrue("Should return true after polling", service.awaitCompletion(30L));
    assertTrue("Should have polled multiple times", callCount.get() >= 3);
  }


  /** awaitCompletion() should return false and restore interrupt flag when interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws Exception {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.hasNonTerminalOperations()).thenReturn(true);

    DeferredIndexServiceImpl service = serviceWithMocks(null, null, mockDao);
    AtomicBoolean result = new AtomicBoolean(true);
    Thread testThread = new Thread(() -> result.set(service.awaitCompletion(60L)));
    testThread.start();
    Thread.sleep(200);
    testThread.interrupt();
    testThread.join(5_000L);

    assertFalse("Should return false when interrupted", result.get());
  }


  /** awaitCompletion() with zero timeout should poll indefinitely until done. */
  @Test
  public void testAwaitCompletionZeroTimeoutWaitsUntilDone() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    AtomicInteger callCount = new AtomicInteger();
    when(mockDao.hasNonTerminalOperations()).thenAnswer(inv -> callCount.incrementAndGet() < 2);

    DeferredIndexServiceImpl service = serviceWithMocks(null, null, mockDao);
    assertTrue("Should return true once done", service.awaitCompletion(0L));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexServiceImpl serviceWithMocks(DeferredIndexRecoveryService recovery,
                                                     DeferredIndexExecutor executor,
                                                     DeferredIndexOperationDAO dao) {
    DeferredIndexConfig config = new DeferredIndexConfig();
    return new DeferredIndexServiceImpl(null, config) {
      @Override
      DeferredIndexRecoveryService createRecoveryService() {
        return recovery;
      }

      @Override
      DeferredIndexExecutor createExecutor() {
        return executor;
      }

      @Override
      DeferredIndexOperationDAO createDAO() {
        return dao;
      }
    };
  }
}
