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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexServiceImpl} covering config validation
 * and the {@code execute()} / {@code awaitCompletion()} orchestration logic.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexServiceImpl {

  // -------------------------------------------------------------------------
  // Config validation (triggered by execute(), not constructor)
  // -------------------------------------------------------------------------

  /** Construction with valid default config should succeed. */
  @Test
  public void testConstructionWithDefaultConfig() {
    new DeferredIndexServiceImpl(null, null, new DeferredIndexExecutionConfig());
  }


  /** Construction with invalid config should succeed — validation happens in execute(). */
  @Test
  public void testConstructionWithInvalidConfigSucceeds() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    new DeferredIndexServiceImpl(null, null, config);
  }


  /** threadPoolSize less than 1 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadPoolSize() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    new DeferredIndexServiceImpl(null, null, config).execute();
  }


  /** maxRetries less than 0 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMaxRetries() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setMaxRetries(-1);
    new DeferredIndexServiceImpl(null, null, config).execute();
  }


  /** retryBaseDelayMs less than 0 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryBaseDelayMs() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(-1L);
    new DeferredIndexServiceImpl(null, null, config).execute();
  }


  /** retryMaxDelayMs less than retryBaseDelayMs should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryMaxDelayMs() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10_000L);
    config.setRetryMaxDelayMs(5_000L);
    new DeferredIndexServiceImpl(null, null, config).execute();
  }


  /** Validate the error message when threadPoolSize is invalid. */
  @Test
  public void testInvalidThreadPoolSizeMessage() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    try {
      new DeferredIndexServiceImpl(null, null, config).execute();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue("Message should mention threadPoolSize", e.getMessage().contains("threadPoolSize"));
    }
  }


  /** Config validation should accept edge-case valid values. */
  @Test
  public void testEdgeCaseValidConfig() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(1);
    config.setMaxRetries(0);
    config.setRetryBaseDelayMs(0L);
    config.setRetryMaxDelayMs(0L);
    config.setExecutionTimeoutSeconds(1L);

    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));
    new DeferredIndexServiceImpl(mockExecutor, mock(DeferredIndexOperationDAO.class), config).execute();

    verify(mockExecutor).execute();
  }


  /** Default config should pass all validation checks. */
  @Test
  public void testDefaultConfigPassesAllValidation() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    assertFalse("Default maxRetries should be >= 0", config.getMaxRetries() < 0);
    assertTrue("Default threadPoolSize should be >= 1", config.getThreadPoolSize() >= 1);
    assertTrue("Default retryBaseDelayMs should be >= 0", config.getRetryBaseDelayMs() >= 0);
    assertTrue("Default retryMaxDelayMs >= retryBaseDelayMs",
        config.getRetryMaxDelayMs() >= config.getRetryBaseDelayMs());
  }


  // -------------------------------------------------------------------------
  // execute() orchestration
  // -------------------------------------------------------------------------

  /** execute() should call executor. */
  @Test
  public void testExecuteCallsExecutor() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor);
    service.execute();

    verify(mockExecutor).execute();
  }


  // -------------------------------------------------------------------------
  // awaitCompletion() orchestration
  // -------------------------------------------------------------------------

  /** awaitCompletion() should throw when execute() has not been called. */
  @Test(expected = IllegalStateException.class)
  public void testAwaitCompletionThrowsWhenNoExecution() {
    DeferredIndexServiceImpl service = serviceWithMocks(null);
    service.awaitCompletion(60L);
  }


  /** awaitCompletion() should return true when the future is already done. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenFutureDone() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor);
    service.execute();

    assertTrue("Should return true when future is complete", service.awaitCompletion(60L));
  }


  /** awaitCompletion() should return false when the future does not complete in time. */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(new CompletableFuture<>()); // never completes

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor);
    service.execute();

    assertFalse("Should return false on timeout", service.awaitCompletion(1L));
  }


  /** awaitCompletion() should return false and restore interrupt flag when interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws Exception {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(new CompletableFuture<>()); // never completes

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor);
    service.execute();

    CountDownLatch enteredAwait = new CountDownLatch(1);
    java.util.concurrent.atomic.AtomicBoolean result = new java.util.concurrent.atomic.AtomicBoolean(true);
    Thread testThread = new Thread(() -> {
      enteredAwait.countDown();
      result.set(service.awaitCompletion(60L));
    });
    testThread.start();
    enteredAwait.await();
    testThread.interrupt();
    testThread.join(5_000L);

    assertFalse("Should return false when interrupted", result.get());
  }


  /** awaitCompletion() with zero timeout should wait indefinitely until done. */
  @Test
  public void testAwaitCompletionZeroTimeoutWaitsUntilDone() throws Exception {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    when(mockExecutor.execute()).thenReturn(future);

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor);
    service.execute();

    CountDownLatch enteredAwait = new CountDownLatch(1);
    // Complete the future once the test thread has entered awaitCompletion
    new Thread(() -> {
      try { enteredAwait.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      future.complete(null);
    }).start();

    enteredAwait.countDown();
    assertTrue("Should return true once done", service.awaitCompletion(0L));
  }


  // -------------------------------------------------------------------------
  // getProgress()
  // -------------------------------------------------------------------------

  /** getProgress() should delegate to the DAO and return the counts map. */
  @Test
  public void testGetProgressDelegatesToDao() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    Map<DeferredIndexStatus, Integer> counts = new EnumMap<>(DeferredIndexStatus.class);
    counts.put(DeferredIndexStatus.COMPLETED, 3);
    counts.put(DeferredIndexStatus.IN_PROGRESS, 1);
    counts.put(DeferredIndexStatus.PENDING, 5);
    counts.put(DeferredIndexStatus.FAILED, 0);
    when(mockDao.countAllByStatus()).thenReturn(counts);

    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, mockDao, new DeferredIndexExecutionConfig());
    Map<DeferredIndexStatus, Integer> result = service.getProgress();

    assertEquals(Integer.valueOf(3), result.get(DeferredIndexStatus.COMPLETED));
    assertEquals(Integer.valueOf(1), result.get(DeferredIndexStatus.IN_PROGRESS));
    assertEquals(Integer.valueOf(5), result.get(DeferredIndexStatus.PENDING));
    assertEquals(Integer.valueOf(0), result.get(DeferredIndexStatus.FAILED));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexServiceImpl serviceWithMocks(DeferredIndexExecutor executor) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    return new DeferredIndexServiceImpl(executor, mock(DeferredIndexOperationDAO.class), config);
  }
}
