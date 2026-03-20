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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexServiceImpl} covering config validation
 * and the {@code execute()} / {@code awaitCompletion()} orchestration logic.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexServiceImpl {

  private static final Collection<Class<? extends UpgradeStep>> EMPTY_STEPS = Collections.emptyList();

  // -------------------------------------------------------------------------
  // Config validation (triggered by execute(), not constructor)
  // -------------------------------------------------------------------------

  /** Construction with valid default config should succeed. */
  @Test
  public void testConstructionWithDefaultConfig() {
    new DeferredIndexServiceImpl(null, new DeferredIndexExecutionConfig(), null);
  }


  /** Construction with invalid config should succeed — validation happens in execute(). */
  @Test
  public void testConstructionWithInvalidConfigSucceeds() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    new DeferredIndexServiceImpl(null, config, null);
  }


  /** threadPoolSize less than 1 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadPoolSize() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, config, mock(DeferredIndexReadinessCheck.class));
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();
  }


  /** maxRetries less than 0 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMaxRetries() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setMaxRetries(-1);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, config, mock(DeferredIndexReadinessCheck.class));
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();
  }


  /** retryBaseDelayMs less than 0 should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryBaseDelayMs() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(-1L);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, config, mock(DeferredIndexReadinessCheck.class));
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();
  }


  /** retryMaxDelayMs less than retryBaseDelayMs should be rejected on execute(). */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidRetryMaxDelayMs() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10_000L);
    config.setRetryMaxDelayMs(5_000L);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, config, mock(DeferredIndexReadinessCheck.class));
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();
  }


  /** Validate the error message when threadPoolSize is invalid. */
  @Test
  public void testInvalidThreadPoolSizeMessage() {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setThreadPoolSize(0);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, config, mock(DeferredIndexReadinessCheck.class));
    service.setUpgradeSteps(EMPTY_STEPS);
    try {
      service.execute();
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
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(Collections.emptyList());
    when(mockExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor, config, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();

    verify(mockExecutor).execute(any());
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

  /** execute() should discover missing indexes via readinessCheck and pass them to executor. */
  @Test
  public void testExecuteCallsExecutor() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);

    List<DeferredAddIndex> missing = Arrays.asList(
        new DeferredAddIndex("T", index("T_1").columns("a", "b"), "uuid-1"));
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(missing);
    when(mockExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();

    verify(mockReadiness).findMissingDeferredIndexes(EMPTY_STEPS);
    verify(mockExecutor).execute(missing);
  }


  /** execute() should throw IllegalStateException if setUpgradeSteps was not called first. */
  @Test(expected = IllegalStateException.class)
  public void testExecuteThrowsWithoutUpgradeSteps() {
    DeferredIndexServiceImpl service = serviceWithMocks(null, null);
    service.execute();
  }


  // -------------------------------------------------------------------------
  // awaitCompletion() orchestration
  // -------------------------------------------------------------------------

  /** awaitCompletion() should throw when execute() has not been called. */
  @Test(expected = IllegalStateException.class)
  public void testAwaitCompletionThrowsWhenNoExecution() {
    DeferredIndexServiceImpl service = serviceWithMocks(null, null);
    service.awaitCompletion(60L);
  }


  /** awaitCompletion() should return true when the future is already done. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenFutureDone() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(Collections.emptyList());
    when(mockExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();

    assertTrue("Should return true when future is complete", service.awaitCompletion(60L));
  }


  /** awaitCompletion() should return false when the future does not complete in time. */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(Collections.emptyList());
    when(mockExecutor.execute(any())).thenReturn(new CompletableFuture<>()); // never completes

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();

    assertFalse("Should return false on timeout", service.awaitCompletion(1L));
  }


  /** awaitCompletion() should return false and restore interrupt flag when interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws Exception {
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(Collections.emptyList());
    when(mockExecutor.execute(any())).thenReturn(new CompletableFuture<>()); // never completes

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
    service.execute();

    CountDownLatch enteredAwait = new CountDownLatch(1);
    AtomicBoolean result = new AtomicBoolean(true);
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
    DeferredIndexReadinessCheck mockReadiness = mock(DeferredIndexReadinessCheck.class);
    when(mockReadiness.findMissingDeferredIndexes(any())).thenReturn(Collections.emptyList());
    CompletableFuture<Void> future = new CompletableFuture<>();
    when(mockExecutor.execute(any())).thenReturn(future);

    DeferredIndexServiceImpl service = serviceWithMocks(mockExecutor, mockReadiness);
    service.setUpgradeSteps(EMPTY_STEPS);
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

  /** getProgress() should return zeros when the executor is a mock (not DeferredIndexExecutorImpl). */
  @Test
  public void testGetProgressReturnsEmptyForMockedExecutor() {
    DeferredIndexServiceImpl service = serviceWithMocks(mock(DeferredIndexExecutor.class), mock(DeferredIndexReadinessCheck.class));
    DeferredIndexProgress progress = service.getProgress();

    assertEquals("total", 0, progress.getTotal());
    assertEquals("completed", 0, progress.getCompleted());
    assertEquals("failed", 0, progress.getFailed());
    assertEquals("remaining", 0, progress.getRemaining());
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexServiceImpl serviceWithMocks(DeferredIndexExecutor executor,
                                                     DeferredIndexReadinessCheck readinessCheck) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    return new DeferredIndexServiceImpl(executor, config, readinessCheck);
  }
}
