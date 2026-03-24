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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexServiceImpl} covering the
 * {@code execute()} / {@code awaitCompletion()} orchestration logic.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexServiceImpl {

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
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws InterruptedException {
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

    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(null, mockDao);
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
    return new DeferredIndexServiceImpl(executor, mock(DeferredIndexOperationDAO.class));
  }
}
