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

import java.util.List;
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
    // given
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);

    // when
    service.execute();

    // then
    verify(mockExecutor).execute();
  }


  // -------------------------------------------------------------------------
  // awaitCompletion() orchestration
  // -------------------------------------------------------------------------

  /** awaitCompletion() should throw when execute() has not been called. */
  @Test(expected = IllegalStateException.class)
  public void testAwaitCompletionThrowsWhenNoExecution() {
    // given
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mock(DeferredIndexExecutor.class));

    // when -- should throw
    service.awaitCompletion(60L);
  }


  /** awaitCompletion() should return true when the future is already done. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenFutureDone() {
    // given
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);
    service.execute();

    // when / then
    assertTrue("Should return true when future is complete", service.awaitCompletion(60L));
  }


  /** awaitCompletion() should return false when the future does not complete in time. */
  @Test
  public void testAwaitCompletionReturnsFalseOnTimeout() {
    // given -- future that never completes
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(new CompletableFuture<>());
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);
    service.execute();

    // when / then
    assertFalse("Should return false on timeout", service.awaitCompletion(1L));
  }


  /** awaitCompletion() should return false and restore interrupt flag when interrupted. */
  @Test
  public void testAwaitCompletionReturnsFalseWhenInterrupted() throws InterruptedException {
    // given -- future that never completes
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(new CompletableFuture<>());
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);
    service.execute();

    // when -- interrupt the waiting thread
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

    // then
    assertFalse("Should return false when interrupted", result.get());
  }


  /** awaitCompletion() with zero timeout should wait indefinitely until done. */
  @Test
  public void testAwaitCompletionZeroTimeoutWaitsUntilDone() {
    // given
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    CompletableFuture<Void> future = new CompletableFuture<>();
    when(mockExecutor.execute()).thenReturn(future);
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);
    service.execute();

    // when -- complete the future from another thread after await starts
    CountDownLatch enteredAwait = new CountDownLatch(1);
    new Thread(() -> {
      try { enteredAwait.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      future.complete(null);
    }).start();
    enteredAwait.countDown();

    // then
    assertTrue("Should return true once done", service.awaitCompletion(0L));
  }


  // -------------------------------------------------------------------------
  // getMissingDeferredIndexStatements()
  // -------------------------------------------------------------------------

  /** getMissingDeferredIndexStatements() should delegate to the executor. */
  @Test
  public void testGetMissingDeferredIndexStatementsDelegatesToExecutor() {
    // given
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.getMissingDeferredIndexStatements())
        .thenReturn(List.of("CREATE INDEX idx ON t(c)"));
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(mockExecutor);

    // when
    List<String> result = service.getMissingDeferredIndexStatements();

    // then
    assertEquals(1, result.size());
    assertEquals("CREATE INDEX idx ON t(c)", result.get(0));
  }
}
