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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexValidatorImpl} covering the
 * {@link DeferredIndexValidator#validateNoPendingOperations()} method
 * with mocked DAO and executor dependencies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexValidatorUnit {

  /** validateNoPendingOperations should return immediately when no pending operations exist. */
  @Test
  public void testValidateNoPendingOperationsWithEmptyQueue() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexValidator validator = new DeferredIndexValidatorImpl(mockDao, null, config);
    validator.validateNoPendingOperations();

    verify(mockDao).findPendingOperations();
    verify(mockDao, never()).countFailedOperations();
  }


  /** validateNoPendingOperations should execute pending operations and succeed when all complete. */
  @Test
  public void testValidateExecutesPendingOperationsSuccessfully() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L)));
    when(mockDao.countFailedOperations()).thenReturn(0);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexValidator validator = new DeferredIndexValidatorImpl(mockDao, mockExecutor, config);
    validator.validateNoPendingOperations();

    verify(mockExecutor).execute();
    verify(mockDao).countFailedOperations();
  }


  /** validateNoPendingOperations should throw IllegalStateException when any operations fail. */
  @Test(expected = IllegalStateException.class)
  public void testValidateThrowsWhenOperationsFail() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L)));
    when(mockDao.countFailedOperations()).thenReturn(1);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexValidator validator = new DeferredIndexValidatorImpl(mockDao, mockExecutor, config);
    validator.validateNoPendingOperations();
  }


  /** The failure exception message should include the failed count. */
  @Test
  public void testValidateFailureMessageIncludesCount() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L), buildOp(2L)));
    when(mockDao.countFailedOperations()).thenReturn(2);

    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexValidator validator = new DeferredIndexValidatorImpl(mockDao, mockExecutor, config);
    try {
      validator.validateNoPendingOperations();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should include count", e.getMessage().contains("2"));
    }
  }


  /** The executor should not be called when the pending queue is empty. */
  @Test
  public void testExecutorNotCalledWhenQueueEmpty() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexConfig config = new DeferredIndexConfig();
    DeferredIndexValidator validator = new DeferredIndexValidatorImpl(mockDao, mockExecutor, config);
    validator.validateNoPendingOperations();

    verify(mockExecutor, never()).execute();
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
