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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexReadinessCheckImpl} covering the
 * {@link DeferredIndexReadinessCheck#run()} and
 * {@link DeferredIndexReadinessCheck#augmentSchemaWithDeferredIndexes} methods
 * with mocked DAO, executor, and connection dependencies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexReadinessCheckUnit {

  private ConnectionResources connWithTable;
  private ConnectionResources connWithoutTable;


  /** Set up mock connections with and without the deferred index table. */
  @Before
  public void setUp() {
    connWithTable = mockConnectionResources(true);
    connWithoutTable = mockConnectionResources(false);
  }


  /** run() should return immediately when no pending operations exist. */
  @Test
  public void testRunWithEmptyQueue() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(0);
    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    check.run();

    verify(mockDao).findPendingOperations();
    verify(mockDao, never()).countAllByStatus();
  }


  /** run() should execute pending operations and succeed when all complete. */
  @Test
  public void testRunExecutesPendingOperationsSuccessfully() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(0);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L)));
    when(mockDao.countAllByStatus()).thenReturn(statusCounts(0));

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, mockExecutor, config, connWithTable);
    check.run();

    verify(mockExecutor).execute();
    verify(mockDao).countAllByStatus();
  }


  /** run() should throw IllegalStateException when any operations fail. */
  @Test(expected = IllegalStateException.class)
  public void testRunThrowsWhenOperationsFail() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(0);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L)));
    when(mockDao.countAllByStatus()).thenReturn(statusCounts(1));

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, mockExecutor, config, connWithTable);
    check.run();
  }


  /** The failure exception message should include the failed count. */
  @Test
  public void testRunFailureMessageIncludesCount() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(0);
    when(mockDao.findPendingOperations()).thenReturn(List.of(buildOp(1L), buildOp(2L)));
    when(mockDao.countAllByStatus()).thenReturn(statusCounts(2));

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    when(mockExecutor.execute()).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, mockExecutor, config, connWithTable);
    try {
      check.run();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should include count", e.getMessage().contains("2"));
    }
  }


  /** The executor should not be called when the pending queue is empty. */
  @Test
  public void testExecutorNotCalledWhenQueueEmpty() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(0);
    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, mockExecutor, config, connWithTable);
    check.run();

    verify(mockExecutor, never()).execute();
  }


  /** run() should skip entirely when the DeferredIndexOperation table does not exist. */
  @Test
  public void testRunSkipsWhenTableDoesNotExist() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    DeferredIndexExecutor mockExecutor = mock(DeferredIndexExecutor.class);
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, mockExecutor, config, connWithoutTable);
    check.run();

    verify(mockDao, never()).findPendingOperations();
    verify(mockExecutor, never()).execute();
  }


  /** run() should reset IN_PROGRESS operations to PENDING before querying. */
  @Test
  public void testRunResetsInProgressToPending() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.resetAllInProgressToPending()).thenReturn(2);
    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    check.run();

    verify(mockDao).resetAllInProgressToPending();
    verify(mockDao).findPendingOperations();
  }


  private DeferredIndexOperation buildOp(long id) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(id);
    op.setUpgradeUUID("test-uuid");
    op.setTableName("TestTable");
    op.setIndexName("TestIndex");
    op.setIndexUnique(false);
    op.setStatus(DeferredIndexStatus.PENDING);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setColumnNames(List.of("col1"));
    return op;
  }


  private Map<DeferredIndexStatus, Integer> statusCounts(int failedCount) {
    Map<DeferredIndexStatus, Integer> counts = new EnumMap<>(DeferredIndexStatus.class);
    for (DeferredIndexStatus s : DeferredIndexStatus.values()) {
      counts.put(s, 0);
    }
    counts.put(DeferredIndexStatus.FAILED, failedCount);
    return counts;
  }


  private static ConnectionResources mockConnectionResources(boolean tableExists) {
    SchemaResource mockSr = mock(SchemaResource.class);
    when(mockSr.tableExists(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME)).thenReturn(tableExists);
    ConnectionResources mockConn = mock(ConnectionResources.class);
    when(mockConn.openSchemaResource()).thenReturn(mockSr);
    return mockConn;
  }
}
