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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
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
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
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

    when(mockDao.findPendingOperations()).thenReturn(Collections.emptyList());

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    check.run();

    verify(mockDao).resetAllInProgressToPending();
    verify(mockDao).findPendingOperations();
  }


  // -------------------------------------------------------------------------
  // augmentSchemaWithDeferredIndexes
  // -------------------------------------------------------------------------

  /** augment should return the same schema when the table does not exist. */
  @Test
  public void testAugmentSkipsWhenTableDoesNotExist() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithoutTable);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    assertSame("Should return input schema unchanged", input, check.augmentSchemaWithDeferredIndexes(input));
    verify(mockDao, never()).findNonTerminalOperations();
  }


  /** augment should return the same schema when no non-terminal ops exist. */
  @Test
  public void testAugmentReturnsUnchangedWhenNoOps() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(Collections.emptyList());
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    assertSame("Should return input schema unchanged", input, check.augmentSchemaWithDeferredIndexes(input));
  }


  /** augment should add a non-unique index to the schema. */
  @Test
  public void testAugmentAddsIndex() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(List.of(buildOp(1L, "Foo", "Foo_Col1_1", false, "col1")));
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(table("Foo").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("col1", DataType.STRING, 50)
    ));

    Schema result = check.augmentSchemaWithDeferredIndexes(input);
    assertTrue("Index should be added",
        result.getTable("Foo").indexes().stream()
            .anyMatch(idx -> "Foo_Col1_1".equals(idx.getName())));
  }


  /** augment should add a unique index when the operation specifies unique. */
  @Test
  public void testAugmentAddsUniqueIndex() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(List.of(buildOp(1L, "Foo", "Foo_Col1_U", true, "col1")));
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(table("Foo").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("col1", DataType.STRING, 50)
    ));

    Schema result = check.augmentSchemaWithDeferredIndexes(input);
    assertTrue("Unique index should be added",
        result.getTable("Foo").indexes().stream()
            .anyMatch(idx -> "Foo_Col1_U".equals(idx.getName()) && idx.isUnique()));
  }


  /** augment should skip an op whose table does not exist in the schema. */
  @Test
  public void testAugmentSkipsOpForMissingTable() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(List.of(buildOp(1L, "NoSuchTable", "Idx_1", false, "col1")));
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    Schema result = check.augmentSchemaWithDeferredIndexes(input);
    // Should still have only the Foo table, no crash
    assertTrue("Foo table should still exist", result.tableExists("Foo"));
    assertEquals("No indexes should be added to Foo", 0, result.getTable("Foo").indexes().size());
  }


  /** augment should skip an op whose index already exists on the table. */
  @Test
  public void testAugmentSkipsExistingIndex() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(List.of(buildOp(1L, "Foo", "Foo_Col1_1", false, "col1")));
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(table("Foo").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("col1", DataType.STRING, 50)
    ).indexes(
        index("Foo_Col1_1").columns("col1")
    ));

    Schema result = check.augmentSchemaWithDeferredIndexes(input);
    long indexCount = result.getTable("Foo").indexes().stream()
        .filter(idx -> "Foo_Col1_1".equals(idx.getName()))
        .count();
    assertEquals("Should not duplicate existing index", 1, indexCount);
  }


  /** augment should handle multiple ops on different tables. */
  @Test
  public void testAugmentMultipleOpsOnDifferentTables() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.findNonTerminalOperations()).thenReturn(List.of(
        buildOp(1L, "Foo", "Foo_Col1_1", false, "col1"),
        buildOp(2L, "Bar", "Bar_Val_1", false, "val")
    ));
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockDao, null, config, connWithTable);
    Schema input = schema(
        table("Foo").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("col1", DataType.STRING, 50)
        ),
        table("Bar").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("val", DataType.STRING, 50)
        )
    );

    Schema result = check.augmentSchemaWithDeferredIndexes(input);
    assertTrue("Foo index should be added",
        result.getTable("Foo").indexes().stream().anyMatch(idx -> "Foo_Col1_1".equals(idx.getName())));
    assertTrue("Bar index should be added",
        result.getTable("Bar").indexes().stream().anyMatch(idx -> "Bar_Val_1".equals(idx.getName())));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

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


  private DeferredIndexOperation buildOp(long id, String tableName, String indexName,
                                          boolean unique, String... columns) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(id);
    op.setUpgradeUUID("test-uuid");
    op.setTableName(tableName);
    op.setIndexName(indexName);
    op.setIndexUnique(unique);
    op.setStatus(DeferredIndexStatus.PENDING);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setColumnNames(List.of(columns));
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
