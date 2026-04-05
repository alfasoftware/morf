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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.Statement;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DeferredIndexChangeServiceImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexChangeServiceImpl {

  private DeferredIndexChangeServiceImpl service;


  /**
   * Create a fresh service before each test.
   */
  @Before
  public void setUp() {
    service = new DeferredIndexChangeServiceImpl();
  }


  /**
   * trackPending returns a single INSERT for the operation row containing the
   * expected table, index, and comma-separated column names.
   */
  @Test
  public void testTrackPendingReturnsInsertStatements() {
    List<Statement> statements = new ArrayList<>(service.trackPending(makeDeferred("TestTable", "TestIdx", "col1", "col2")));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("PENDING"));
    assertThat(statements.get(0).toString(), containsString("TestTable"));
    assertThat(statements.get(0).toString(), containsString("TestIdx"));
    assertThat(statements.get(0).toString(), containsString("col1,col2"));
  }


  /**
   * hasPendingDeferred returns true after trackPending and false before.
   */
  @Test
  public void testHasPendingDeferredReflectsTracking() {
    assertFalse(service.hasPendingDeferred("TestTable", "TestIdx"));
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1"));
    assertTrue(service.hasPendingDeferred("TestTable", "TestIdx"));
  }


  /**
   * hasPendingDeferred is case-insensitive for both table name and index name.
   */
  @Test
  public void testHasPendingDeferredIsCaseInsensitive() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1"));
    assertTrue(service.hasPendingDeferred("testtable", "testidx"));
    assertTrue(service.hasPendingDeferred("TESTTABLE", "TESTIDX"));
  }


  /**
   * cancelPending returns a single DELETE statement on the operation table
   * and removes the operation from tracking.
   */
  @Test
  public void testCancelPendingReturnsDeleteAndRemovesFromTracking() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1"));

    List<Statement> statements = new ArrayList<>(service.cancelPending("TestTable", "TestIdx"));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("TestIdx"));
    assertFalse(service.hasPendingDeferred("TestTable", "TestIdx"));
  }


  /**
   * cancelPending leaves other indexes on the same table still tracked.
   */
  @Test
  public void testCancelPendingLeavesOtherIndexesOnSameTableTracked() {
    service.trackPending(makeDeferred("TestTable", "Idx1", "col1"));
    service.trackPending(makeDeferred("TestTable", "Idx2", "col2"));

    service.cancelPending("TestTable", "Idx1");

    assertFalse(service.hasPendingDeferred("TestTable", "Idx1"));
    assertTrue(service.hasPendingDeferred("TestTable", "Idx2"));
  }


  /**
   * cancelPending returns an empty list when no pending operation is tracked for that table/index.
   */
  @Test
  public void testCancelPendingReturnsEmptyWhenNoPending() {
    assertThat(service.cancelPending("TestTable", "TestIdx"), is(empty()));
  }


  /**
   * cancelAllPendingForTable returns a single DELETE statement scoped to the table
   * and removes all tracked operations for that table, even when multiple indexes are registered.
   */
  @Test
  public void testCancelAllPendingForTableClearsAllIndexesOnTable() {
    service.trackPending(makeDeferred("TestTable", "Idx1", "col1"));
    service.trackPending(makeDeferred("TestTable", "Idx2", "col2"));

    List<Statement> statements = new ArrayList<>(service.cancelAllPendingForTable("TestTable"));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("TestTable"));
    assertFalse(service.hasPendingDeferred("TestTable", "Idx1"));
    assertFalse(service.hasPendingDeferred("TestTable", "Idx2"));
  }


  /**
   * cancelAllPendingForTable returns an empty list when no pending operations exist for that table.
   */
  @Test
  public void testCancelAllPendingForTableReturnsEmptyWhenNoPending() {
    assertThat(service.cancelAllPendingForTable("TestTable"), is(empty()));
  }


  /**
   * cancelPendingReferencingColumn returns a DELETE statement for any pending index
   * that includes the named column, and removes only those from tracking.
   */
  @Test
  public void testCancelPendingReferencingColumnCancelsAffectedIndex() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1", "col2"));

    List<Statement> statements = new ArrayList<>(service.cancelPendingReferencingColumn("TestTable", "col1"));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("TestIdx"));
    assertFalse(service.hasPendingDeferred("TestTable", "TestIdx"));
  }


  /**
   * cancelPendingReferencingColumn leaves indexes that do not reference the column still tracked.
   */
  @Test
  public void testCancelPendingReferencingColumnLeavesUnaffectedIndexTracked() {
    service.trackPending(makeDeferred("TestTable", "Idx1", "col1"));
    service.trackPending(makeDeferred("TestTable", "Idx2", "col2"));

    service.cancelPendingReferencingColumn("TestTable", "col1");

    assertFalse(service.hasPendingDeferred("TestTable", "Idx1"));
    assertTrue(service.hasPendingDeferred("TestTable", "Idx2"));
  }


  /**
   * cancelPendingReferencingColumn is case-insensitive for the column name.
   */
  @Test
  public void testCancelPendingReferencingColumnIsCaseInsensitive() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "MyColumn"));

    List<Statement> statements = service.cancelPendingReferencingColumn("TestTable", "mycolumn");

    assertThat(statements, hasSize(1));
    assertFalse(service.hasPendingDeferred("TestTable", "TestIdx"));
  }


  /**
   * cancelPendingReferencingColumn returns an empty list when no pending index references
   * the named column.
   */
  @Test
  public void testCancelPendingReferencingColumnReturnsEmptyForUnrelatedColumn() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1", "col2"));
    assertThat(service.cancelPendingReferencingColumn("TestTable", "col3"), is(empty()));
  }


  /**
   * updatePendingTableName returns an UPDATE statement renaming the table in pending rows
   * and updates internal tracking so subsequent lookups use the new name.
   */
  @Test
  public void testUpdatePendingTableNameReturnsUpdateStatement() {
    service.trackPending(makeDeferred("OldTable", "TestIdx", "col1"));

    List<Statement> statements = new ArrayList<>(service.updatePendingTableName("OldTable", "NewTable"));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("OldTable"));
    assertThat(statements.get(0).toString(), containsString("NewTable"));
    assertTrue(service.hasPendingDeferred("NewTable", "TestIdx"));
    assertFalse(service.hasPendingDeferred("OldTable", "TestIdx"));
  }


  /**
   * updatePendingTableName returns an empty list when no pending operations exist for the old table name.
   */
  @Test
  public void testUpdatePendingTableNameReturnsEmptyWhenNoPending() {
    assertThat(service.updatePendingTableName("OldTable", "NewTable"), is(empty()));
  }


  /**
   * updatePendingColumnName returns an UPDATE statement on the operation table
   * setting the indexColumns to the new comma-separated string.
   */
  @Test
  public void testUpdatePendingColumnNameReturnsUpdateStatement() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "oldCol"));

    List<Statement> statements = new ArrayList<>(service.updatePendingColumnName("TestTable", "oldCol", "newCol"));

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("newCol"));
  }


  /**
   * updatePendingColumnName returns one UPDATE per affected index on the main table
   * when multiple indexes on the same table both reference the renamed column.
   */
  @Test
  public void testUpdatePendingColumnNameReturnsOneUpdatePerAffectedIndex() {
    service.trackPending(makeDeferred("TestTable", "Idx1", "sharedCol", "col1"));
    service.trackPending(makeDeferred("TestTable", "Idx2", "sharedCol", "col2"));

    List<Statement> statements = service.updatePendingColumnName("TestTable", "sharedCol", "renamedCol");

    assertThat(statements, hasSize(2));
    assertThat(statements.get(0).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(0).toString(), containsString("renamedCol"));
    assertThat(statements.get(1).toString(), containsString("DeferredIndexOperation"));
    assertThat(statements.get(1).toString(), containsString("renamedCol"));
  }


  /**
   * updatePendingColumnName returns an empty list when no pending index references the old column name.
   */
  @Test
  public void testUpdatePendingColumnNameReturnsEmptyWhenColumnNotReferenced() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "col1"));
    assertThat(service.updatePendingColumnName("TestTable", "otherCol", "newCol"), is(empty()));
  }


  /**
   * updatePendingIndexName updates tracking and returns an UPDATE statement.
   */
  @Test
  public void testUpdatePendingIndexNameUpdatesTrackingAndReturnsStatement() {
    service.trackPending(makeDeferred("TestTable", "OldIdx", "col1"));
    List<Statement> stmts = service.updatePendingIndexName("TestTable", "OldIdx", "NewIdx");
    assertThat(stmts, hasSize(1));
    assertTrue("Should track new name", service.hasPendingDeferred("TestTable", "NewIdx"));
    assertFalse("Should not track old name", service.hasPendingDeferred("TestTable", "OldIdx"));
  }


  /**
   * updatePendingIndexName returns an empty list when no pending index matches.
   */
  @Test
  public void testUpdatePendingIndexNameReturnsEmptyWhenNotTracked() {
    service.trackPending(makeDeferred("TestTable", "SomeIdx", "col1"));
    assertThat(service.updatePendingIndexName("TestTable", "OtherIdx", "NewIdx"), is(empty()));
  }


  /**
   * updatePendingIndexName returns an empty list when the table is not tracked.
   */
  @Test
  public void testUpdatePendingIndexNameReturnsEmptyWhenTableNotTracked() {
    assertThat(service.updatePendingIndexName("NoTable", "OldIdx", "NewIdx"), is(empty()));
  }


  /**
   * After updatePendingColumnName, cancelPendingReferencingColumn finds the
   * index by the new column name.
   */
  @Test
  public void testCancelPendingReferencingColumnFindsRenamedColumn() {
    service.trackPending(makeDeferred("TestTable", "TestIdx", "oldCol"));
    service.updatePendingColumnName("TestTable", "oldCol", "newCol");

    List<Statement> stmts = new ArrayList<>(service.cancelPendingReferencingColumn("TestTable", "newCol"));
    assertThat("should cancel by the new column name", stmts, hasSize(1));
    assertFalse(service.hasPendingDeferred("TestTable", "TestIdx"));
  }


  /**
   * After updatePendingTableName, cancelPendingReferencingColumn finds the
   * index under the new table name.
   */
  @Test
  public void testCancelPendingReferencingColumnAfterTableRename() {
    service.trackPending(makeDeferred("OldTable", "TestIdx", "col1"));
    service.updatePendingTableName("OldTable", "NewTable");

    List<Statement> stmts = new ArrayList<>(service.cancelPendingReferencingColumn("NewTable", "col1"));
    assertThat("should cancel under the new table name", stmts, hasSize(1));
    assertFalse(service.hasPendingDeferred("NewTable", "TestIdx"));
  }


  // -------------------------------------------------------------------------
  // Helper
  // -------------------------------------------------------------------------

  private DeferredAddIndex makeDeferred(String tableName, String indexName, String... columns) {
    Index index = mock(Index.class);
    when(index.getName()).thenReturn(indexName);
    when(index.isUnique()).thenReturn(false);
    when(index.columnNames()).thenReturn(List.of(columns));

    DeferredAddIndex deferred = mock(DeferredAddIndex.class);
    when(deferred.getTableName()).thenReturn(tableName);
    when(deferred.getNewIndex()).thenReturn(index);
    when(deferred.getUpgradeUUID()).thenReturn("test-uuid");
    return deferred;
  }
}
