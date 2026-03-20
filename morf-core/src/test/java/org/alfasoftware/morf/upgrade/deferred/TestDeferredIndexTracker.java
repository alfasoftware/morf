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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DeferredIndexTracker}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexTracker {

  private DeferredIndexTracker tracker;


  @Before
  public void setUp() {
    tracker = new DeferredIndexTracker();
  }


  /** Tracking a deferred index makes it visible via hasPendingDeferred. */
  @Test
  public void testTrackPendingMakesIndexVisible() {
    DeferredAddIndex dai = deferredAddIndex("MyTable", "idx_1", "col1", "col2");
    tracker.trackPending(dai);
    assertTrue(tracker.hasPendingDeferred("MyTable", "idx_1"));
  }


  /** hasPendingDeferred is case-insensitive. */
  @Test
  public void testHasPendingDeferredIsCaseInsensitive() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    assertTrue(tracker.hasPendingDeferred("MYTABLE", "IDX_1"));
    assertTrue(tracker.hasPendingDeferred("mytable", "idx_1"));
  }


  /** getPendingDeferred returns the tracked instance. */
  @Test
  public void testGetPendingDeferredReturnsTracked() {
    DeferredAddIndex dai = deferredAddIndex("MyTable", "idx_1", "col1");
    tracker.trackPending(dai);
    assertTrue(tracker.getPendingDeferred("MyTable", "idx_1").isPresent());
    assertThat(tracker.getPendingDeferred("MyTable", "idx_1").get().getNewIndex().getName(), is("idx_1"));
  }


  /** getPendingDeferred returns empty when not tracked. */
  @Test
  public void testGetPendingDeferredReturnsEmptyWhenNotTracked() {
    assertFalse(tracker.getPendingDeferred("MyTable", "idx_1").isPresent());
  }


  /** cancelPending removes the index from tracking. */
  @Test
  public void testCancelPendingRemovesIndex() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.cancelPending("MyTable", "idx_1");
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_1"));
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** cancelPending leaves other indexes on the same table tracked. */
  @Test
  public void testCancelPendingLeavesOtherIndexes() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.trackPending(deferredAddIndex("MyTable", "idx_2", "col2"));
    tracker.cancelPending("MyTable", "idx_1");
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_1"));
    assertTrue(tracker.hasPendingDeferred("MyTable", "idx_2"));
  }


  /** cancelPending is a no-op for untracked indexes. */
  @Test
  public void testCancelPendingNoOpWhenNotTracked() {
    tracker.cancelPending("MyTable", "idx_1");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** cancelAllPendingForTable clears all indexes on the table. */
  @Test
  public void testCancelAllPendingForTable() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.trackPending(deferredAddIndex("MyTable", "idx_2", "col2"));
    tracker.cancelAllPendingForTable("MyTable");
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_1"));
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_2"));
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** cancelAllPendingForTable is a no-op when table not tracked. */
  @Test
  public void testCancelAllPendingForTableNoOpWhenNotTracked() {
    tracker.cancelAllPendingForTable("MyTable");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** cancelPendingReferencingColumn cancels indexes referencing that column. */
  @Test
  public void testCancelPendingReferencingColumn() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1", "col2"));
    tracker.trackPending(deferredAddIndex("MyTable", "idx_2", "col3"));
    tracker.cancelPendingReferencingColumn("MyTable", "col1");
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_1"));
    assertTrue(tracker.hasPendingDeferred("MyTable", "idx_2"));
  }


  /** cancelPendingReferencingColumn is case-insensitive on column name. */
  @Test
  public void testCancelPendingReferencingColumnIsCaseInsensitive() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "Col1"));
    tracker.cancelPendingReferencingColumn("MyTable", "COL1");
    assertFalse(tracker.hasPendingDeferred("MyTable", "idx_1"));
  }


  /** cancelPendingReferencingColumn is a no-op for unrelated columns. */
  @Test
  public void testCancelPendingReferencingColumnNoOpForUnrelatedColumn() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.cancelPendingReferencingColumn("MyTable", "col99");
    assertTrue(tracker.hasPendingDeferred("MyTable", "idx_1"));
  }


  /** updatePendingTableName renames the table in all tracked operations. */
  @Test
  public void testUpdatePendingTableName() {
    tracker.trackPending(deferredAddIndex("OldTable", "idx_1", "col1"));
    tracker.updatePendingTableName("OldTable", "NewTable");
    assertFalse(tracker.hasPendingDeferred("OldTable", "idx_1"));
    assertTrue(tracker.hasPendingDeferred("NewTable", "idx_1"));
    assertThat(tracker.getSurvivingDeferredIndexes().get(0).getTableName(), is("NewTable"));
  }


  /** updatePendingTableName is a no-op when table not tracked. */
  @Test
  public void testUpdatePendingTableNameNoOpWhenNotTracked() {
    tracker.updatePendingTableName("OldTable", "NewTable");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** updatePendingColumnName renames column references in tracked indexes. */
  @Test
  public void testUpdatePendingColumnName() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "oldCol", "otherCol"));
    tracker.updatePendingColumnName("MyTable", "oldCol", "newCol");
    DeferredAddIndex surviving = tracker.getSurvivingDeferredIndexes().get(0);
    assertThat(surviving.getNewIndex().columnNames(), contains("newCol", "otherCol"));
  }


  /** updatePendingColumnName is a no-op when column not referenced. */
  @Test
  public void testUpdatePendingColumnNameNoOpWhenNotReferenced() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.updatePendingColumnName("MyTable", "col99", "newCol");
    assertThat(tracker.getSurvivingDeferredIndexes().get(0).getNewIndex().columnNames(), contains("col1"));
  }


  /** updatePendingIndexName renames the index. */
  @Test
  public void testUpdatePendingIndexName() {
    tracker.trackPending(deferredAddIndex("MyTable", "oldIdx", "col1"));
    tracker.updatePendingIndexName("MyTable", "oldIdx", "newIdx");
    assertFalse(tracker.hasPendingDeferred("MyTable", "oldIdx"));
    assertTrue(tracker.hasPendingDeferred("MyTable", "newIdx"));
    assertThat(tracker.getSurvivingDeferredIndexes().get(0).getNewIndex().getName(), is("newIdx"));
  }


  /** updatePendingIndexName is a no-op when index not tracked. */
  @Test
  public void testUpdatePendingIndexNameNoOpWhenNotTracked() {
    tracker.updatePendingIndexName("MyTable", "oldIdx", "newIdx");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** getSurvivingDeferredIndexes returns all tracked operations. */
  @Test
  public void testGetSurvivingDeferredIndexes() {
    tracker.trackPending(deferredAddIndex("Table1", "idx_1", "col1"));
    tracker.trackPending(deferredAddIndex("Table2", "idx_2", "col2"));
    List<DeferredAddIndex> surviving = tracker.getSurvivingDeferredIndexes();
    assertThat(surviving, hasSize(2));
  }


  /** getSurvivingDeferredIndexes returns empty when nothing tracked. */
  @Test
  public void testGetSurvivingDeferredIndexesEmpty() {
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** Defer then remove in the same pass = cancelled. */
  @Test
  public void testDeferThenRemoveCancels() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.cancelPending("MyTable", "idx_1");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** Defer then rename = renamed index survives. */
  @Test
  public void testDeferThenRenameIndex() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_old", "col1"));
    tracker.updatePendingIndexName("MyTable", "idx_old", "idx_new");
    List<DeferredAddIndex> surviving = tracker.getSurvivingDeferredIndexes();
    assertThat(surviving, hasSize(1));
    assertThat(surviving.get(0).getNewIndex().getName(), is("idx_new"));
  }


  /** Defer then removeTable = all indexes on table cancelled. */
  @Test
  public void testDeferThenRemoveTable() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1"));
    tracker.trackPending(deferredAddIndex("MyTable", "idx_2", "col2"));
    tracker.cancelAllPendingForTable("MyTable");
    assertThat(tracker.getSurvivingDeferredIndexes(), is(empty()));
  }


  /** Defer then removeColumn cancels only indexes referencing that column. */
  @Test
  public void testDeferThenRemoveColumnCancelsAffected() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "col1", "col2"));
    tracker.trackPending(deferredAddIndex("MyTable", "idx_2", "col3"));
    tracker.cancelPendingReferencingColumn("MyTable", "col1");
    List<DeferredAddIndex> surviving = tracker.getSurvivingDeferredIndexes();
    assertThat(surviving, hasSize(1));
    assertThat(surviving.get(0).getNewIndex().getName(), is("idx_2"));
  }


  /** Defer then renameTable updates table name in surviving indexes. */
  @Test
  public void testDeferThenRenameTable() {
    tracker.trackPending(deferredAddIndex("OldTable", "idx_1", "col1"));
    tracker.updatePendingTableName("OldTable", "NewTable");
    List<DeferredAddIndex> surviving = tracker.getSurvivingDeferredIndexes();
    assertThat(surviving, hasSize(1));
    assertThat(surviving.get(0).getTableName(), is("NewTable"));
  }


  /** Defer then changeColumn (rename) updates column name in affected indexes. */
  @Test
  public void testDeferThenChangeColumnRename() {
    tracker.trackPending(deferredAddIndex("MyTable", "idx_1", "oldCol", "otherCol"));
    tracker.updatePendingColumnName("MyTable", "oldCol", "newCol");
    List<DeferredAddIndex> surviving = tracker.getSurvivingDeferredIndexes();
    assertThat(surviving, hasSize(1));
    assertThat(surviving.get(0).getNewIndex().columnNames(), contains("newCol", "otherCol"));
  }


  /** Unique index survives rename and preserves uniqueness. */
  @Test
  public void testUniqueIndexPreservedThroughRename() {
    Index uniqueIdx = index("idx_u").columns("col1").unique();
    tracker.trackPending(new DeferredAddIndex("MyTable", uniqueIdx, "uuid-1"));
    tracker.updatePendingIndexName("MyTable", "idx_u", "idx_u_new");
    DeferredAddIndex surviving = tracker.getSurvivingDeferredIndexes().get(0);
    assertTrue(surviving.getNewIndex().isUnique());
    assertThat(surviving.getNewIndex().getName(), is("idx_u_new"));
  }


  /** Unique index preserves uniqueness through column rename. */
  @Test
  public void testUniqueIndexPreservedThroughColumnRename() {
    Index uniqueIdx = index("idx_u").columns("oldCol").unique();
    tracker.trackPending(new DeferredAddIndex("MyTable", uniqueIdx, "uuid-1"));
    tracker.updatePendingColumnName("MyTable", "oldCol", "newCol");
    DeferredAddIndex surviving = tracker.getSurvivingDeferredIndexes().get(0);
    assertTrue(surviving.getNewIndex().isUnique());
    assertThat(surviving.getNewIndex().columnNames(), contains("newCol"));
  }


  /**
   * Builds a non-unique {@link DeferredAddIndex} for testing.
   */
  private static DeferredAddIndex deferredAddIndex(String tableName, String indexName, String... columns) {
    Index idx = index(indexName).columns(columns);
    return new DeferredAddIndex(tableName, idx, "test-uuid");
  }
}
