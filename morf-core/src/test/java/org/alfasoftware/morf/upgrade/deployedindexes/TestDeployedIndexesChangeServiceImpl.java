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

package org.alfasoftware.morf.upgrade.deployedindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.Statement;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesChangeServiceImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesChangeServiceImpl {

  private DeployedIndexesChangeServiceImpl service;

  @Before
  public void setUp() {
    service = new DeployedIndexesChangeServiceImpl();
  }


  /** trackIndex should register and return INSERT statement. */
  @Test
  public void testTrackIndexReturnsInsert() {
    // given
    Index idx = index("Idx1").columns("col1");

    // when
    List<Statement> stmts = service.trackIndex("Table1", idx);

    // then
    assertEquals(1, stmts.size());
    assertTrue("Should be tracked", service.isTracked("Table1", "Idx1"));
    assertTrue("Should contain DeployedIndexes", stmts.get(0).toString().contains("DeployedIndexes"));
  }


  /** trackIndex for deferred should set isTrackedDeferred. */
  @Test
  public void testTrackDeferredIndex() {
    // given
    Index idx = index("Idx1").deferred().columns("col1");

    // when
    service.trackIndex("Table1", idx);

    // then
    assertTrue("Should be tracked", service.isTracked("Table1", "Idx1"));
    assertTrue("Should be tracked as deferred", service.isTrackedDeferred("Table1", "Idx1"));
  }


  /** trackIndex for non-deferred should not be tracked as deferred. */
  @Test
  public void testTrackNonDeferredIndex() {
    // given
    Index idx = index("Idx1").columns("col1");

    // when
    service.trackIndex("Table1", idx);

    // then
    assertTrue("Should be tracked", service.isTracked("Table1", "Idx1"));
    assertFalse("Should not be tracked as deferred", service.isTrackedDeferred("Table1", "Idx1"));
  }


  /** isTracked should be case-insensitive. */
  @Test
  public void testIsTrackedCaseInsensitive() {
    // given
    service.trackIndex("MyTable", index("MyIdx").columns("col1"));

    // then
    assertTrue(service.isTracked("MYTABLE", "MYIDX"));
    assertTrue(service.isTracked("mytable", "myidx"));
  }


  /** removeIndex should return DELETE and untrack. */
  @Test
  public void testRemoveIndex() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("col1"));

    // when
    List<Statement> stmts = service.removeIndex("Table1", "Idx1");

    // then
    assertEquals(1, stmts.size());
    assertFalse("Should be untracked", service.isTracked("Table1", "Idx1"));
  }


  /** removeIndex for non-tracked should return empty. */
  @Test
  public void testRemoveNonTrackedIndex() {
    // when
    List<Statement> stmts = service.removeIndex("Table1", "NonExistent");

    // then
    assertTrue("Should return empty", stmts.isEmpty());
  }


  /** removeAllForTable should remove all indexes for that table. */
  @Test
  public void testRemoveAllForTable() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("col1"));
    service.trackIndex("Table1", index("Idx2").columns("col2"));
    service.trackIndex("Table2", index("Idx3").columns("col3"));

    // when
    List<Statement> stmts = service.removeAllForTable("Table1");

    // then
    assertEquals(1, stmts.size());
    assertFalse(service.isTracked("Table1", "Idx1"));
    assertFalse(service.isTracked("Table1", "Idx2"));
    assertTrue("Table2 should be unaffected", service.isTracked("Table2", "Idx3"));
  }


  /** removeIndexesReferencingColumn should remove matching indexes. */
  @Test
  public void testRemoveIndexesReferencingColumn() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("col1", "col2"));
    service.trackIndex("Table1", index("Idx2").columns("col3"));

    // when
    List<Statement> stmts = service.removeIndexesReferencingColumn("Table1", "col1");

    // then
    assertFalse("Idx1 should be removed", service.isTracked("Table1", "Idx1"));
    assertTrue("Idx2 should remain", service.isTracked("Table1", "Idx2"));
  }


  /** updateTableName should update tracked entries. */
  @Test
  public void testUpdateTableName() {
    // given
    service.trackIndex("OldTable", index("Idx1").columns("col1"));

    // when
    List<Statement> stmts = service.updateTableName("OldTable", "NewTable");

    // then
    assertEquals(1, stmts.size());
    assertFalse(service.isTracked("OldTable", "Idx1"));
    assertTrue(service.isTracked("NewTable", "Idx1"));
  }


  /** updateIndexName should rename in tracking. */
  @Test
  public void testUpdateIndexName() {
    // given
    service.trackIndex("Table1", index("OldIdx").columns("col1"));

    // when
    List<Statement> stmts = service.updateIndexName("Table1", "OldIdx", "NewIdx");

    // then
    assertEquals(1, stmts.size());
    assertFalse(service.isTracked("Table1", "OldIdx"));
    assertTrue(service.isTracked("Table1", "NewIdx"));
  }


  /** updateColumnName should update column references on the matching index only,
   *  emit an UPDATE whose SET targets indexColumns with the renamed CSV, and
   *  filter on (tableName, indexName) of the affected index. */
  @Test
  public void testUpdateColumnName() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("oldCol", "col2"));
    service.trackIndex("Table1", index("Idx2").columns("col3"));

    // when
    List<Statement> stmts = service.updateColumnName("Table1", "oldCol", "newCol");

    // then -- only Idx1 is affected
    assertEquals("Only Idx1 should be affected", 1, stmts.size());

    // and -- UPDATE sets indexColumns="newCol,col2" and filters on (Table1, Idx1)
    org.alfasoftware.morf.sql.UpdateStatement upd =
        (org.alfasoftware.morf.sql.UpdateStatement) stmts.get(0);
    assertEquals(1, upd.getFields().size());
    assertEquals("indexColumns", upd.getFields().get(0).getAlias());
    assertEquals("newCol,col2",
        ((org.alfasoftware.morf.sql.element.FieldLiteral) upd.getFields().get(0)).getValue());
    org.alfasoftware.morf.sql.element.Criterion where = upd.getWhereCriterion();
    assertEquals(org.alfasoftware.morf.sql.element.Operator.AND, where.getOperator());
    assertEquals(2, where.getCriteria().size());
  }


  // ---- Negative / no-op paths -------------------------------------------

  /** removeIndex for an untracked (table, index) pair is a no-op. */
  @Test
  public void testRemoveIndexOnUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.removeIndex("NoSuchTable", "NoSuchIdx");

    // then
    assertTrue("no-op should return empty list", stmts.isEmpty());
  }


  /** removeAllForTable on a table that isn't tracked is a no-op. */
  @Test
  public void testRemoveAllForUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.removeAllForTable("NoSuchTable");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** removeIndexesReferencingColumn on an untracked table is a no-op. */
  @Test
  public void testRemoveIndexesReferencingColumnOnUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.removeIndexesReferencingColumn("NoSuchTable", "anyCol");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateTableName on a table that isn't tracked is a no-op. */
  @Test
  public void testUpdateTableNameOnUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.updateTableName("NoSuchTable", "NewName");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateColumnName on a table that isn't tracked is a no-op. */
  @Test
  public void testUpdateColumnNameOnUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.updateColumnName("NoSuchTable", "oldCol", "newCol");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateIndexName on a table that isn't tracked is a no-op. */
  @Test
  public void testUpdateIndexNameOnUntrackedTableIsNoOp() {
    // when
    List<Statement> stmts = service.updateIndexName("NoSuchTable", "oldIdx", "newIdx");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateIndexName on a tracked table with unknown index is a no-op. */
  @Test
  public void testUpdateIndexNameOnUnknownIndexIsNoOp() {
    // given -- table tracked, but only has Idx1
    service.trackIndex("Table1", index("Idx1").columns("col1"));

    // when
    List<Statement> stmts = service.updateIndexName("Table1", "DifferentIdx", "NewIdx");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateColumnName matches columns case-insensitively. */
  @Test
  public void testUpdateColumnNameIsCaseInsensitive() {
    // given -- column stored in mixed case
    service.trackIndex("Table1", index("Idx1").columns("MyCol"));

    // when -- upper-case lookup
    List<Statement> stmts = service.updateColumnName("Table1", "MYCOL", "newName");

    // then -- matches and emits the UPDATE
    assertEquals(1, stmts.size());
  }


  /** trackIndex for a multi-column index emits an INSERT whose indexColumns
   *  value is the columns comma-joined in the order they were declared. */
  @Test
  public void testTrackMultiColumnIndexJoinsCommaSeparated() {
    // given
    Index idx = index("Multi").columns("a", "b", "c");

    // when
    List<Statement> stmts = service.trackIndex("Table1", idx);

    // then -- the INSERT statement has a FieldLiteral "a,b,c" among its values
    assertEquals(1, stmts.size());
    org.alfasoftware.morf.sql.InsertStatement insert = (org.alfasoftware.morf.sql.InsertStatement) stmts.get(0);
    boolean sawJoined = insert.getValues().stream()
        .filter(f -> f instanceof org.alfasoftware.morf.sql.element.FieldLiteral)
        .map(f -> ((org.alfasoftware.morf.sql.element.FieldLiteral) f).getValue())
        .anyMatch("a,b,c"::equals);
    assertTrue("multi-column indexColumns should be comma-joined in declared order", sawJoined);
  }
}
