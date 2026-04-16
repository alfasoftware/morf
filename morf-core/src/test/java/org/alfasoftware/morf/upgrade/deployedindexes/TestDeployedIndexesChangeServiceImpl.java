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
    List<Statement> stmts = service.trackIndex("Table1", idx, "uuid-1");

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
    service.trackIndex("Table1", idx, null);

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
    service.trackIndex("Table1", idx, null);

    // then
    assertTrue("Should be tracked", service.isTracked("Table1", "Idx1"));
    assertFalse("Should not be tracked as deferred", service.isTrackedDeferred("Table1", "Idx1"));
  }


  /** isTracked should be case-insensitive. */
  @Test
  public void testIsTrackedCaseInsensitive() {
    // given
    service.trackIndex("MyTable", index("MyIdx").columns("col1"), null);

    // then
    assertTrue(service.isTracked("MYTABLE", "MYIDX"));
    assertTrue(service.isTracked("mytable", "myidx"));
  }


  /** removeIndex should return DELETE and untrack. */
  @Test
  public void testRemoveIndex() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("col1"), null);

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
    service.trackIndex("Table1", index("Idx1").columns("col1"), null);
    service.trackIndex("Table1", index("Idx2").columns("col2"), null);
    service.trackIndex("Table2", index("Idx3").columns("col3"), null);

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
    service.trackIndex("Table1", index("Idx1").columns("col1", "col2"), null);
    service.trackIndex("Table1", index("Idx2").columns("col3"), null);

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
    service.trackIndex("OldTable", index("Idx1").columns("col1"), null);

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
    service.trackIndex("Table1", index("OldIdx").columns("col1"), null);

    // when
    List<Statement> stmts = service.updateIndexName("Table1", "OldIdx", "NewIdx");

    // then
    assertEquals(1, stmts.size());
    assertFalse(service.isTracked("Table1", "OldIdx"));
    assertTrue(service.isTracked("Table1", "NewIdx"));
  }


  /** updateColumnName should update column references. */
  @Test
  public void testUpdateColumnName() {
    // given
    service.trackIndex("Table1", index("Idx1").columns("oldCol", "col2"), null);
    service.trackIndex("Table1", index("Idx2").columns("col3"), null);

    // when
    List<Statement> stmts = service.updateColumnName("Table1", "oldCol", "newCol");

    // then
    assertEquals("Only Idx1 should be affected", 1, stmts.size());
  }
}
