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

package org.alfasoftware.morf.upgrade.deferredindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.Operator;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexSessionImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexSessionImpl {

  private DeferredIndexSessionImpl session;

  @Before
  public void setUp() {
    session = new DeferredIndexSessionImpl(new DeferredIndexesStatements());
  }


  /**
   * prime populates the in-session map from a persisted registration row
   * without emitting any DML. After priming, isRegistered / isRegistered
   * must return true so subsequent remove/rename/etc. calls correctly
   * produce DML against the persisted row.
   */
  @Test
  public void testPrimeSeedsInSessionStateWithoutEmittingDml() {
    // given — a persisted deferred row
    DeferredIndex entry = new DeferredIndex();
    entry.setTableName("Product");
    entry.setIndexName("Product_Name_1");
    entry.setIndexUnique(false);
    entry.setIndexColumns(List.of("name"));
    entry.setStatus(DeferredIndexStatus.PENDING);

    // when
    session.prime(entry);

    // then — state is seeded
    assertTrue("Primed entry should be registered as deferred", session.isRegistered("Product", "Product_Name_1"));

    // and — a subsequent removeIndex produces a DELETE DML (not a no-op),
    // because the primed row is treated as if it existed in-session.
    List<? extends Statement> deleteStmts = session.unregisterIndex("Product", "Product_Name_1");
    assertEquals("removeIndex on primed row should emit one DELETE", 1, deleteStmts.size());
  }


  /** registerIndex should register and return INSERT statement. */
  @Test
  public void testRegisterIndexReturnsInsert() {
    // given
    Index idx = index("Idx1").columns("col1");

    // when
    List<? extends Statement> stmts = session.registerIndex("Table1", idx);

    // then
    assertEquals(1, stmts.size());
    assertTrue("Should be registered", session.isRegistered("Table1", "Idx1"));
    assertTrue("Should contain DeferredIndexes", stmts.get(0).toString().contains("DeferredIndexes"));
  }


  /** registerIndex for deferred should set isRegistered. In the slim
   *  invariant the visitor only ever calls registerIndex for deferred indexes,
   *  so there is no non-deferred case to test. */
  @Test
  public void testRegisterDeferredIndex() {
    // given
    Index idx = index("Idx1").deferred().columns("col1");

    // when
    session.registerIndex("Table1", idx);

    // then
    assertTrue("Should be registered as deferred", session.isRegistered("Table1", "Idx1"));
  }


  /** isRegistered should be case-insensitive. */
  @Test
  public void testIsRegisteredCaseInsensitive() {
    // given
    session.registerIndex("MyTable", index("MyIdx").columns("col1"));

    // then
    assertTrue(session.isRegistered("MYTABLE", "MYIDX"));
    assertTrue(session.isRegistered("mytable", "myidx"));
  }


  /** removeIndex should return DELETE and unregister. */
  @Test
  public void testUnregisterIndex() {
    // given
    session.registerIndex("Table1", index("Idx1").columns("col1"));

    // when
    List<? extends Statement> stmts = session.unregisterIndex("Table1", "Idx1");

    // then
    assertEquals(1, stmts.size());
    assertFalse("Should be unregistered", session.isRegistered("Table1", "Idx1"));
  }


  /** removeIndex for non-registered should return empty. */
  @Test
  public void testUnregisterUnknownIndex() {
    // when
    List<? extends Statement> stmts = session.unregisterIndex("Table1", "NonExistent");

    // then
    assertTrue("Should return empty", stmts.isEmpty());
  }


  /** unregisterAllFor should remove all indexes for that table. */
  @Test
  public void testUnregisterAllForTable() {
    // given
    session.registerIndex("Table1", index("Idx1").columns("col1"));
    session.registerIndex("Table1", index("Idx2").columns("col2"));
    session.registerIndex("Table2", index("Idx3").columns("col3"));

    // when
    List<? extends Statement> stmts = session.unregisterAllFor("Table1");

    // then
    assertEquals(1, stmts.size());
    assertFalse(session.isRegistered("Table1", "Idx1"));
    assertFalse(session.isRegistered("Table1", "Idx2"));
    assertTrue("Table2 should be unaffected", session.isRegistered("Table2", "Idx3"));
  }


  /** unregisterByColumn should remove matching indexes. */
  @Test
  public void testUnregisterByColumn() {
    // given
    session.registerIndex("Table1", index("Idx1").columns("col1", "col2"));
    session.registerIndex("Table1", index("Idx2").columns("col3"));

    // when
    List<? extends Statement> stmts = session.unregisterByColumn("Table1", "col1");

    // then
    assertFalse("Idx1 should be removed", session.isRegistered("Table1", "Idx1"));
    assertTrue("Idx2 should remain", session.isRegistered("Table1", "Idx2"));
  }


  /** updateTableName should update registered entries. */
  @Test
  public void testUpdateTableName() {
    // given
    session.registerIndex("OldTable", index("Idx1").columns("col1"));

    // when
    List<? extends Statement> stmts = session.updateTableName("OldTable", "NewTable");

    // then
    assertEquals(1, stmts.size());
    assertFalse(session.isRegistered("OldTable", "Idx1"));
    assertTrue(session.isRegistered("NewTable", "Idx1"));
  }


  /** updateIndexName should rename in registration. */
  @Test
  public void testUpdateIndexName() {
    // given
    session.registerIndex("Table1", index("OldIdx").columns("col1"));

    // when
    List<? extends Statement> stmts = session.updateIndexName("Table1", "OldIdx", "NewIdx");

    // then
    assertEquals(1, stmts.size());
    assertFalse(session.isRegistered("Table1", "OldIdx"));
    assertTrue(session.isRegistered("Table1", "NewIdx"));
  }


  /** updateColumnName should update column references on the matching index only,
   *  emit an UPDATE whose SET targets indexColumns with the renamed CSV, and
   *  filter on (tableName, indexName) of the affected index. */
  @Test
  public void testUpdateColumnName() {
    // given
    session.registerIndex("Table1", index("Idx1").columns("oldCol", "col2"));
    session.registerIndex("Table1", index("Idx2").columns("col3"));

    // when
    List<? extends Statement> stmts = session.updateColumnName("Table1", "oldCol", "newCol");

    // then -- only Idx1 is affected
    assertEquals("Only Idx1 should be affected", 1, stmts.size());

    // and -- UPDATE sets indexColumns="newCol,col2" and filters on (Table1, Idx1)
    UpdateStatement upd = (UpdateStatement) stmts.get(0);
    assertEquals(1, upd.getFields().size());
    assertEquals("indexColumns", upd.getFields().get(0).getAlias());
    assertEquals("newCol,col2", ((FieldLiteral) upd.getFields().get(0)).getValue());
    Criterion where = upd.getWhereCriterion();
    assertEquals(Operator.AND, where.getOperator());
    assertEquals(2, where.getCriteria().size());
  }


  // ---- Negative / no-op paths -------------------------------------------

  /** removeIndex for an unregistered (table, index) pair is a no-op. */
  @Test
  public void testUnregisterIndexOnUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.unregisterIndex("NoSuchTable", "NoSuchIdx");

    // then
    assertTrue("no-op should return empty list", stmts.isEmpty());
  }


  /** unregisterAllFor on a table that isn't registered is a no-op. */
  @Test
  public void testUnregisterAllForUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.unregisterAllFor("NoSuchTable");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** unregisterByColumn on an unregistered table is a no-op. */
  @Test
  public void testUnregisterByColumnOnUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.unregisterByColumn("NoSuchTable", "anyCol");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateTableName on a table that isn't registered is a no-op. */
  @Test
  public void testUpdateTableNameOnUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.updateTableName("NoSuchTable", "NewName");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateColumnName on a table that isn't registered is a no-op. */
  @Test
  public void testUpdateColumnNameOnUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.updateColumnName("NoSuchTable", "oldCol", "newCol");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateIndexName on a table that isn't registered is a no-op. */
  @Test
  public void testUpdateIndexNameOnUnregisteredTableIsNoOp() {
    // when
    List<? extends Statement> stmts = session.updateIndexName("NoSuchTable", "oldIdx", "newIdx");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateIndexName on a registered table with unknown index is a no-op. */
  @Test
  public void testUpdateIndexNameOnUnknownIndexIsNoOp() {
    // given -- table registered, but only has Idx1
    session.registerIndex("Table1", index("Idx1").columns("col1"));

    // when
    List<? extends Statement> stmts = session.updateIndexName("Table1", "DifferentIdx", "NewIdx");

    // then
    assertTrue(stmts.isEmpty());
  }


  /** updateColumnName matches columns case-insensitively. */
  @Test
  public void testUpdateColumnNameIsCaseInsensitive() {
    // given -- column stored in mixed case
    session.registerIndex("Table1", index("Idx1").columns("MyCol"));

    // when -- upper-case lookup
    List<? extends Statement> stmts = session.updateColumnName("Table1", "MYCOL", "newName");

    // then -- matches and emits the UPDATE
    assertEquals(1, stmts.size());
  }


  /** registerIndex for a multi-column index emits an INSERT whose indexColumns
   *  value is the columns comma-joined in the order they were declared. */
  @Test
  public void testRegisterMultiColumnIndexJoinsCommaSeparated() {
    // given
    Index idx = index("Multi").columns("a", "b", "c");

    // when
    List<? extends Statement> stmts = session.registerIndex("Table1", idx);

    // then -- the INSERT statement has a FieldLiteral "a,b,c" among its values
    assertEquals(1, stmts.size());
    InsertStatement insert = (InsertStatement) stmts.get(0);
    boolean sawJoined = insert.getValues().stream()
        .filter(f -> f instanceof FieldLiteral)
        .map(f -> ((FieldLiteral) f).getValue())
        .anyMatch("a,b,c"::equals);
    assertTrue("multi-column indexColumns should be comma-joined in declared order", sawJoined);
  }
}
