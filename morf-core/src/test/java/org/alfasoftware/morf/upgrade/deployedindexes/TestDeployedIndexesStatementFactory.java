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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesStatementFactory}. Asserts DSL
 * shape — not the SQL dialect output, which varies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesStatementFactory {

  private final DeployedIndexesStatementFactory factory = new DeployedIndexesStatementFactory();


  // ---- Read queries ------------------------------------------------------

  /** findAll projects all columns and orders by id. */
  @Test
  public void testStatementToFindAll() {
    // when
    SelectStatement stmt = factory.statementToFindAll();

    // then -- targets the correct table, orders by id
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(1, stmt.getOrderBys().size());
    assertEquals("id", ((org.alfasoftware.morf.sql.element.FieldReference) stmt.getOrderBys().get(0)).getName());
    // and -- projects all columns
    assertEquals(DeployedIndexesStatementFactory.allColumns().size(), stmt.getFields().size());
  }


  /** findByTable filters on tableName. */
  @Test
  public void testStatementToFindByTable() {
    // when
    SelectStatement stmt = factory.statementToFindByTable("Product");

    // then
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertTrue("should have a WHERE clause", stmt.getWhereCriterion() != null);
  }


  /** findNonTerminalOperations uses an OR across three statuses. */
  @Test
  public void testStatementToFindNonTerminalOperations() {
    // when
    SelectStatement stmt = factory.statementToFindNonTerminalOperations();

    // then -- WHERE is OR(status=PENDING, status=IN_PROGRESS, status=FAILED)
    assertTrue(stmt.getWhereCriterion() != null);
    assertEquals(org.alfasoftware.morf.sql.element.Operator.OR,
        stmt.getWhereCriterion().getOperator());
    assertEquals(3, stmt.getWhereCriterion().getCriteria().size());
  }


  /** statusColumn select is a single-field projection of status. */
  @Test
  public void testStatementToSelectStatusColumn() {
    // when
    SelectStatement stmt = factory.statementToSelectStatusColumn();

    // then
    assertEquals(1, stmt.getFields().size());
  }


  // ---- Status update statements ------------------------------------------

  /** markStarted sets status=IN_PROGRESS and startedTime. */
  @Test
  public void testStatementToMarkStarted() {
    // when
    UpdateStatement stmt = factory.statementToMarkStarted("Product", "Idx1", 12345L);

    // then
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(2, stmt.getFields().size());  // status + startedTime
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** markCompleted sets status=COMPLETED and completedTime. */
  @Test
  public void testStatementToMarkCompleted() {
    // when
    UpdateStatement stmt = factory.statementToMarkCompleted("Product", "Idx1", 12345L);

    // then
    assertEquals(2, stmt.getFields().size());  // status + completedTime
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** markFailed sets status=FAILED and errorMessage. */
  @Test
  public void testStatementToMarkFailed() {
    // when
    UpdateStatement stmt = factory.statementToMarkFailed("Product", "Idx1", "boom");

    // then
    assertEquals(2, stmt.getFields().size());  // status + errorMessage
  }


  /** resetInProgress filters on status=IN_PROGRESS. */
  @Test
  public void testStatementToResetInProgress() {
    // when
    UpdateStatement stmt = factory.statementToResetInProgress();

    // then
    assertEquals(1, stmt.getFields().size());  // status
    assertTrue(stmt.getWhereCriterion() != null);
  }


  // ---- Tracking DML ------------------------------------------------------

  /** trackIndex produces an INSERT against the DeployedIndexes table with
   *  status=PENDING for a deferred index. */
  @Test
  public void testStatementToTrackDeferredIndex() {
    // given
    Index idx = index("DeferIdx").deferred().columns("col1", "col2");

    // when
    InsertStatement stmt = factory.statementToTrackIndex("Product", idx);

    // then -- 9 values corresponding to the 9 columns the factory populates
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(9, stmt.getValues().size());
  }


  /** trackIndex for a non-deferred index emits status=COMPLETED. */
  @Test
  public void testStatementToTrackNonDeferredIndex() {
    // given
    Index idx = index("ImmIdx").columns("col1");

    // when
    InsertStatement stmt = factory.statementToTrackIndex("Product", idx);

    // then -- status literal should be COMPLETED (verify by scanning values)
    boolean sawCompleted = stmt.getValues().stream()
        .filter(f -> f instanceof org.alfasoftware.morf.sql.element.FieldLiteral)
        .map(f -> ((org.alfasoftware.morf.sql.element.FieldLiteral) f).getValue())
        .anyMatch(v -> DeployedIndexStatus.COMPLETED.name().equals(v));
    assertTrue("non-deferred track should emit COMPLETED", sawCompleted);
  }


  /** Multi-column indexes produce a comma-joined indexColumns value. */
  @Test
  public void testMultiColumnTrackIndexJoinsCommaSeparated() {
    // given
    Index idx = index("MultiIdx").columns("a", "b", "c");

    // when
    InsertStatement stmt = factory.statementToTrackIndex("Product", idx);

    // then -- one of the literals should be "a,b,c"
    boolean sawJoined = stmt.getValues().stream()
        .filter(f -> f instanceof org.alfasoftware.morf.sql.element.FieldLiteral)
        .map(f -> ((org.alfasoftware.morf.sql.element.FieldLiteral) f).getValue())
        .anyMatch("a,b,c"::equals);
    assertTrue("multi-column indexColumns should be comma-joined", sawJoined);
  }


  /** removeIndex produces a DELETE with WHERE on (tableName, indexName). */
  @Test
  public void testStatementToRemoveIndex() {
    // when
    DeleteStatement stmt = factory.statementToRemoveIndex("Product", "Idx1");

    // then
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** removeAllForTable produces a DELETE with WHERE on tableName only. */
  @Test
  public void testStatementToRemoveAllForTable() {
    // when
    DeleteStatement stmt = factory.statementToRemoveAllForTable("Product");

    // then
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** updateTableName produces an UPDATE SETTING tableName WHERE old name. */
  @Test
  public void testStatementToUpdateTableName() {
    // when
    UpdateStatement stmt = factory.statementToUpdateTableName("OldT", "NewT");

    // then
    assertEquals(1, stmt.getFields().size());
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** updateIndexColumns produces an UPDATE SETTING indexColumns WHERE (table, index). */
  @Test
  public void testStatementToUpdateIndexColumns() {
    // when
    UpdateStatement stmt = factory.statementToUpdateIndexColumns("Product", "Idx1", "newCol");

    // then
    assertEquals(1, stmt.getFields().size());
    assertTrue(stmt.getWhereCriterion() != null);
  }


  /** updateIndexName produces an UPDATE SETTING indexName WHERE old name. */
  @Test
  public void testStatementToUpdateIndexName() {
    // when
    UpdateStatement stmt = factory.statementToUpdateIndexName("Product", "Old", "New");

    // then
    assertEquals(1, stmt.getFields().size());
    assertTrue(stmt.getWhereCriterion() != null);
  }


  // ---- Column constants --------------------------------------------------

  /** allColumns() returns a stable 12-column list in deterministic order. */
  @Test
  public void testAllColumns() {
    // when
    List<String> cols = DeployedIndexesStatementFactory.allColumns();

    // then
    assertEquals(12, cols.size());
    assertEquals("id", cols.get(0));
    assertEquals("errorMessage", cols.get(cols.size() - 1));
  }
}
