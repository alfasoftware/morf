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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexesStatements}. Asserts DSL shape — not the
 * SQL dialect output, which varies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesStatements {

  private final DeployedIndexesStatements statements = new DeployedIndexesStatements();


  // ---- Read queries ------------------------------------------------------

  /** selectAll projects all columns and orders by id. */
  @Test
  public void testSelectAll() {
    // when
    SelectStatement stmt = statements.selectAll();

    // then -- targets the correct table, orders by id
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(1, stmt.getOrderBys().size());
    assertEquals("id", ((FieldReference) stmt.getOrderBys().get(0)).getName());
    // and -- projects all 11 tracked columns (indexDeferred dropped in SP5 slim)
    assertEquals(11, stmt.getFields().size());
  }


  /** selectNonTerminal uses an OR across three statuses. */
  @Test
  public void testSelectNonTerminal() {
    // when
    SelectStatement stmt = statements.selectNonTerminal();

    // then -- WHERE is OR(status=PENDING, status=IN_PROGRESS, status=FAILED)
    assertNotNull(stmt.getWhereCriterion());
    assertEquals(Operator.OR, stmt.getWhereCriterion().getOperator());
    assertEquals(3, stmt.getWhereCriterion().getCriteria().size());
  }


  /** selectStatusColumn is a single-field projection of status. */
  @Test
  public void testSelectStatusColumn() {
    // when
    SelectStatement stmt = statements.selectStatusColumn();

    // then
    assertEquals(1, stmt.getFields().size());
  }


  // ---- Status update statements ------------------------------------------

  /** markStarted sets status=IN_PROGRESS and startedTime, filters on (tableName, indexName). */
  @Test
  public void testMarkStarted() {
    // when
    UpdateStatement stmt = statements.markStarted("Product", "Idx1", 12345L);

    // then -- SET status=IN_PROGRESS, startedTime=12345
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(List.of("status", "startedTime"), aliases(stmt.getFields()));
    assertEquals(List.of(DeployedIndexStatus.IN_PROGRESS.name(), "12345"), literalValues(stmt.getFields()));
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** markCompleted sets status=COMPLETED and completedTime, filters on (tableName, indexName). */
  @Test
  public void testMarkCompleted() {
    // when
    UpdateStatement stmt = statements.markCompleted("Product", "Idx1", 12345L);

    // then
    assertEquals(List.of("status", "completedTime"), aliases(stmt.getFields()));
    assertEquals(List.of(DeployedIndexStatus.COMPLETED.name(), "12345"), literalValues(stmt.getFields()));
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** markFailed sets status=FAILED and errorMessage, filters on (tableName, indexName). */
  @Test
  public void testMarkFailed() {
    // when
    UpdateStatement stmt = statements.markFailed("Product", "Idx1", "boom");

    // then
    assertEquals(List.of("status", "errorMessage"), aliases(stmt.getFields()));
    assertEquals(List.of(DeployedIndexStatus.FAILED.name(), "boom"), literalValues(stmt.getFields()));
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** resetInProgress sets status=PENDING, filters on status=IN_PROGRESS. */
  @Test
  public void testResetInProgress() {
    // when
    UpdateStatement stmt = statements.resetInProgress();

    // then -- SET status=PENDING
    assertEquals(List.of("status"), aliases(stmt.getFields()));
    assertEquals(List.of(DeployedIndexStatus.PENDING.name()), literalValues(stmt.getFields()));
    // and -- WHERE status=IN_PROGRESS
    Criterion where = stmt.getWhereCriterion();
    assertEquals(Operator.EQ, where.getOperator());
    assertEquals("status", ((FieldReference) where.getField()).getName());
    assertEquals(DeployedIndexStatus.IN_PROGRESS.name(), where.getValue());
  }


  // ---- Tracking DML ------------------------------------------------------

  /** trackIndex produces an INSERT against the DeployedIndexes table with
   *  status=PENDING for a deferred index (slim: only deferred gets tracked). */
  @Test
  public void testTrackDeferredIndex() {
    // given
    Index idx = index("DeferIdx").deferred().columns("col1", "col2");

    // when
    InsertStatement stmt = statements.trackIndex("Product", idx);

    // then -- 8 values corresponding to the 8 columns the factory populates
    // (id, tableName, indexName, indexUnique, indexColumns, status, retryCount, createdTime)
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(8, stmt.getValues().size());
    // and -- status literal should be PENDING
    boolean sawPending = stmt.getValues().stream()
        .filter(f -> f instanceof FieldLiteral)
        .map(f -> ((FieldLiteral) f).getValue())
        .anyMatch(v -> DeployedIndexStatus.PENDING.name().equals(v));
    assertTrue("deferred track should emit PENDING", sawPending);
  }


  /** Multi-column indexes produce a comma-joined indexColumns value. */
  @Test
  public void testMultiColumnTrackIndexJoinsCommaSeparated() {
    // given
    Index idx = index("MultiIdx").columns("a", "b", "c");

    // when
    InsertStatement stmt = statements.trackIndex("Product", idx);

    // then -- one of the literals should be "a,b,c"
    boolean sawJoined = stmt.getValues().stream()
        .filter(f -> f instanceof FieldLiteral)
        .map(f -> ((FieldLiteral) f).getValue())
        .anyMatch("a,b,c"::equals);
    assertTrue("multi-column indexColumns should be comma-joined", sawJoined);
  }


  /** removeIndex produces a DELETE with WHERE on (tableName, indexName). */
  @Test
  public void testRemoveIndex() {
    // when
    DeleteStatement stmt = statements.removeIndex("Product", "Idx1");

    // then
    assertEquals(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME,
        stmt.getTable().getName());
    assertNotNull(stmt.getWhereCriterion());
  }


  /** removeAllForTable produces a DELETE with WHERE on tableName only. */
  @Test
  public void testRemoveAllForTable() {
    // when
    DeleteStatement stmt = statements.removeAllForTable("Product");

    // then
    assertNotNull(stmt.getWhereCriterion());
  }


  /** updateTableName produces an UPDATE SETTING tableName WHERE old name. */
  @Test
  public void testUpdateTableName() {
    // when
    UpdateStatement stmt = statements.updateTableName("OldT", "NewT");

    // then
    assertEquals(1, stmt.getFields().size());
    assertNotNull(stmt.getWhereCriterion());
  }


  /** updateIndexColumns produces an UPDATE SETTING indexColumns WHERE (table, index). */
  @Test
  public void testUpdateIndexColumns() {
    // when
    UpdateStatement stmt = statements.updateIndexColumns("Product", "Idx1", "newCol");

    // then
    assertEquals(1, stmt.getFields().size());
    assertNotNull(stmt.getWhereCriterion());
  }


  /** updateIndexName produces an UPDATE SETTING indexName WHERE old name. */
  @Test
  public void testUpdateIndexName() {
    // when
    UpdateStatement stmt = statements.updateIndexName("Product", "Old", "New");

    // then
    assertEquals(1, stmt.getFields().size());
    assertNotNull(stmt.getWhereCriterion());
  }


  // ---- Helpers -----------------------------------------------------------

  private static List<String> aliases(List<AliasedField> fields) {
    return fields.stream().map(AliasedField::getAlias).collect(Collectors.toList());
  }


  private static List<String> literalValues(List<AliasedField> fields) {
    return fields.stream()
        .map(f -> f instanceof FieldLiteral ? ((FieldLiteral) f).getValue() : null)
        .collect(Collectors.toList());
  }


  /**
   * Assert that a WHERE criterion is an AND of exactly two EQ leaves:
   * {@code tableName=<t>} and {@code indexName=<i>}, in any order.
   */
  private static void assertWhereOnTableAndIndex(Criterion where, String expectedTable, String expectedIndex) {
    assertNotNull(where);
    assertEquals(Operator.AND, where.getOperator());
    List<Criterion> leaves = where.getCriteria();
    assertEquals("AND should have exactly two leaves", 2, leaves.size());
    List<String> fieldNames = leaves.stream()
        .map(c -> ((FieldReference) c.getField()).getName())
        .sorted()
        .collect(Collectors.toList());
    List<Object> values = leaves.stream()
        .map(Criterion::getValue)
        .collect(Collectors.toList());
    assertEquals(List.of("indexName", "tableName"), fieldNames);
    assertTrue("values should include the expected table: " + values, values.contains(expectedTable));
    assertTrue("values should include the expected index: " + values, values.contains(expectedIndex));
  }
}
