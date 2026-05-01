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
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexesStatements}. Asserts DSL shape — not the
 * SQL dialect output, which varies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexesStatements {

  private final DeferredIndexesStatements statements = new DeferredIndexesStatements();


  // ---- Read queries ------------------------------------------------------

  /** selectAll targets the correct table, orders by id, and projects every
   *  column (SELECT *). The DSL has no explicit field list -- mapRow reads
   *  by column name so position doesn't matter. */
  @Test
  public void testSelectAll() {
    // when
    SelectStatement stmt = statements.selectAll();

    // then -- targets the correct table, orders by id
    assertEquals(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(1, stmt.getOrderBys().size());
    assertEquals("id", ((FieldReference) stmt.getOrderBys().get(0)).getName());
    // and -- empty field list = SELECT *
    assertTrue(stmt.getFields().isEmpty());
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

  /** markStarted sets status=IN_PROGRESS, startedTime, and attemptsCount; explicitly does NOT touch errorMessage. */
  @Test
  public void testMarkStarted() {
    // when -- attempts=3 means this is the 3rd attempt (build task computed prior+1)
    UpdateStatement stmt = statements.markStarted("Product", "Idx1", 12345L, 3);

    // then -- SET status=IN_PROGRESS, startedTime=12345, attemptsCount=3
    assertEquals(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(List.of("status", "startedTime", "attemptsCount"), aliases(stmt.getFields()));
    assertEquals(List.of(DeferredIndexStatus.IN_PROGRESS.name(), "12345", "3"), literalValues(stmt.getFields()));
    // and -- errorMessage is intentionally absent so prior failure detail stays visible until success clears it
    assertFalse("markStarted must not touch errorMessage",
        aliases(stmt.getFields()).contains("errorMessage"));
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** markCompleted sets status=COMPLETED, completedTime, and clears attemptsCount + errorMessage. */
  @Test
  public void testMarkCompleted() {
    // when
    UpdateStatement stmt = statements.markCompleted("Product", "Idx1", 12345L);

    // then -- SET status, completedTime, AND reset attemptsCount=0, errorMessage=NULL
    assertEquals(List.of("status", "completedTime", "attemptsCount", "errorMessage"), aliases(stmt.getFields()));
    List<String> values = literalValues(stmt.getFields());
    assertEquals(DeferredIndexStatus.COMPLETED.name(), values.get(0));
    assertEquals("12345", values.get(1));
    assertEquals("0", values.get(2));
    // and -- errorMessage is set to a SQL NULL literal; verify the field type, not just the value
    assertTrue("errorMessage column must be set to a NullFieldLiteral, not an empty string literal: "
            + stmt.getFields().get(3).getClass().getSimpleName(),
        stmt.getFields().get(3) instanceof NullFieldLiteral);
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** markFailed sets status=FAILED and errorMessage; does not touch attemptsCount. */
  @Test
  public void testMarkFailed() {
    // when
    UpdateStatement stmt = statements.markFailed("Product", "Idx1", "boom");

    // then -- only status + errorMessage; attemptsCount was bumped at markStarted
    assertEquals(List.of("status", "errorMessage"), aliases(stmt.getFields()));
    assertEquals(List.of(DeferredIndexStatus.FAILED.name(), "boom"), literalValues(stmt.getFields()));
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  /** selectByTableAndIndex projects every column (SELECT *) and filters on
   *  (tableName, indexName). */
  @Test
  public void testSelectByTableAndIndex() {
    // when
    SelectStatement stmt = statements.selectByTableAndIndex("Product", "Idx1");

    // then -- empty field list = SELECT *, plus the WHERE criterion
    assertTrue(stmt.getFields().isEmpty());
    assertWhereOnTableAndIndex(stmt.getWhereCriterion(), "Product", "Idx1");
  }


  // ---- Registration DML ------------------------------------------------------

  /** registerIndex produces an INSERT against the DeferredIndexes table with
   *  status=PENDING for a deferred index (slim: only deferred gets registered). */
  @Test
  public void testRegisterDeferredIndex() {
    // given
    Index idx = index("DeferIdx").deferred().columns("col1", "col2");

    // when
    InsertStatement stmt = statements.registerIndex("Product", idx);

    // then -- 8 values corresponding to the 8 columns the factory populates
    // (id, tableName, indexName, indexUnique, indexColumns, status, attemptsCount, createdTime)
    assertEquals(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME,
        stmt.getTable().getName());
    assertEquals(8, stmt.getValues().size());
    // and -- status literal should be PENDING
    boolean sawPending = stmt.getValues().stream()
        .filter(f -> f instanceof FieldLiteral)
        .map(f -> ((FieldLiteral) f).getValue())
        .anyMatch(v -> DeferredIndexStatus.PENDING.name().equals(v));
    assertTrue("deferred track should emit PENDING", sawPending);
  }


  /** Multi-column indexes produce a comma-joined indexColumns value. */
  @Test
  public void testMultiColumnRegisterIndexJoinsCommaSeparated() {
    // given
    Index idx = index("MultiIdx").columns("a", "b", "c");

    // when
    InsertStatement stmt = statements.registerIndex("Product", idx);

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
    DeleteStatement stmt = statements.unregisterIndex("Product", "Idx1");

    // then
    assertEquals(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME,
        stmt.getTable().getName());
    assertNotNull(stmt.getWhereCriterion());
  }


  /** unregisterAllFor produces a DELETE with WHERE on tableName only. */
  @Test
  public void testRemoveAllForTable() {
    // when
    DeleteStatement stmt = statements.unregisterAllFor("Product");

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
