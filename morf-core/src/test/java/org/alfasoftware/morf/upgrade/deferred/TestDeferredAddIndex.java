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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.SchemaChangeVisitor;
import org.mockito.ArgumentMatchers;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link DeferredAddIndex}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredAddIndex {

  /** Table with no indexes used as a starting point in most tests. */
  private Table appleTable;

  /** Subject under test with a simple unique index on "pips". */
  private DeferredAddIndex deferredAddIndex;


  /**
   * Set up a fresh table and a {@link DeferredAddIndex} before each test.
   */
  @Before
  public void setUp() {
    appleTable = table("Apple").columns(
      column("pips", DataType.STRING, 10).nullable(),
      column("colour", DataType.STRING, 10).nullable()
    );

    deferredAddIndex = new DeferredAddIndex("Apple", index("Apple_1").unique().columns("pips"), "test-uuid-1234");
  }


  /**
   * Verify that apply() adds the index to the in-memory schema.
   */
  @Test
  public void testApplyAddsIndexToSchema() {
    Schema result = deferredAddIndex.apply(schema(appleTable));

    Table resultTable = result.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post-apply index count", 1, resultTable.indexes().size());
    assertEquals("Post-apply index name", "Apple_1", resultTable.indexes().get(0).getName());
    assertEquals("Post-apply index column", "pips", resultTable.indexes().get(0).columnNames().get(0));
    assertTrue("Post-apply index unique", resultTable.indexes().get(0).isUnique());
  }


  /**
   * Verify that apply() throws when the target table does not exist in the schema.
   */
  @Test
  public void testApplyThrowsWhenTableMissing() {
    DeferredAddIndex missingTable = new DeferredAddIndex("NoSuchTable", index("NoSuchTable_1").columns("pips"), "");
    try {
      missingTable.apply(schema(appleTable));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("NoSuchTable"));
    }
  }


  /**
   * Verify that apply() throws when the index already exists on the table.
   */
  @Test
  public void testApplyThrowsWhenIndexAlreadyExists() {
    Table tableWithIndex = table("Apple").columns(
      column("pips", DataType.STRING, 10).nullable()
    ).indexes(
      index("Apple_1").unique().columns("pips")
    );

    try {
      deferredAddIndex.apply(schema(tableWithIndex));
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Apple_1"));
    }
  }


  /**
   * Verify that reverse() removes the index from the in-memory schema.
   */
  @Test
  public void testReverseRemovesIndexFromSchema() {
    Table tableWithIndex = table("Apple").columns(
      column("pips", DataType.STRING, 10).nullable(),
      column("colour", DataType.STRING, 10).nullable()
    ).indexes(
      index("Apple_1").unique().columns("pips")
    );

    Schema result = deferredAddIndex.reverse(schema(tableWithIndex));

    Table resultTable = result.getTable("Apple");
    assertNotNull(resultTable);
    assertEquals("Post-reverse index count", 0, resultTable.indexes().size());
  }


  /**
   * Verify that reverse() throws when the index to remove is not present.
   */
  @Test
  public void testReverseThrowsWhenIndexNotFound() {
    try {
      deferredAddIndex.reverse(schema(appleTable));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Apple_1"));
    }
  }


  /**
   * Verify that isApplied() returns true when the index already exists in the database schema.
   */
  @Test
  public void testIsAppliedTrueWhenIndexExistsInSchema() {
    Table tableWithIndex = table("Apple").columns(
      column("pips", DataType.STRING, 10).nullable()
    ).indexes(
      index("Apple_1").unique().columns("pips")
    );

    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    DeferredAddIndex subject = new DeferredAddIndex("Apple", index("Apple_1").unique().columns("pips"), "", mockDao);

    assertTrue("Should be applied when index exists in schema",
      subject.isApplied(schema(tableWithIndex), null));
    verify(mockDao, never()).existsByTableNameAndIndexName(ArgumentMatchers.any(), ArgumentMatchers.any());
  }


  /**
   * Verify that isApplied() returns true when a matching record exists in the deferred queue,
   * even if the index is not yet in the database schema.
   */
  @Test
  public void testIsAppliedTrueWhenOperationInQueue() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.existsByTableNameAndIndexName("Apple", "Apple_1")).thenReturn(true);

    DeferredAddIndex subject = new DeferredAddIndex("Apple", index("Apple_1").unique().columns("pips"), "", mockDao);

    assertTrue("Should be applied when operation is queued",
      subject.isApplied(schema(appleTable), null));
    verify(mockDao).existsByTableNameAndIndexName("Apple", "Apple_1");
  }


  /**
   * Verify that isApplied() returns false when the index is absent from both
   * the database schema and the deferred queue.
   */
  @Test
  public void testIsAppliedFalseWhenNeitherSchemaNorQueue() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.existsByTableNameAndIndexName("Apple", "Apple_1")).thenReturn(false);

    DeferredAddIndex subject = new DeferredAddIndex("Apple", index("Apple_1").unique().columns("pips"), "", mockDao);

    assertFalse("Should not be applied when neither in schema nor queued",
      subject.isApplied(schema(appleTable), null));
  }


  /**
   * Verify that isApplied() returns false when the table is not present in the schema.
   */
  @Test
  public void testIsAppliedFalseWhenTableMissingFromSchema() {
    DeferredIndexOperationDAO mockDao = mock(DeferredIndexOperationDAO.class);
    when(mockDao.existsByTableNameAndIndexName("Apple", "Apple_1")).thenReturn(false);

    DeferredAddIndex subject = new DeferredAddIndex("Apple", index("Apple_1").unique().columns("pips"), "", mockDao);

    assertFalse("Should not be applied when table is absent from schema",
      subject.isApplied(schema(), null));
  }


  /**
   * Verify that accept() delegates to the visitor's visit(DeferredAddIndex) method.
   */
  @Test
  public void testAcceptDelegatesToVisitor() {
    SchemaChangeVisitor visitor = mock(SchemaChangeVisitor.class);

    deferredAddIndex.accept(visitor);

    verify(visitor).visit(deferredAddIndex);
  }


  /**
   * Verify that getTableName(), getNewIndex() and getUpgradeUUID() return the values supplied at construction.
   */
  @Test
  public void testGetters() {
    assertEquals("getTableName", "Apple", deferredAddIndex.getTableName());
    assertEquals("getNewIndex name", "Apple_1", deferredAddIndex.getNewIndex().getName());
    assertEquals("getUpgradeUUID", "test-uuid-1234", deferredAddIndex.getUpgradeUUID());
  }
}
