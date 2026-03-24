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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

/**
 * Tests for the {@link DeferredIndexOperation} POJO, covering all
 * getters and setters.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexOperation {

  /** The id field should return the value set via setId. */
  @Test
  public void testId() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(42L);
    assertEquals(42L, op.getId());
  }


  /** The upgradeUUID field should return the value set via setUpgradeUUID. */
  @Test
  public void testUpgradeUUID() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setUpgradeUUID("uuid-1234");
    assertEquals("uuid-1234", op.getUpgradeUUID());
  }


  /** The tableName field should return the value set via setTableName. */
  @Test
  public void testTableName() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setTableName("MyTable");
    assertEquals("MyTable", op.getTableName());
  }


  /** The indexName field should return the value set via setIndexName. */
  @Test
  public void testIndexName() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setIndexName("MyTable_1");
    assertEquals("MyTable_1", op.getIndexName());
  }


  /** The indexUnique field should default to false and return the value set via setIndexUnique. */
  @Test
  public void testIndexUnique() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    assertFalse(op.isIndexUnique());
    op.setIndexUnique(true);
    assertTrue(op.isIndexUnique());
  }


  /** The status field should return the value set via setStatus. */
  @Test
  public void testStatus() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setStatus(DeferredIndexStatus.COMPLETED);
    assertEquals(DeferredIndexStatus.COMPLETED, op.getStatus());
  }


  /** The retryCount field should return the value set via setRetryCount. */
  @Test
  public void testRetryCount() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setRetryCount(3);
    assertEquals(3, op.getRetryCount());
  }


  /** The createdTime field should return the value set via setCreatedTime. */
  @Test
  public void testCreatedTime() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setCreatedTime(20260101120000L);
    assertEquals(20260101120000L, op.getCreatedTime());
  }


  /** The startedTime field is nullable and should return the value set via setStartedTime. */
  @Test
  public void testStartedTime() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setStartedTime(20260101120100L);
    assertEquals(Long.valueOf(20260101120100L), op.getStartedTime());
  }


  /** The completedTime field is nullable and should return the value set via setCompletedTime. */
  @Test
  public void testCompletedTime() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setCompletedTime(20260101120200L);
    assertEquals(Long.valueOf(20260101120200L), op.getCompletedTime());
  }


  /** The errorMessage field is nullable and should return the value set via setErrorMessage. */
  @Test
  public void testErrorMessage() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setErrorMessage("something went wrong");
    assertEquals("something went wrong", op.getErrorMessage());
  }


  /** The columnNames field stores ordered column names. */
  @Test
  public void testColumnNames() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setColumnNames(List.of("col1", "col2"));
    assertEquals(List.of("col1", "col2"), op.getColumnNames());
  }


  /** Nullable fields should default to null before being set. */
  @Test
  public void testNullableFieldsDefaultToNull() {
    DeferredIndexOperation op = new DeferredIndexOperation();
    assertNull(op.getStartedTime());
    assertNull(op.getCompletedTime());
    assertNull(op.getErrorMessage());
  }
}
