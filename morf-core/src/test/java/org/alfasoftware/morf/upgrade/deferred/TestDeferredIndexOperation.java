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

  /** All getters should return the values set via their corresponding setters. */
  @Test
  public void testAllGettersAndSetters() {
    DeferredIndexOperation op = new DeferredIndexOperation();

    op.setId(42L);
    assertEquals(42L, op.getId());

    op.setUpgradeUUID("uuid-1234");
    assertEquals("uuid-1234", op.getUpgradeUUID());

    op.setTableName("MyTable");
    assertEquals("MyTable", op.getTableName());

    op.setIndexName("MyTable_1");
    assertEquals("MyTable_1", op.getIndexName());

    op.setIndexUnique(true);
    assertTrue(op.isIndexUnique());

    op.setStatus(DeferredIndexStatus.COMPLETED);
    assertEquals(DeferredIndexStatus.COMPLETED, op.getStatus());

    op.setRetryCount(3);
    assertEquals(3, op.getRetryCount());

    op.setCreatedTime(20260101120000L);
    assertEquals(20260101120000L, op.getCreatedTime());

    op.setStartedTime(20260101120100L);
    assertEquals(Long.valueOf(20260101120100L), op.getStartedTime());

    op.setCompletedTime(20260101120200L);
    assertEquals(Long.valueOf(20260101120200L), op.getCompletedTime());

    op.setErrorMessage("something went wrong");
    assertEquals("something went wrong", op.getErrorMessage());

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
