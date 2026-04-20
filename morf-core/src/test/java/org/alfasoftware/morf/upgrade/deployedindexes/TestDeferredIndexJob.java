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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexJob}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexJob {

  /** Getters return what the constructor received. */
  @Test
  public void testGettersReturnConstructorArgs() {
    // given
    DeferredIndexJob job = new DeferredIndexJob("Product", "Idx1", List.of("CREATE INDEX ..."));

    // then
    assertEquals("Product", job.getTableName());
    assertEquals("Idx1", job.getIndexName());
    assertEquals(List.of("CREATE INDEX ..."), job.getSql());
  }


  /** Multi-statement SQL (e.g. PostgreSQL CREATE + COMMENT) preserves order. */
  @Test
  public void testMultipleStatementsPreservedInOrder() {
    // given
    List<String> sql = List.of("CREATE INDEX Idx1 ...", "COMMENT ON INDEX Idx1 IS '...'");
    DeferredIndexJob job = new DeferredIndexJob("Product", "Idx1", sql);

    // then
    assertEquals(sql, job.getSql());
  }


  /** getSql() returns an unmodifiable list — structural mutations throw. */
  @Test
  public void testSqlListIsUnmodifiable() {
    // given
    DeferredIndexJob job = new DeferredIndexJob("Product", "Idx1", List.of("sql"));

    // when / then
    assertThrows(UnsupportedOperationException.class, () -> job.getSql().add("mutated"));
    assertThrows(UnsupportedOperationException.class, () -> job.getSql().remove(0));
  }


  /** The job's SQL is decoupled from the caller's mutable input list. */
  @Test
  public void testSqlListDecoupledFromCallerInput() {
    // given -- caller passes a mutable list
    List<String> callerList = new ArrayList<>(Arrays.asList("first"));
    DeferredIndexJob job = new DeferredIndexJob("Product", "Idx1", callerList);

    // when -- caller mutates their original
    callerList.add("second");
    callerList.clear();

    // then -- job is unaffected
    assertEquals(List.of("first"), job.getSql());
  }


  /** Null tableName fails fast with a clear message. */
  @Test
  public void testNullTableNameThrows() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> new DeferredIndexJob(null, "Idx1", List.of("sql")));
    if (e.getMessage() == null || !e.getMessage().contains("tableName")) {
      fail("NPE should mention the parameter name; got: " + e.getMessage());
    }
  }


  /** Null indexName fails fast with a clear message. */
  @Test
  public void testNullIndexNameThrows() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> new DeferredIndexJob("Product", null, List.of("sql")));
    if (e.getMessage() == null || !e.getMessage().contains("indexName")) {
      fail("NPE should mention the parameter name; got: " + e.getMessage());
    }
  }


  /** Null sql fails fast with a clear message. */
  @Test
  public void testNullSqlThrows() {
    NullPointerException e = assertThrows(NullPointerException.class,
        () -> new DeferredIndexJob("Product", "Idx1", null));
    if (e.getMessage() == null || !e.getMessage().contains("sql")) {
      fail("NPE should mention the parameter name; got: " + e.getMessage());
    }
  }
}
