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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexServiceImpl}. Verifies that
 * {@link DeferredIndexService#getBuildTasks()} fans non-{@code COMPLETED} rows
 * out into one task per row, and that {@link DeferredIndexService#getProgress()}
 * delegates straight to the DAO.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexServiceImpl {

  private DeferredIndexBuilder builder;
  private DeferredIndexesDAO dao;
  private DeferredIndexServiceImpl service;


  @Before
  public void setUp() {
    builder = mock(DeferredIndexBuilder.class);
    dao = mock(DeferredIndexesDAO.class);
    service = new DeferredIndexServiceImpl(builder, dao);
  }


  /** getBuildTasks returns one task per non-terminal row, preserving table/index identity. */
  @Test
  public void testGetBuildTasksOneTaskPerNonTerminalRow() {
    when(dao.findNonTerminal()).thenReturn(List.of(
        row("Product", "Idx_A", DeferredIndexStatus.PENDING),
        row("Customer", "Idx_B", DeferredIndexStatus.IN_PROGRESS),
        row("Order", "Idx_C", DeferredIndexStatus.FAILED)));

    List<DeferredIndexBuildTask> tasks = service.getBuildTasks();

    assertEquals(3, tasks.size());
    assertEquals("Product", tasks.get(0).getTableName());
    assertEquals("Idx_A", tasks.get(0).getIndexName());
    assertEquals("Customer", tasks.get(1).getTableName());
    assertEquals("Idx_B", tasks.get(1).getIndexName());
    assertEquals("Order", tasks.get(2).getTableName());
    assertEquals("Idx_C", tasks.get(2).getIndexName());
  }


  /** getBuildTasks returns empty when the DAO has no non-terminal rows. */
  @Test
  public void testGetBuildTasksEmptyWhenAllCompleted() {
    when(dao.findNonTerminal()).thenReturn(List.of());

    assertTrue(service.getBuildTasks().isEmpty());
  }


  /** Returned list is unmodifiable so callers can't mutate it after dispatch. */
  @Test
  public void testGetBuildTasksReturnsUnmodifiableList() {
    when(dao.findNonTerminal()).thenReturn(List.of(row("Product", "Idx", DeferredIndexStatus.PENDING)));

    List<DeferredIndexBuildTask> tasks = service.getBuildTasks();

    assertThrows(UnsupportedOperationException.class, () -> tasks.add(null));
  }


  /**
   * Each returned task carries a snapshot of its row's status/attemptsCount/errorMessage
   * so adopters can implement caps (e.g. skip retrying after N attempts) and surface
   * per-row diagnostics without an extra DB query.
   */
  @Test
  public void testGetBuildTasksExposesRowSnapshotForAdopterFiltering() {
    DeferredIndex r1 = row("Product", "Idx_OK", DeferredIndexStatus.PENDING);
    r1.setAttemptsCount(0);
    DeferredIndex r2 = row("Product", "Idx_Failing", DeferredIndexStatus.FAILED);
    r2.setAttemptsCount(7);
    r2.setErrorMessage("unique constraint violated");
    when(dao.findNonTerminal()).thenReturn(List.of(r1, r2));

    List<DeferredIndexBuildTask> tasks = service.getBuildTasks();

    assertEquals(DeferredIndexStatus.PENDING, tasks.get(0).getStatus());
    assertEquals(0, tasks.get(0).getAttemptsCount());
    assertEquals(Optional.empty(), tasks.get(0).getErrorMessage());

    assertEquals(DeferredIndexStatus.FAILED, tasks.get(1).getStatus());
    assertEquals(7, tasks.get(1).getAttemptsCount());
    assertEquals(Optional.of("unique constraint violated"), tasks.get(1).getErrorMessage());
  }


  /** getProgress delegates the count map straight from the DAO (same instance, no copy). */
  @Test
  public void testGetProgressDelegatesToDao() {
    Map<DeferredIndexStatus, Integer> counts = new EnumMap<>(DeferredIndexStatus.class);
    counts.put(DeferredIndexStatus.PENDING, 2);
    counts.put(DeferredIndexStatus.IN_PROGRESS, 1);
    counts.put(DeferredIndexStatus.COMPLETED, 5);
    counts.put(DeferredIndexStatus.FAILED, 0);
    when(dao.getProgressCounts()).thenReturn(counts);

    assertSame(counts, service.getProgress());
  }


  // ---- Helpers -----------------------------------------------------------

  private static DeferredIndex row(String table, String index, DeferredIndexStatus status) {
    DeferredIndex r = new DeferredIndex();
    r.setTableName(table);
    r.setIndexName(index);
    r.setIndexUnique(false);
    r.setIndexColumns(List.of("col1"));
    r.setStatus(status);
    r.setAttemptsCount(0);
    return r;
  }
}
