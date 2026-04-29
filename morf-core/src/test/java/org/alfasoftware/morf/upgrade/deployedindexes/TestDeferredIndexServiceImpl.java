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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.jdbc.ConnectionResources;
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

  private ConnectionResources connectionResources;
  private DeployedIndexesDAO dao;
  private DeferredIndexServiceImpl service;


  @Before
  public void setUp() {
    connectionResources = mock(ConnectionResources.class);
    dao = mock(DeployedIndexesDAO.class);
    service = new DeferredIndexServiceImpl(connectionResources, dao);
  }


  /** getBuildTasks returns one task per non-terminal row, preserving table/index identity. */
  @Test
  public void testGetBuildTasksOneTaskPerNonTerminalRow() {
    when(dao.findNonTerminal()).thenReturn(List.of(
        row("Product", "Idx_A", DeployedIndexStatus.PENDING),
        row("Customer", "Idx_B", DeployedIndexStatus.IN_PROGRESS),
        row("Order", "Idx_C", DeployedIndexStatus.FAILED)));

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


  /** Each task is a {@link DeferredIndexBuildTaskImpl} (so adopters get the package-private behaviour). */
  @Test
  public void testGetBuildTasksReturnsBuildTaskImpl() {
    when(dao.findNonTerminal()).thenReturn(List.of(row("Product", "Idx", DeployedIndexStatus.PENDING)));

    DeferredIndexBuildTask t = service.getBuildTasks().get(0);

    assertTrue("expected DeferredIndexBuildTaskImpl, got " + t.getClass().getName(),
        t instanceof DeferredIndexBuildTaskImpl);
  }


  /** Returned list is unmodifiable so callers can't mutate it after dispatch. */
  @Test
  public void testGetBuildTasksReturnsUnmodifiableList() {
    when(dao.findNonTerminal()).thenReturn(List.of(row("Product", "Idx", DeployedIndexStatus.PENDING)));

    List<DeferredIndexBuildTask> tasks = service.getBuildTasks();

    assertThrows(UnsupportedOperationException.class, () -> tasks.add(null));
  }


  /** getProgress delegates the count map straight from the DAO (same instance, no copy). */
  @Test
  public void testGetProgressDelegatesToDao() {
    Map<DeployedIndexStatus, Integer> counts = new EnumMap<>(DeployedIndexStatus.class);
    counts.put(DeployedIndexStatus.PENDING, 2);
    counts.put(DeployedIndexStatus.IN_PROGRESS, 1);
    counts.put(DeployedIndexStatus.COMPLETED, 5);
    counts.put(DeployedIndexStatus.FAILED, 0);
    when(dao.getProgressCounts()).thenReturn(counts);

    assertSame(counts, service.getProgress());
  }


  // ---- Helpers -----------------------------------------------------------

  private static DeployedIndex row(String table, String index, DeployedIndexStatus status) {
    DeployedIndex r = new DeployedIndex();
    r.setTableName(table);
    r.setIndexName(index);
    r.setIndexUnique(false);
    r.setIndexColumns(List.of("col1"));
    r.setStatus(status);
    r.setAttemptsCount(0);
    return r;
  }
}
