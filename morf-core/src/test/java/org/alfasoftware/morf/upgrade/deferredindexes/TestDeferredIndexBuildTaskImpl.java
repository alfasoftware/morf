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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.Optional;

import org.junit.Test;

/**
 * Unit tests for the thin {@link DeferredIndexBuildTaskImpl} wrapper -- delegate
 * the snapshot getters to the captured {@link DeferredIndex}, and delegate
 * {@link Runnable#run()} to the shared {@link DeferredIndexBuilder}. The
 * reconciliation algorithm is tested separately in
 * {@link TestDeferredIndexBuilder}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexBuildTaskImpl {

  /** Snapshot getters reflect the row captured at construction time. */
  @Test
  public void testSnapshotGettersExposeRowStateAtConstructionTime() {
    DeferredIndex row = makeRow(DeferredIndexStatus.FAILED, 3, "disk full");
    DeferredIndexBuildTaskImpl task = new DeferredIndexBuildTaskImpl(row, mock(DeferredIndexBuilder.class));

    assertEquals("Product", task.getTableName());
    assertEquals("Idx1", task.getIndexName());
    assertEquals(DeferredIndexStatus.FAILED, task.getStatus());
    assertEquals(3, task.getAttemptsCount());
    assertEquals(Optional.of("disk full"), task.getErrorMessage());
  }


  /** errorMessage getter wraps a null-on-the-row as {@code Optional.empty()}. */
  @Test
  public void testErrorMessageEmptyWhenNeverFailed() {
    DeferredIndex row = makeRow(DeferredIndexStatus.PENDING, 0, null);
    DeferredIndexBuildTaskImpl task = new DeferredIndexBuildTaskImpl(row, mock(DeferredIndexBuilder.class));

    assertEquals(Optional.empty(), task.getErrorMessage());
  }


  /** run() delegates straight to {@code builder.build(snapshot)} and does nothing else. */
  @Test
  public void testRunDelegatesToBuilder() {
    DeferredIndex row = makeRow(DeferredIndexStatus.PENDING, 0, null);
    DeferredIndexBuilder builder = mock(DeferredIndexBuilder.class);
    DeferredIndexBuildTaskImpl task = new DeferredIndexBuildTaskImpl(row, builder);

    task.run();

    verify(builder).build(row);
    verifyNoMoreInteractions(builder);
  }


  private static DeferredIndex makeRow(DeferredIndexStatus status, int attempts, String errorMessage) {
    DeferredIndex row = new DeferredIndex();
    row.setTableName("Product");
    row.setIndexName("Idx1");
    row.setIndexUnique(false);
    row.setIndexColumns(List.of("col1"));
    row.setStatus(status);
    row.setAttemptsCount(attempts);
    row.setErrorMessage(errorMessage);
    return row;
  }
}
