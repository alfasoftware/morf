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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link DeployedIndexTrackerImpl}. Mocks the DAO and
 * verifies pure delegation — the tracker is intentionally a thin adapter
 * that injects a wall-clock timestamp and forwards everything else.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexTrackerImpl {

  private DeployedIndexesDAO dao;
  private DeployedIndexTrackerImpl tracker;


  @Before
  public void setUp() {
    dao = mock(DeployedIndexesDAO.class);
    tracker = new DeployedIndexTrackerImpl(dao);
  }


  /** markStarted passes a time in the [before, after] window to DAO.markStarted. */
  @Test
  public void testMarkStartedDelegatesWithCurrentTime() {
    // given
    long before = System.currentTimeMillis();

    // when
    tracker.markStarted("Product", "Idx1");
    long after = System.currentTimeMillis();

    // then -- captured time is bounded on both sides
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(dao).markStarted(eq("Product"), eq("Idx1"), captor.capture());
    long passed = captor.getValue();
    assertTrue("time should be >= before snapshot (" + before + "), was " + passed, passed >= before);
    assertTrue("time should be <= after snapshot (" + after + "), was " + passed, passed <= after);
  }


  /** markCompleted passes a time in the [before, after] window to DAO.markCompleted. */
  @Test
  public void testMarkCompletedDelegatesWithCurrentTime() {
    // given
    long before = System.currentTimeMillis();

    // when
    tracker.markCompleted("Product", "Idx1");
    long after = System.currentTimeMillis();

    // then
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(dao).markCompleted(eq("Product"), eq("Idx1"), captor.capture());
    long passed = captor.getValue();
    assertTrue("time should be >= before snapshot (" + before + "), was " + passed, passed >= before);
    assertTrue("time should be <= after snapshot (" + after + "), was " + passed, passed <= after);
  }


  /** markFailed delegates to DAO.markFailed. */
  @Test
  public void testMarkFailedDelegates() {
    // when
    tracker.markFailed("Product", "Idx1", "boom");

    // then
    verify(dao).markFailed("Product", "Idx1", "boom");
  }


  /** getProgress returns the map from DAO.getProgressCounts. */
  @Test
  public void testGetProgressDelegates() {
    // given
    Map<DeployedIndexStatus, Integer> daoResult = new EnumMap<>(DeployedIndexStatus.class);
    daoResult.put(DeployedIndexStatus.PENDING, 5);
    when(dao.getProgressCounts()).thenReturn(daoResult);

    // when
    Map<DeployedIndexStatus, Integer> result = tracker.getProgress();

    // then
    assertSame(daoResult, result);
  }


  /** getPendingIndexes returns what DAO.findNonTerminal returns. */
  @Test
  public void testGetPendingIndexesDelegates() {
    // given
    DeployedIndex e = new DeployedIndex();
    List<DeployedIndex> daoResult = List.of(e);
    when(dao.findNonTerminal()).thenReturn(daoResult);

    // when
    List<DeployedIndex> result = tracker.getPendingIndexes();

    // then
    assertSame(daoResult, result);
  }


  /** resetInProgress delegates to DAO.resetInProgress. */
  @Test
  public void testResetInProgressDelegates() {
    // when
    tracker.resetInProgress();

    // then
    verify(dao).resetInProgress();
  }
}
