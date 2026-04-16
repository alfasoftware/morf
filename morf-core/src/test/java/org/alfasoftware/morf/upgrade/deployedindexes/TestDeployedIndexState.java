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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndexState}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexState {

  /** empty() state reports UNKNOWN for any lookup. */
  @Test
  public void testEmptyReportsUnknown() {
    // given
    DeployedIndexState state = DeployedIndexState.empty();

    // then
    assertEquals(IndexPresence.UNKNOWN, state.getPresence("AnyTable", "AnyIndex"));
  }


  /** A state constructed with a PRESENT entry reports PRESENT. */
  @Test
  public void testPresentEntryReportsPresent() {
    // given
    Map<String, IndexPresence> map = new HashMap<>();
    map.put(DeployedIndexState.key("MyTable", "MyIdx"), IndexPresence.PRESENT);
    DeployedIndexState state = new DeployedIndexState(map);

    // then
    assertEquals(IndexPresence.PRESENT, state.getPresence("MyTable", "MyIdx"));
  }


  /** A state constructed with an ABSENT entry reports ABSENT. */
  @Test
  public void testAbsentEntryReportsAbsent() {
    // given
    Map<String, IndexPresence> map = new HashMap<>();
    map.put(DeployedIndexState.key("MyTable", "MyIdx"), IndexPresence.ABSENT);
    DeployedIndexState state = new DeployedIndexState(map);

    // then
    assertEquals(IndexPresence.ABSENT, state.getPresence("MyTable", "MyIdx"));
  }


  /** A key not in the state reports UNKNOWN, independent of keys that are present. */
  @Test
  public void testUnknownEntryReportsUnknown() {
    // given
    Map<String, IndexPresence> map = new HashMap<>();
    map.put(DeployedIndexState.key("MyTable", "MyIdx"), IndexPresence.PRESENT);
    DeployedIndexState state = new DeployedIndexState(map);

    // then
    assertEquals(IndexPresence.UNKNOWN, state.getPresence("OtherTable", "OtherIdx"));
    assertEquals(IndexPresence.UNKNOWN, state.getPresence("MyTable", "OtherIdx"));
  }


  /** Lookups are case-insensitive on both table and index name. */
  @Test
  public void testLookupIsCaseInsensitive() {
    // given -- stored in mixed case
    Map<String, IndexPresence> map = new HashMap<>();
    map.put(DeployedIndexState.key("MyTable", "MyIdx"), IndexPresence.PRESENT);
    DeployedIndexState state = new DeployedIndexState(map);

    // then -- any casing retrieves the same entry
    assertEquals(IndexPresence.PRESENT, state.getPresence("MYTABLE", "MYIDX"));
    assertEquals(IndexPresence.PRESENT, state.getPresence("mytable", "myidx"));
    assertEquals(IndexPresence.PRESENT, state.getPresence("MyTable", "myidx"));
  }
}
