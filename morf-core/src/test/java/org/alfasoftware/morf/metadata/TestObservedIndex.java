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

package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

/**
 * Unit tests for {@link ObservedIndex}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestObservedIndex {

  /** Observed index delegates name, columns, unique to the underlying index. */
  @Test
  public void testDelegation() {
    // given
    Index base = index("Idx1").unique().columns("col1", "col2");
    ObservedIndex observed = new ObservedIndex(base, true, false);

    // then
    assertEquals("Idx1", observed.getName());
    assertEquals(List.of("col1", "col2"), observed.columnNames());
    assertTrue(observed.isUnique());
    assertTrue(observed.isDeferred());
    assertFalse(observed.isPhysicallyPresent());
  }


  /** Non-deferred, physically present observed index. */
  @Test
  public void testNonDeferredPhysicallyPresent() {
    // given
    Index base = index("Idx2").columns("col1");
    ObservedIndex observed = new ObservedIndex(base, false, true);

    // then
    assertFalse(observed.isDeferred());
    assertTrue(observed.isPhysicallyPresent());
  }


  /** toString should include deferred and virtual markers. */
  @Test
  public void testToStringWithDeferred() {
    // given
    Index base = index("Idx3").columns("col1");
    ObservedIndex observed = new ObservedIndex(base, true, false);

    // then
    String str = observed.toString();
    assertTrue("Should contain deferred", str.contains("deferred"));
    assertTrue("Should contain virtual", str.contains("virtual"));
  }
}
