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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.junit.Test;

/**
 * Unit tests for {@link DeployedIndex}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndex {

  /** toIndex reconstructs a non-unique deferred index (slim: always deferred). */
  @Test
  public void testToIndexBasic() {
    // given
    DeployedIndex entry = new DeployedIndex();
    entry.setIndexName("Idx1");
    entry.setIndexColumns(List.of("col1", "col2"));
    entry.setIndexUnique(false);

    // when
    Index idx = entry.toIndex();

    // then
    assertEquals("Idx1", idx.getName());
    assertEquals(List.of("col1", "col2"), idx.columnNames());
    assertFalse(idx.isUnique());
    assertTrue("Slim invariant: every persisted row reconstructs as deferred", idx.isDeferred());
  }


  /** toIndex preserves the unique flag. */
  @Test
  public void testToIndexUnique() {
    // given
    DeployedIndex entry = new DeployedIndex();
    entry.setIndexName("Idx2");
    entry.setIndexColumns(List.of("col1"));
    entry.setIndexUnique(true);

    // when
    Index idx = entry.toIndex();

    // then
    assertTrue(idx.isUnique());
    assertTrue(idx.isDeferred());
  }


  /** toIndex preserves column order for composite indexes. */
  @Test
  public void testToIndexPreservesCompositeColumnOrder() {
    // given — columns declared in a specific non-alphabetical order
    DeployedIndex entry = new DeployedIndex();
    entry.setIndexName("CompositeIdx");
    entry.setIndexColumns(List.of("z", "a", "m"));
    entry.setIndexUnique(false);

    // when
    Index idx = entry.toIndex();

    // then — same order preserved
    assertEquals(List.of("z", "a", "m"), idx.columnNames());
  }
}
