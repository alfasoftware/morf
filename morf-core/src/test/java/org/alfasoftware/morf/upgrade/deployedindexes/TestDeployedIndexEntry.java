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
 * Unit tests for {@link DeployedIndexEntry}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexEntry {

  /** toIndex should reconstruct a non-deferred, non-unique index. */
  @Test
  public void testToIndexBasic() {
    // given
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setIndexName("Idx1");
    entry.setIndexColumns(List.of("col1", "col2"));
    entry.setIndexUnique(false);
    entry.setIndexDeferred(false);

    // when
    Index idx = entry.toIndex();

    // then
    assertEquals("Idx1", idx.getName());
    assertEquals(List.of("col1", "col2"), idx.columnNames());
    assertFalse(idx.isUnique());
    assertFalse(idx.isDeferred());
  }


  /** toIndex should preserve unique and deferred flags. */
  @Test
  public void testToIndexUniqueDeferred() {
    // given
    DeployedIndexEntry entry = new DeployedIndexEntry();
    entry.setIndexName("Idx2");
    entry.setIndexColumns(List.of("col1"));
    entry.setIndexUnique(true);
    entry.setIndexDeferred(true);

    // when
    Index idx = entry.toIndex();

    // then
    assertTrue(idx.isUnique());
    assertTrue(idx.isDeferred());
  }


  /** parseColumns should split comma-separated values. */
  @Test
  public void testParseColumns() {
    // when
    List<String> cols = DeployedIndexEntry.parseColumns("col1,col2,col3");

    // then
    assertEquals(List.of("col1", "col2", "col3"), cols);
  }


  /** joinColumns should produce comma-separated string. */
  @Test
  public void testJoinColumns() {
    // when
    String result = DeployedIndexEntry.joinColumns(List.of("a", "b", "c"));

    // then
    assertEquals("a,b,c", result);
  }
}
