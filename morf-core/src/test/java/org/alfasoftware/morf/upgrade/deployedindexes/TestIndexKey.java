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
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

/**
 * Unit tests for {@link IndexKey}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestIndexKey {

  /** Two keys with identical names are equal. */
  @Test
  public void testEqualForSameNames() {
    assertEquals(new IndexKey("T", "I"), new IndexKey("T", "I"));
  }


  /** Equality is case-insensitive on both names. */
  @Test
  public void testEqualityIsCaseInsensitive() {
    assertEquals(new IndexKey("T", "I"), new IndexKey("t", "i"));
    assertEquals(new IndexKey("Table", "Idx"), new IndexKey("TABLE", "IDX"));
  }


  /** hashCode is consistent with equals. */
  @Test
  public void testHashCodeConsistentWithEquals() {
    assertEquals(new IndexKey("T", "I").hashCode(), new IndexKey("t", "i").hashCode());
  }


  /** Keys with different table names are not equal. */
  @Test
  public void testNotEqualForDifferentTable() {
    assertNotEquals(new IndexKey("T1", "I"), new IndexKey("T2", "I"));
  }


  /** Keys with different index names are not equal. */
  @Test
  public void testNotEqualForDifferentIndex() {
    assertNotEquals(new IndexKey("T", "I1"), new IndexKey("T", "I2"));
  }


  /** toString reveals upper-cased table:index (diagnostic only). */
  @Test
  public void testToStringHasDiagnosticFormat() {
    assertEquals("TABLE:IDX", new IndexKey("table", "idx").toString());
  }


  /** Null names are rejected. */
  @Test(expected = NullPointerException.class)
  public void testNullTableNameRejected() {
    new IndexKey(null, "I");
  }


  /** Null names are rejected. */
  @Test(expected = NullPointerException.class)
  public void testNullIndexNameRejected() {
    new IndexKey("T", null);
  }
}
