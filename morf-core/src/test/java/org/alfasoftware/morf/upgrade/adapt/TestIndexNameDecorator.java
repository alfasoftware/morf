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

package org.alfasoftware.morf.upgrade.adapt;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.alfasoftware.morf.metadata.Index;
import org.junit.Test;

/**
 * Unit tests for {@link IndexNameDecorator}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestIndexNameDecorator {

  /** getName returns the override; columnNames and isUnique delegate to the wrapped index. */
  @Test
  public void testDelegatesAndOverridesName() {
    Index wrapped = index("Original_Idx").unique().columns("col1", "col2");
    Index decorated = new IndexNameDecorator(wrapped, "Renamed_Idx");

    assertEquals("Renamed_Idx", decorated.getName());
    assertEquals(wrapped.columnNames(), decorated.columnNames());
    assertTrue(decorated.isUnique());
  }


  /** isDeferred delegates to the wrapped index -- previously the override was missing
   *  and the decorator silently inherited the interface's default-false. */
  @Test
  public void testIsDeferredDelegatesToWrappedDeferredIndex() {
    Index wrapped = index("Original_Idx").deferred().columns("col1");
    Index decorated = new IndexNameDecorator(wrapped, "Renamed_Idx");

    assertTrue(decorated.isDeferred());
  }


  /** isDeferred returns false when the wrapped index is non-deferred. */
  @Test
  public void testIsDeferredFalseWhenWrappedIsNotDeferred() {
    Index wrapped = index("Original_Idx").columns("col1");
    Index decorated = new IndexNameDecorator(wrapped, "Renamed_Idx");

    assertFalse(decorated.isDeferred());
  }
}
