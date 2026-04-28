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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexTrackingPolicy}: the matrix of
 * (declared-deferred × dialect-supports-deferred-creation).
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexTrackingPolicy {

  /** Non-deferred index on supporting dialect: not tracked, immediate build. */
  @Test
  public void testNonDeferredOnSupportingDialect() {
    DeferredIndexTrackingPolicy policy = new DeferredIndexTrackingPolicy(dialect(true));
    Index idx = index("Foo_Idx").columns("col");

    assertFalse("non-deferred should not be tracked",
        policy.toTrackedIndex(idx).isPresent());
    assertTrue("non-deferred requires immediate build",
        policy.requiresImmediateBuild(idx));
    assertEquals("effective form unchanged for non-deferred",
        idx, policy.effectiveIndex(idx));
  }


  /** Deferred index on supporting dialect: tracked, no immediate build. */
  @Test
  public void testDeferredOnSupportingDialect() {
    DeferredIndexTrackingPolicy policy = new DeferredIndexTrackingPolicy(dialect(true));
    Index idx = index("Foo_Idx").deferred().columns("col");

    Optional<Index> tracked = policy.toTrackedIndex(idx);
    assertTrue("deferred on supporting dialect should be tracked", tracked.isPresent());
    assertTrue("tracked form keeps deferred flag", tracked.get().isDeferred());
    assertFalse("deferred on supporting dialect skips immediate build",
        policy.requiresImmediateBuild(idx));
    assertTrue("effective form preserves deferred flag",
        policy.effectiveIndex(idx).isDeferred());
  }


  /** Deferred index on non-supporting dialect: not tracked, immediate build,
   *  effective form normalizes to non-deferred. */
  @Test
  public void testDeferredOnNonSupportingDialect() {
    DeferredIndexTrackingPolicy policy = new DeferredIndexTrackingPolicy(dialect(false));
    Index idx = index("Foo_Idx").deferred().columns("col");

    assertFalse("deferred on non-supporting dialect should not be tracked",
        policy.toTrackedIndex(idx).isPresent());
    assertTrue("deferred on non-supporting dialect requires immediate build",
        policy.requiresImmediateBuild(idx));
    Index effective = policy.effectiveIndex(idx);
    assertFalse("effective form drops deferred flag on non-supporting dialect",
        effective.isDeferred());
    assertEquals("effective form preserves name", "Foo_Idx", effective.getName());
    assertEquals("effective form preserves columns", idx.columnNames(), effective.columnNames());
  }


  /** Non-deferred on non-supporting dialect: not tracked, immediate build. */
  @Test
  public void testNonDeferredOnNonSupportingDialect() {
    DeferredIndexTrackingPolicy policy = new DeferredIndexTrackingPolicy(dialect(false));
    Index idx = index("Foo_Idx").columns("col");

    assertFalse(policy.toTrackedIndex(idx).isPresent());
    assertTrue(policy.requiresImmediateBuild(idx));
    assertEquals(idx, policy.effectiveIndex(idx));
  }


  /** Unique flag preserved through effectiveIndex normalization. */
  @Test
  public void testUniqueFlagPreservedOnNormalization() {
    DeferredIndexTrackingPolicy policy = new DeferredIndexTrackingPolicy(dialect(false));
    Index uniqueDeferred = index("Foo_Idx").unique().deferred().columns("col");

    Index effective = policy.effectiveIndex(uniqueDeferred);
    assertTrue("uniqueness preserved", effective.isUnique());
    assertFalse("deferred flag dropped", effective.isDeferred());
  }


  private static SqlDialect dialect(boolean supportsDeferred) {
    SqlDialect d = mock(SqlDialect.class);
    when(d.supportsDeferredIndexCreation()).thenReturn(supportsDeferred);
    return d;
  }
}
