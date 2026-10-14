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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.junit.Test;

/**
 * Tests history-based eligibility of reversal steps, including composition with OnlyWith.
 */
public class TestRevertsUpgradeStep {

  private static final String ORIGINAL_UUID = "9823a150-2578-11e0-ac64-0800200c9a66";
  private static final String REVERSAL_UUID = "5854164b-53c6-42aa-944f-5e5f09121abc";
  private static final String COMPANION_UUID = "777828f0-2578-11e0-ac64-0800200c9a66";

  @Test
  public void testReversalRunsForAppliedOriginalWithoutOriginalClass() {
    UpgradePathFinder finder = finder(Collections.singleton(java.util.UUID.fromString(ORIGINAL_UUID)), Reversal.class);
    assertTrue(finder.hasStepsToApply());
    Schema current = schema(table("Example").columns(column("exampleNumber", DataType.INTEGER)));
    assertEquals(Reversal.class, finder.determinePath(current, schema(), Collections.emptySet())
        .getUpgradeSteps().get(0).getClass());
  }

  @Test
  public void testReversalRunsWithOriginalClassStillAvailable() {
    UpgradePathFinder finder = finder(Collections.singleton(java.util.UUID.fromString(ORIGINAL_UUID)),
        Original.class, Reversal.class);
    assertEquals(1, finder.getSchemaChangeSequence().getUpgradeSteps().size());
    assertEquals(Reversal.class, finder.getSchemaChangeSequence().getUpgradeSteps().get(0).getClass());
  }

  @Test
  public void testReversalSkippedWithoutHistory() {
    UpgradePathFinder finder = finder(Collections.emptySet(), Reversal.class);
    assertFalse(finder.hasStepsToApply());
    assertTrue(finder.determinePath(schema(), schema(), Collections.emptySet()).getUpgradeSteps().isEmpty());
  }

  @Test
  public void testUnrelatedHistoryDoesNotEnableReversal() {
    assertFalse(finder(Collections.singleton(java.util.UUID.fromString(COMPANION_UUID)), Reversal.class).hasStepsToApply());
  }

  @Test
  public void testOriginalAndReversalCancelInSameUpgrade() {
    UpgradePathFinder finder = finder(Collections.emptySet(), Reversal.class, Original.class);
    assertFalse(finder.hasStepsToApply());
    assertTrue(finder.determinePath(schema(), schema(), Collections.emptySet()).getUpgradeSteps().isEmpty());
  }

  @Test
  public void testCancellationSkipsOnlyWithDependants() {
    assertFalse(finder(Collections.emptySet(), Companion.class, Original.class, Reversal.class).hasStepsToApply());
    assertFalse(finder(Collections.emptySet(), OriginalCompanion.class, Original.class, Reversal.class).hasStepsToApply());
  }

  @Test
  public void testAppliedReversalDoesNotCancelPendingOriginal() {
    UpgradePathFinder finder = finder(Collections.singleton(java.util.UUID.fromString(REVERSAL_UUID)),
        Original.class, Reversal.class);
    assertEquals(1, finder.getSchemaChangeSequence().getUpgradeSteps().size());
    assertEquals(Original.class, finder.getSchemaChangeSequence().getUpgradeSteps().get(0).getClass());
  }

  @Test
  public void testAppliedReversalIsNotRepeated() {
    Set<java.util.UUID> applied = new HashSet<>(Arrays.asList(
        java.util.UUID.fromString(ORIGINAL_UUID), java.util.UUID.fromString(REVERSAL_UUID)));
    assertFalse(finder(applied, Original.class, Reversal.class).hasStepsToApply());
  }

  @Test
  public void testOnlyWithDependentFollowsReversalEligibility() {
    assertFalse(finder(Collections.emptySet(), Companion.class, Reversal.class).hasStepsToApply());
    UpgradePathFinder finder = finder(Collections.singleton(java.util.UUID.fromString(ORIGINAL_UUID)),
        Companion.class, Reversal.class);
    assertEquals(2, finder.getSchemaChangeSequence().getUpgradeSteps().size());
    assertEquals(Reversal.class, finder.getSchemaChangeSequence().getUpgradeSteps().get(0).getClass());
    assertEquals(Companion.class, finder.getSchemaChangeSequence().getUpgradeSteps().get(1).getClass());
  }

  @Test
  public void testBothAnnotationsRequireBothConditions() {
    assertFalse(finder(Collections.emptySet(), Original.class, Combined.class)
        .getSchemaChangeSequence().getUpgradeSteps().stream().anyMatch(Combined.class::isInstance));
    Set<java.util.UUID> bothApplied = new HashSet<>(Arrays.asList(
        java.util.UUID.fromString(ORIGINAL_UUID), java.util.UUID.fromString(REVERSAL_UUID)));
    assertFalse(finder(bothApplied, Original.class, Combined.class).hasStepsToApply());
    UpgradePathFinder finder = finder(Collections.singleton(java.util.UUID.fromString(REVERSAL_UUID)),
        Combined.class, Original.class);
    assertEquals(2, finder.getSchemaChangeSequence().getUpgradeSteps().size());
  }

  @Test
  public void testReadReferencedUUID() {
    assertNull(UpgradePathFinder.readRevertsUpgradeStepUUID(Original.class));
    assertEquals(java.util.UUID.fromString(ORIGINAL_UUID), UpgradePathFinder.readRevertsUpgradeStepUUID(Reversal.class));
  }

  @Test
  public void testInvalidUUIDRejected() {
    assertThrows(IllegalArgumentException.class, () -> finder(Collections.emptySet(), Invalid.class));
  }

  @Test
  public void testBlankUUIDRejected() {
    assertThrows(IllegalArgumentException.class, () -> finder(Collections.emptySet(), Blank.class));
  }

  @SafeVarargs
  private final UpgradePathFinder finder(Set<java.util.UUID> applied, Class<? extends UpgradeStep>... steps) {
    return new UpgradePathFinder(Arrays.asList(steps), applied);
  }

  public abstract static class TestStep implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-1";
    }

    @Override
    public String getDescription() {
      return "Test reversal eligibility";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No schema changes unless overridden by a test fixture.
    }
  }

  @UUID(ORIGINAL_UUID)
  @Sequence(1)
  @Version("1.0.0")
  public static class Original extends TestStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(table("Example").columns(column("exampleNumber", DataType.INTEGER)));
    }
  }

  @UUID(REVERSAL_UUID)
  @Sequence(2)
  @Version("1.0.0")
  @RevertsUpgradeStep(ORIGINAL_UUID)
  public static class Reversal extends TestStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.removeTable(table("Example").columns(column("exampleNumber", DataType.INTEGER)));
    }
  }

  @UUID(COMPANION_UUID)
  @Sequence(3)
  @Version("1.0.0")
  @OnlyWith(REVERSAL_UUID)
  public static class Companion extends TestStep { }

  @UUID(COMPANION_UUID)
  @Sequence(3)
  @Version("1.0.0")
  @OnlyWith(ORIGINAL_UUID)
  public static class OriginalCompanion extends TestStep { }

  @UUID(COMPANION_UUID)
  @Sequence(3)
  @Version("1.0.0")
  @OnlyWith(ORIGINAL_UUID)
  @RevertsUpgradeStep(REVERSAL_UUID)
  public static class Combined extends TestStep { }

  @UUID(REVERSAL_UUID)
  @Sequence(2)
  @Version("1.0.0")
  @RevertsUpgradeStep("not-a-uuid")
  public static class Invalid extends TestStep { }

  @UUID(REVERSAL_UUID)
  @Sequence(2)
  @Version("1.0.0")
  @RevertsUpgradeStep(" ")
  public static class Blank extends TestStep { }
}
