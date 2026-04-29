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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

/**
 * Tests for {@link UpgradeGraph}.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public class TestUpgradeGraph {

  /**
   * Test that valid steps with @Version annotation are accepted.
   */
  @Test
  public void testValidStepsWithVersionAnnotation() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepWithVersion.class);
    steps.add(ValidStepMinimalVersion.class);
    steps.add(ValidStepComplexVersion.class);

    UpgradeGraph graph = new UpgradeGraph(steps);

    Collection<Class<? extends UpgradeStep>> ordered = graph.orderedSteps();
    assertEquals("Should contain all three steps", 3, ordered.size());
  }


  /**
   * Test that valid steps with package-based versioning are accepted.
   */
  @Test
  public void testValidStepsWithPackageName() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(org.alfasoftware.morf.upgrade.testupgradegraph.upgrade.v1_0.ValidPackageStep.class);

    UpgradeGraph graph = new UpgradeGraph(steps);

    Collection<Class<? extends UpgradeStep>> ordered = graph.orderedSteps();
    assertEquals("Should contain the step", 1, ordered.size());
  }


  /**
   * Test that steps are ordered by sequence number.
   */
  @Test
  public void testStepsOrderedBySequence() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepComplexVersion.class);     // seq 4000
    steps.add(ValidStepWithVersion.class);        // seq 1000
    steps.add(ValidStepHighSequence.class);       // seq 9999
    steps.add(ValidStepMinimalVersion.class);     // seq 3000

    UpgradeGraph graph = new UpgradeGraph(steps);

    List<Class<? extends UpgradeStep>> ordered = new ArrayList<>(graph.orderedSteps());
    assertEquals("First should be seq 1000", ValidStepWithVersion.class, ordered.get(0));
    assertEquals("Second should be seq 3000", ValidStepMinimalVersion.class, ordered.get(1));
    assertEquals("Third should be seq 4000", ValidStepComplexVersion.class, ordered.get(2));
    assertEquals("Fourth should be seq 9999", ValidStepHighSequence.class, ordered.get(3));
  }


  /**
   * Test that empty collection of steps is handled correctly.
   */
  @Test
  public void testEmptyStepsCollection() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();

    UpgradeGraph graph = new UpgradeGraph(steps);

    assertThat("Should be empty", graph.orderedSteps(), empty());
  }


  /**
   * Test that missing @Sequence annotation is detected.
   */
  @Test
  public void testMissingSequenceAnnotation() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepMissingSequence.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("does not have an @Sequence annotation"));
    assertThat(e.getMessage(), containsString("StepMissingSequence"));
  }


  /**
   * Test that duplicate sequence numbers are detected.
   */
  @Test
  public void testDuplicateSequenceNumbers() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepWithVersion.class);    // seq 1000
    steps.add(StepDuplicateSequence.class);   // seq 1000

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("sh  are the same @Sequence annotation"));
    assertThat(e.getMessage(), containsString("[1000]"));
  }


  /**
   * Test that invalid @Version annotation format is detected.
   */
  @Test
  public void testInvalidVersionAnnotation() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepInvalidVersionFormat.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("invalid @Version annotation"));
    assertThat(e.getMessage(), containsString("StepInvalidVersionFormat"));
  }


  /**
   * A @Version with no minor number ("1") is rejected.
   */
  @Test
  public void testRejectsVersionWithNoMinor() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepInvalidVersionNoMinor.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("invalid @Version annotation"));
  }


  /**
   * A @Version with a leading 'v' ("v1.0.0") is rejected.
   */
  @Test
  public void testRejectsVersionWithLeadingV() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepInvalidVersionLeadingV.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("invalid @Version annotation"));
  }


  /**
   * Test that invalid package name is detected when no @Version annotation is present.
   */
  @Test
  public void testInvalidPackageName() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepNoVersionInvalidPackage.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    assertThat(e.getMessage(), containsString("not contained in a package named after the release version"));
    assertThat(e.getMessage(), containsString("StepNoVersionInvalidPackage"));
  }


  /**
   * Test that multiple validation errors are accumulated.
   */
  @Test
  public void testMultipleValidationErrors() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepMissingSequence.class);
    steps.add(ValidStepWithVersion.class);      // seq 1000
    steps.add(StepDuplicateSequence.class);     // seq 1000
    steps.add(StepInvalidVersionFormat.class);

    IllegalStateException e = assertThrows(IllegalStateException.class, () -> new UpgradeGraph(steps));
    String message = e.getMessage();
    assertThat(message, containsString("does not have an @Sequence annotation"));
    assertThat(message, containsString("sh  are the same @Sequence annotation"));
    assertThat(message, containsString("invalid @Version annotation"));
  }


  /**
   * Test that various valid @Version annotation formats are accepted.
   */
  @Test
  public void testVersionAnnotationValidFormats() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepMinimalVersion.class);     // "1.0"
    steps.add(ValidStepWithVersion.class);        // "1.0.0"
    steps.add(ValidStepComplexVersion.class);     // "5.3.20a"
    steps.add(ValidStepMultiSegmentVersion.class); // "10.20.30.40"

    UpgradeGraph graph = new UpgradeGraph(steps);

    assertEquals("All valid formats should be accepted", 4, graph.orderedSteps().size());
  }


  /**
   * Test sequence ordering with boundary values.
   */
  @Test
  public void testSequenceOrderingBoundaryValues() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepHighSequence.class);       // seq 9999
    steps.add(ValidStepWithVersion.class);        // seq 1000
    steps.add(ValidStepMinimalVersion.class);     // seq 3000

    UpgradeGraph graph = new UpgradeGraph(steps);

    List<Class<? extends UpgradeStep>> ordered = new ArrayList<>(graph.orderedSteps());
    assertEquals("Should be sorted in ascending order", 3, ordered.size());
    assertEquals("First", ValidStepWithVersion.class, ordered.get(0));
    assertEquals("Second", ValidStepMinimalVersion.class, ordered.get(1));
    assertEquals("Third", ValidStepHighSequence.class, ordered.get(2));
  }


  /**
   * Test that orderedSteps() returns an unmodifiable collection.
   */
  @Test
  public void testOrderedStepsReturnsSortedCollection() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepComplexVersion.class);
    steps.add(ValidStepWithVersion.class);

    UpgradeGraph graph = new UpgradeGraph(steps);

    Collection<Class<? extends UpgradeStep>> ordered = graph.orderedSteps();
    List<Class<? extends UpgradeStep>> orderedList = new ArrayList<>(ordered);

    assertEquals("Should be in sequence order", ValidStepWithVersion.class, orderedList.get(0));
    assertEquals("Should be in sequence order", ValidStepComplexVersion.class, orderedList.get(1));
  }


  /**
   * Test that complex valid package names are accepted.
   */
  @Test
  public void testComplexValidPackageNames() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(org.alfasoftware.morf.upgrade.testupgradegraph.upgrade.v10_20_30a.ComplexValidPackageStep.class);

    UpgradeGraph graph = new UpgradeGraph(steps);

    assertEquals("Should contain the step", 1, graph.orderedSteps().size());
  }


  // ========================================================================
  // Mock UpgradeStep implementations for testing
  //
  // These are pure scaffolding for exercising UpgradeGraph's annotation
  // parsing. The behaviour under test is entirely in the @Sequence and
  // @Version annotations on each subclass; getJiraId / getDescription /
  // execute are inherited no-ops so the relevant differences stand out.
  // ========================================================================

  abstract static class TestStepBase implements UpgradeStep {
    @Override public final String getJiraId() { return "TEST-" + getClass().getSimpleName(); }
    @Override public final String getDescription() { return getClass().getSimpleName(); }
    @Override public final void execute(SchemaEditor schema, DataEditor data) { /* No-op */ }
  }

  @Sequence(1000) @Version("1.0.0")
  public static class ValidStepWithVersion extends TestStepBase {}

  @Sequence(3000) @Version("1.0")
  public static class ValidStepMinimalVersion extends TestStepBase {}

  @Sequence(4000) @Version("5.3.20a")
  public static class ValidStepComplexVersion extends TestStepBase {}

  @Sequence(9999) @Version("2.0.0")
  public static class ValidStepHighSequence extends TestStepBase {}

  @Sequence(6001) @Version("10.20.30.40")
  public static class ValidStepMultiSegmentVersion extends TestStepBase {}

  @Version("1.0.0")
  public static class StepMissingSequence extends TestStepBase {}

  @Sequence(1000) @Version("2.0.0")
  public static class StepDuplicateSequence extends TestStepBase {}

  @Sequence(5000) @Version("invalid")
  public static class StepInvalidVersionFormat extends TestStepBase {}

  @Sequence(6000) @Version("1")
  public static class StepInvalidVersionNoMinor extends TestStepBase {}

  @Sequence(7000) @Version("v1.0.0")
  public static class StepInvalidVersionLeadingV extends TestStepBase {}

  @Sequence(8000)
  public static class StepNoVersionInvalidPackage extends TestStepBase {}
}
