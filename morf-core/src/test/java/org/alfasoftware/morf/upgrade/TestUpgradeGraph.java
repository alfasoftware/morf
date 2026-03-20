/* Copyright 2017 Alfa Financial Software
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests for {@link UpgradeGraph}.
 *
 * @author Copyright (c) Alfa Financial Software 2024
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

    List<Class<? extends UpgradeStep>> ordered = Lists.newArrayList(graph.orderedSteps());
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

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for missing @Sequence");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("does not have an @Sequence annotation"));
      assertThat(e.getMessage(), containsString("StepMissingSequence"));
    }
  }


  /**
   * Test that duplicate sequence numbers are detected.
   */
  @Test
  public void testDuplicateSequenceNumbers() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(ValidStepWithVersion.class);    // seq 1000
    steps.add(StepDuplicateSequence.class);   // seq 1000

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for duplicate sequence");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("sh  are the same @Sequence annotation"));
      assertThat(e.getMessage(), containsString("[1000]"));
    }
  }


  /**
   * Test that invalid @Version annotation format is detected.
   */
  @Test
  public void testInvalidVersionAnnotation() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepInvalidVersionFormat.class);

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for invalid @Version");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("invalid @Version annotation"));
      assertThat(e.getMessage(), containsString("StepInvalidVersionFormat"));
    }
  }


  /**
   * Test various invalid version formats.
   */
  @Test
  public void testInvalidVersionFormats() {
    // Test version with no minor number
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepInvalidVersionNoMinor.class);

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for version with no minor number");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("invalid @Version annotation"));
    }

    // Test version with leading 'v'
    steps.clear();
    steps.add(StepInvalidVersionLeadingV.class);

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for version with leading v");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("invalid @Version annotation"));
    }
  }


  /**
   * Test that invalid package name is detected when no @Version annotation is present.
   */
  @Test
  public void testInvalidPackageName() {
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    steps.add(StepNoVersionInvalidPackage.class);

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException for invalid package name");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), containsString("not contained in a package named after the release version"));
      assertThat(e.getMessage(), containsString("StepNoVersionInvalidPackage"));
    }
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

    try {
      new UpgradeGraph(steps);
      fail("Should throw IllegalStateException with multiple errors");
    } catch (IllegalStateException e) {
      String message = e.getMessage();
      assertThat(message, containsString("does not have an @Sequence annotation"));
      assertThat(message, containsString("sh  are the same @Sequence annotation"));
      assertThat(message, containsString("invalid @Version annotation"));
    }
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

    List<Class<? extends UpgradeStep>> ordered = Lists.newArrayList(graph.orderedSteps());
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
    List<Class<? extends UpgradeStep>> orderedList = Lists.newArrayList(ordered);

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
  // ========================================================================

  @Sequence(1000)
  @Version("1.0.0")
  public static class ValidStepWithVersion implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-1";
    }

    @Override
    public String getDescription() {
      return "Valid step with version";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(3000)
  @Version("1.0")
  public static class ValidStepMinimalVersion implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-3";
    }

    @Override
    public String getDescription() {
      return "Valid step with minimal version";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(4000)
  @Version("5.3.20a")
  public static class ValidStepComplexVersion implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-4";
    }

    @Override
    public String getDescription() {
      return "Valid step with complex version";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(9999)
  @Version("2.0.0")
  public static class ValidStepHighSequence implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-9999";
    }

    @Override
    public String getDescription() {
      return "Valid step with high sequence";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(6001)
  @Version("10.20.30.40")
  public static class ValidStepMultiSegmentVersion implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-6001";
    }

    @Override
    public String getDescription() {
      return "Valid step with multi-segment version";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Version("1.0.0")
  public static class StepMissingSequence implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-MISSING";
    }

    @Override
    public String getDescription() {
      return "Step missing sequence annotation";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(1000)
  @Version("2.0.0")
  public static class StepDuplicateSequence implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-DUP";
    }

    @Override
    public String getDescription() {
      return "Step with duplicate sequence";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(5000)
  @Version("invalid")
  public static class StepInvalidVersionFormat implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-INVALID";
    }

    @Override
    public String getDescription() {
      return "Step with invalid version format";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(6000)
  @Version("1")
  public static class StepInvalidVersionNoMinor implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-NO-MINOR";
    }

    @Override
    public String getDescription() {
      return "Step with version missing minor number";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(7000)
  @Version("v1.0.0")
  public static class StepInvalidVersionLeadingV implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-LEADING-V";
    }

    @Override
    public String getDescription() {
      return "Step with version having leading v";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }


  @Sequence(8000)
  public static class StepNoVersionInvalidPackage implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-INVALID-PKG";
    }

    @Override
    public String getDescription() {
      return "Step with no version and invalid package";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // No-op
    }
  }
}
