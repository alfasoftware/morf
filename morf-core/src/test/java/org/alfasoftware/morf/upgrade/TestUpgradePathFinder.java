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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradePathFinder.NoUpgradePathExistsException;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddCakeTable;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddDiameter;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddGrams;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddJamAmount;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddJamAmountUnit;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddJamType;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddRating;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddStars;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.AddWeight;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.ChangeGramsToWeight;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.DeleteRating;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.DuplicateUUIDOfAddJamType;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.InsertACheescake;
import org.alfasoftware.morf.upgrade.testupgradepathfinder.upgrade.v1_0_0.InsertAVictoriaSpongeRowUsingDSL;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests upgrade path determination.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestUpgradePathFinder {

  /** Test table definition. */
  private final Table sconeTable = table("Scone").columns(
      column("raisins", DataType.BOOLEAN).nullable(),
      column("flour", DataType.STRING).nullable(),
      column("weight", DataType.DECIMAL).nullable()
    );

  /** Test table definition. */
  private final Table upgradedSconeTable = table("Scone").columns(
      column("raisins", DataType.BOOLEAN).nullable(),
      column("flour", DataType.STRING).nullable(),
      column("weight", DataType.DECIMAL).nullable(),
      column("jamType", DataType.STRING).nullable(),
      column("diameter", DataType.DECIMAL).nullable()
    );


  /** Test table definition. */
  private final Table upgradedSconeTableWithJamAmount = table("Scone").columns(
    column("raisins", DataType.BOOLEAN).nullable(),
    column("flour", DataType.STRING).nullable(),
    column("weight", DataType.DECIMAL).nullable(),
    column("jamType", DataType.STRING).nullable(),
    column("jamAmountUnit", DataType.STRING, 3).nullable(),
    column("jamAmount", DataType.DECIMAL, 10).nullable(),
    column("diameter", DataType.DECIMAL).nullable()
      );

  /** Test table definition. */
  private final Table cakeTable = table("Cake").columns(
      column("type", DataType.STRING, 10).nullable(),
      column("iced", DataType.BOOLEAN).nullable()
    );

  /** Test table definition. */
  private final Table cakeTablev2= table("Cake").columns(
      column("type", DataType.STRING, 10).nullable(),
      column("iced", DataType.BOOLEAN).nullable(),
      column("stars", DataType.DECIMAL).nullable()
    );

  private List<Class<?>> appliedSteps(Class<?>... array) {
    return Arrays.asList(array);
  }

  /**
   * Tests that an exact path (i.e. every available upgrade step is reqruied).
   */
  @Test
  public void testExactPath() {
    // This should select the four of the five upgrade steps - the weight
    // column is already present
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTable, cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrades steps", 4, path.size());
    assertSame("First upgrades step", AddCakeTable.class, path.get(0).getClass());
    assertSame("Second upgrades step", AddDiameter.class, path.get(1).getClass());
    assertSame("Third upgrades step", AddJamType.class, path.get(2).getClass());
  }

  @Test(expected = IllegalStateException.class)
  public void testfindDiscrepanciesThrowsException() {
    // This test will check that an IllegalStateException is thrown as we are trying to run an upgrade step that has already run
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddJamType.class, AddCakeTable.class));

    Map<String, String> upgradeAudit = new HashMap<>();
    upgradeAudit.put(InsertAVictoriaSpongeRowUsingDSL.class.getName(), "0fde0d93-f57e-405c-81e9-245ef1ba0591");
    upgradeAudit.put(AddDiameter.class.getName(), "6bb40ce0-2578-11e0-ac64-0800200c9a61");
    try {
      pathFinder.findDiscrepancies(upgradeAudit);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(InsertAVictoriaSpongeRowUsingDSL.class.getAnnotation(UUID.class).value()));
      throw e;
    }
  }

  @Test
  public void testfindDiscrepancies() {
    //Returns successful when everything is in order.
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddJamType.class, AddCakeTable.class, AddDiameter.class));

    Map<String, String> upgradeAudit = new HashMap<>();
    upgradeAudit.put(AddDiameter.class.getName(), "6bb40ce0-2578-11e0-ac64-0800200c9a66");
    pathFinder.findDiscrepancies(upgradeAudit);
  }


  /**
   * When providing 2 unapplied upgrade steps
   * and one unapplied upgrade with with-only constraint (AddJamAmount should be applied only with AddJamType)
   * When determining the upgrade path
   * Then all 3 steps are applied in the correct order.
   */
  @Test
  public void testConditionalUpgradeStepIsExecuted() {
    // given
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(AddJamAmount.class); // to be executed only with AddJamType
    upgradeSteps.add(AddJamAmountUnit.class); // to be executed only with AddJamAmount

    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTableWithJamAmount);

    // when
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    // then
    assertEquals("Number of upgrades steps", 4, path.size());
    assertSame("First", AddDiameter.class, path.get(0).getClass());
    assertSame("Second", AddJamType.class, path.get(1).getClass());
    assertSame("Third", AddJamAmountUnit.class, path.get(2).getClass());
    assertSame("Last", AddJamAmount.class, path.get(3).getClass());
  }


  /**
   * When providing 2 already applied upgrade steps
   * and one unapplied upgrade with with-only constraint (AddJamAmount should be applied only with AddJamType)
   * When determining the upgrade path
   * Then no upgrade steps are applied.
   */
  @Test
  public void testConditionalUpgradeStepNotExecuted() {
    // given
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(AddJamAmount.class);  // to be executed only with AddJamType
    upgradeSteps.add(AddJamAmountUnit.class); // to be executed only with AddJamAmount

    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddJamType.class, AddDiameter.class));

    Schema current = schema(upgradedSconeTable);
    Schema target = schema(upgradedSconeTable);

    // when
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    // then
    assertEquals("Number of upgrades steps", 0, path.size());
  }


  /**
   * Given providing 1 unapplied upgrade step that via {@link OnlyWith} references an upgrade step that is not in the sequence of upgrade steps
   * When building upgrade path finder
   * Then exception is thrown.
   */
  @Test
  public void testConditionalUpgradeStepReferencesStepThatIsNotInTheSequence() {
    // given
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamAmount.class);  // to be executed only with AddJamType, which is not in the sequence

    try {
      // when
      makeFinder(upgradeSteps, appliedSteps());
      fail("Exception is expected.");
    } catch (IllegalStateException e) {
      // then
      assertThat(e.getMessage(), containsString("references non-existing upgrade step"));
    }
  }


  /**
   * Tests that a basic linear upgrade path can be correctly determined.
   *
   * This should select the four of the five upgrade steps - the weight
   * column is already present
   */
  @Test
  public void testLinearPath() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    upgradeSteps.add(AddWeight.class);
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddWeight.class));

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTable, cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrades steps", 4, path.size());
    assertSame("Second upgrades step", AddCakeTable.class, path.get(0).getClass());
    assertSame("Third upgrades step", AddDiameter.class, path.get(1).getClass());
    assertSame("First upgrades step", AddJamType.class, path.get(2).getClass());
    assertSame("Fourth upgrade step", InsertAVictoriaSpongeRowUsingDSL.class, path.get(3).getClass());
  }


  /**
   * This should fail, because the weight column is already present
   */
  @Test
  public void testAddColumnFailsWhenItAlreadyExists() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    upgradeSteps.add(AddWeight.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTable);
    try {
      pathFinder.determinePath(current, target, Sets.newHashSet());
      fail();
    } catch(RuntimeException rte) {
      Throwable ex = rte;
      // we should find weight somewhere in the causes
      while(ex != null) {
        if (ex.getMessage().contains("[weight]"))
          return;
        ex = ex.getCause();
      }
      throw rte;
    }
  }


  /**
   * Tests that if two steps modify the same column (ie create it then delete
   * it) this does not produce a 'no upgrade path found' error.
   */
  @Test
  public void testAddDeleteSameColumn() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    upgradeSteps.add(AddStars.class);
    upgradeSteps.add(AddRating.class);
    upgradeSteps.add(DeleteRating.class);

    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(cakeTable);
    Schema target = schema(cakeTablev2);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();
    assertEquals("Should only be one upgrade setep.", 3, path.size());
  }


  /**
   * Tests that un-applied data upgrades are still applied even if there are no
   * pending schema changes.
   */
  @Test
  public void testSchemasMatchWithUnappliedDataChange() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    upgradeSteps.add(AddRating.class);
    upgradeSteps.add(DeleteRating.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);
    UpgradePathFinder pathFinder =  makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(cakeTable);
    Schema target = schema(cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();
    assertEquals("Should only be one upgrade step.", 3, path.size());
    assertEquals("Upgrade step 1 should be Insert row using DSL.", InsertAVictoriaSpongeRowUsingDSL.class, path.get(0).getClass());
  }


  /**
   * Tests that an upgrade step that has already been applied is correctly skipped.
   */
  @Test
  public void testUpgradeWithSkippedStep() {
    // This should select the three of the four upgrade steps - the weight
    // column is already present
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddWeight.class);
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddWeight.class, AddDiameter.class));

    // Add the diameter column to the current schema even though it is meant to
    // be the last upgrade step
    Table newScone = table("Scone").columns(
        column("raisins", DataType.BOOLEAN).nullable(),
        column("flour", DataType.STRING).nullable(),
        column("weight", DataType.DECIMAL).nullable(),
        column("diameter", DataType.DECIMAL).nullable()
      );

    Schema current = schema(newScone);
    Schema target = schema(upgradedSconeTable, cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrades steps", 2, path.size());
    assertSame("Second upgrades step", AddCakeTable.class, path.get(0).getClass());
    assertSame("First upgrades step", AddJamType.class, path.get(1).getClass());
  }


  /**
   * Tests that a data upgrade step that is a prerequisite is still applied.
   */
  @Test
  public void testUpgradeWithPrerequisiteDataSteps() {
    // This should select the three of the four upgrade steps - the weight
    // column is already present
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(InsertACheescake.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    assertTrue("Steps to apply", pathFinder.hasStepsToApply());

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrades steps: " + upgradeSteps, 3, path.size());
    assertSame("Second upgrades step", AddDiameter.class, path.get(0).getClass());
    assertSame("First upgrades step", AddJamType.class, path.get(1).getClass());
    assertSame("Third upgrades step", InsertACheescake.class, path.get(2).getClass());
  }


  /**
   * Tests that when there is no database to upgrade no steps are included.
   */
  @Test
  public void testUpgradeWithNoSteps() {
    // Add all the upgrade steps - none of them are relevant
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddWeight.class);
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(AddDiameter.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddWeight.class, AddJamType.class, AddCakeTable.class, AddDiameter.class));

    assertFalse("No steps to apply", pathFinder.hasStepsToApply());

    Schema current = schema(sconeTable, cakeTable);
    Schema target = schema(sconeTable, cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrades steps", 0, path.size());
  }


  /**
   * Tests that when there is no upgrade path an exception is thrown.
   */
  @Test
  public void testNoUpgradeExists() {
    // We deliberately leave out the add diameter ugprade step
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddCakeTable.class);
    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps());

    Schema current = schema(sconeTable);
    Schema target = schema(upgradedSconeTable, cakeTable);
    try {
      pathFinder.determinePath(current, target, Sets.newHashSet());
      fail("No upgrade path exists so an exception should be thrown");
    } catch (NoUpgradePathExistsException e) {
      assertEquals("Message text", "No upgrade path exists", e.getMessage());
      assertTrue("Message is instance of IllegalStateException", e instanceof IllegalStateException);
    }
  }


  /**
   * Tests that when there are multiple linked steps where a later upgrade step
   * reverses the work of an earlier step the upgrade path finder does not
   * become confused and attempt to re-apply the earlier step.
   */
  @Test
  public void testInterdependentSequence() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    // This step is not really part of the test but ensures there is definitely
    // something to do which triggers processing.

    // Instead of adding the weight column we add a different version of it and
    // then make a change

    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(AddDiameter.class);
    upgradeSteps.add(ChangeGramsToWeight.class);
    upgradeSteps.add(AddGrams.class);
    upgradeSteps.add(AddCakeTable.class);
    upgradeSteps.add(InsertAVictoriaSpongeRowUsingDSL.class);

    UpgradePathFinder pathFinder = makeFinder(upgradeSteps, appliedSteps(AddDiameter.class, AddJamType.class, AddGrams.class, ChangeGramsToWeight.class));

    Schema current = schema(upgradedSconeTable);
    Schema target = schema(upgradedSconeTable, cakeTable);
    List<UpgradeStep> path = pathFinder.determinePath(current, target, Sets.newHashSet()).getUpgradeSteps();

    assertEquals("Number of upgrade steps", 2, path.size());
    assertEquals("Type of upgrade step", AddCakeTable.class, path.get(0).getClass());
  }


  /**
   * Check we can detect duplicate UUIDs. Allowing these would be very bad.
   */
  @Test
  public void testDetectDuplicateUUIDs() {
    List<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(AddJamType.class);
    upgradeSteps.add(DuplicateUUIDOfAddJamType.class);

    try {
      makeFinder(upgradeSteps, appliedSteps());
      fail("should detect duplicate UUID");
    } catch(IllegalStateException ise) {
      String message = ise.getMessage();

      assertTrue(message.contains(AddJamType.class.getSimpleName()));
      assertTrue(message.contains(DuplicateUUIDOfAddJamType.class.getSimpleName()));
      assertTrue(message.contains(DuplicateUUIDOfAddJamType.class.getAnnotation(UUID.class).value()));
    }
  }


  private UpgradePathFinder makeFinder(List<Class<? extends UpgradeStep>> availableSteps, List<Class<?>> appliedSteps) {
    Set<java.util.UUID> uuids = new HashSet<>();
    for(Class<?> appliedStep : appliedSteps) {
      uuids.add(java.util.UUID.fromString(appliedStep.getAnnotation(UUID.class).value()));
    }

    return new UpgradePathFinder(availableSteps, uuids);
  }
}
