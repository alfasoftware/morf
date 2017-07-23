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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

/**
 * Determines an upgrade path if possible between the current schema definition and a target.
 *
 * <p>This class works by attempting to back out changes in reverse order from the target position
 * until the current position is reached. The upgrade path is then deemed to be all the changes
 * that were used in reverse by applied in their correct order.</p>
 *
 * <p>An important exception to this processing is that the {@linkplain UpgradePathFinder} will overlook
 * any changes that appear to have already been applied and that do not create a contradiction
 * in the dependency chain if they are left out.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class UpgradePathFinder {

  private static final Log log = LogFactory.getLog(UpgradePathFinder.class);

  private final UpgradeGraph upgradeGraph;

  private final Set<java.util.UUID> stepsAlreadyApplied;

  private final List<CandidateStep> stepsToApply;

  /**
   * @param availableUpgradeSteps Steps that are available for building a path.
   * @param stepsAlreadyApplied The UUIDs of steps which have already been applied.
   */
  public UpgradePathFinder(Collection<Class<? extends UpgradeStep>> availableUpgradeSteps, Set<java.util.UUID> stepsAlreadyApplied) {
    this.upgradeGraph = new UpgradeGraph(availableUpgradeSteps);
    this.stepsAlreadyApplied = addAssumedAppliedUUIDs(stepsAlreadyApplied);

    this.stepsToApply = upgradeStepsToApply();
  }


  /**
   * @return Whether this path finder has steps to apply, based on the available steps and those already applied.
   */
  public boolean hasStepsToApply() {
    return !stepsToApply.isEmpty();
  }


  /**
   * Returns a {@link SchemaChangeSequence} from all steps to apply.
   * @return All the steps to apply
   */
  public SchemaChangeSequence getSchemaChangeSequence() {
    List<UpgradeStep> upgradeSteps = Lists.newArrayList();
    for (CandidateStep upgradeStepClass : stepsToApply) {
      upgradeSteps.add(upgradeStepClass.createStep());
    }
    return new SchemaChangeSequence(upgradeSteps);
  }


  /**
   * Determines the upgrade path between two schemas. If no path can be found an
   * {@link IllegalStateException} is thrown.
   *
   * @param current The current schema to be upgraded.
   * @param target The target schema to upgrade to.
   * @param exceptionRegexes Regular exceptions for the table exceptions.
   * @return An ordered list of upgrade steps between the two schemas.
   */
  public SchemaChangeSequence determinePath(Schema current, Schema target, Collection<String> exceptionRegexes) {

    SchemaChangeSequence schemaChangeSequence = getSchemaChangeSequence();

    // We have changes to make. Apply them against the current schema to see whether they get us the right position
    Schema trialUpgradedSchema = schemaChangeSequence.applyToSchema(current);
    if (!schemasMatch(target, trialUpgradedSchema, "target domain schema", "trial upgraded schema", exceptionRegexes)) {
      throw new IllegalStateException("No upgrade path exists");
    }

    // Now reverse-apply those changes to see whether they get us back to where we started
    Schema reversal = schemaChangeSequence.applyInReverseToSchema(trialUpgradedSchema);
    if (!schemasMatch(reversal, current, "upgraded schema reversal", "current schema", exceptionRegexes)) {
      throw new IllegalStateException("Upgrade reversals are invalid");
    }

    return schemaChangeSequence;
  }


  /**
   * Read the UUID from a class, doing some sanity checking.
   *
   * @param upgradeStepClass The upgrade step class.
   * @return The UUID of the class.
   */
  public static java.util.UUID readUUID(Class<? extends UpgradeStep> upgradeStepClass) {
    UUID annotation = upgradeStepClass.getAnnotation(UUID.class);
    if (annotation== null || StringUtils.isBlank(annotation.value())) {
      throw new IllegalStateException("Upgrade step [" + upgradeStepClass.getSimpleName() + "] does not have a UUID annotation.");
    }
    return java.util.UUID.fromString(annotation.value());
  }


  /**
   * Reads the {@link OnlyWith} UUID from a class, doing some sanity checking.
   *
   * @param upgradeStepClass The upgrade step class.
   * @return The UUID of the referenced class; null if no annotation is present on the given class.
   */
  public static java.util.UUID readOnlyWithUUID(Class<? extends UpgradeStep> upgradeStepClass) {
    OnlyWith annotation = upgradeStepClass.getAnnotation(OnlyWith.class);
    if (annotation == null || StringUtils.isBlank(annotation.value())) {
      return null;
    }
    return java.util.UUID.fromString(annotation.value());
  }


  /**
   * Returns a list of upgrade steps to be applied.
   */
  private List<CandidateStep> upgradeStepsToApply() {
    final Map<java.util.UUID, CandidateStep> candidateSteps = candidateStepsByUUID();

    List<CandidateStep> steps = FluentIterable.from(candidateSteps.values())
        .filter(new Predicate<CandidateStep>() {
          @Override public boolean apply(CandidateStep step) {
            return step.isApplicable(stepsAlreadyApplied, candidateSteps);
          }})
        .toList();

    return steps;
  }


  /**
   * Transforms a collection of upgrade step classes into a map of UUIDs and {@link CandidateStep} instances.
   *
   * @throws IllegalStateException if any particular UUID appears on more than once upgrade step class.
   */
  private Map<java.util.UUID, CandidateStep> candidateStepsByUUID() {
    // track which candidate step is attached to which UUID to detect duplicates
    // also the map must retain the original ordering of the upgrade steps
    Map<java.util.UUID, CandidateStep> uuidsForClass = new LinkedHashMap<>();
    for (Class<? extends UpgradeStep> candidateStepClass : upgradeGraph.orderedSteps()) {
      CandidateStep candidateStep = new CandidateStep(candidateStepClass);

      // store the candidate in the map and check that each UUID is only present once
      CandidateStep duplicateStep = uuidsForClass.put(candidateStep.getUuid(), candidateStep);
      if (duplicateStep != null) {
        throw new IllegalStateException(String.format("Upgrade step [%s] has the same UUID as [%s]", candidateStep, duplicateStep));
      }
    }

    return uuidsForClass;
  }


  /**
   * Wraps calls to the {@link SchemaHomology} to provide debug logging.
   *
   * @param targetSchema First schema to compare.
   * @param trialSchema Second schema to compare.
   * @param firstSchemaContext Context of the target schema for logging.
   * @param secondScehmaContext Context of the trial schema for logging.
   * @param exceptionRegexes Regular exceptions for the table exceptions.
   * @return True if the schemas match.
   */
  private boolean schemasMatch(Schema targetSchema, Schema trialSchema, String firstSchemaContext, String secondScehmaContext, Collection<String> exceptionRegexes) {
    log.info("Comparing schema [" + firstSchemaContext + "] to [" + secondScehmaContext + "]");
    DifferenceWriter differenceWriter = new DifferenceWriter() {
      @Override
      public void difference(String message) {
        log.info(message);
      }
    };

    SchemaHomology homology = new SchemaHomology(differenceWriter, firstSchemaContext, secondScehmaContext );
    if (homology.schemasMatch(targetSchema, trialSchema, exceptionRegexes)) {
      log.info("Schemas match");
      return true;
    } else {
      log.info("Schema differences found");
      return false;
    }
  }


  /**
   * For backwards compatibility, we need to assume the presence of some UUIDs - the ones which were added prior to the introduction of the UpgradeAudit table.
   *
   * @param loadedUUIDS
   * @return
   */
  private Set<java.util.UUID> addAssumedAppliedUUIDs(Set<java.util.UUID> loadedUUIDS) {

    // The UpgradeAudit table was added in 5.0.18
    java.util.UUID addUpgradeAuditUUID = java.util.UUID.fromString("5521d849-39b7-4f6f-b95d-5625eac947a7");

    // This step (ProvisionHistoryUpgrade, but it doesn't really matter) was added in 5.0.17
    java.util.UUID some5017UUID = java.util.UUID.fromString("521cb292-9722-4655-a462-aaee8ee19185");

    if (loadedUUIDS.contains(addUpgradeAuditUUID) && !loadedUUIDS.contains(some5017UUID)) {
      log.debug("Assuming the presence of steps in 5.0.18 and earlier");

      Set<java.util.UUID> result = new HashSet<>(loadedUUIDS);
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(UpgradePathFinder.class.getResourceAsStream("AssumedUUIDs.txt"), "UTF-8"))) {
        // these are all the steps from the start of time to the end of 5.0.18
        String line = reader.readLine();
        while (line != null) {
          result.add(java.util.UUID.fromString(line));
          line = reader.readLine();
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      return result;
    } else {
      log.debug("Using literal content of UpgradeAudit");
      return loadedUUIDS;
    }
  }


  /**
   * Helper class encapsulating the logic relevant to individual upgrade steps.
   */
  private static final class CandidateStep {

    private final Class<? extends UpgradeStep> clazz;
    private final java.util.UUID uuid;
    private final java.util.UUID onlyWithUuid;

    private CandidateStep(Class<? extends UpgradeStep> clazz) {
      this.clazz = clazz;
      uuid = readUUID(clazz);
      onlyWithUuid = readOnlyWithUUID(clazz);
    }


    /**
     * Tells the name of the upgrade step, i.e. the class name of the upgrade step.
     */
    public String getName() {
      return clazz.getName();
    }


    /**
     * Tells the UUID of the candidate.
     */
    public java.util.UUID getUuid() {
      return uuid;
    }


    /**
     * Tells the UUID referenced by the candidate via the {@link OnlyWith}.
     */
    public java.util.UUID getOnlyWithUuid() {
      return onlyWithUuid;
    }


    /**
     * Decides whether this step is applicable based on list of already applied steps and other conditions.
     *
     * @param stepsAlreadyApplied List of already applied steps.
     * @param candidateSteps List of all potential candidates.
     * @return true if the candidate is to be applied, false otherwise.
     * @throws IllegalStateException if the {@link OnlyWith} annotation references a non-existing class.
     */

    public boolean isApplicable(Set<java.util.UUID> stepsAlreadyApplied, Map<java.util.UUID, CandidateStep> candidateSteps) {

      // If it has already been applied, then it is not applicable
      if (stepsAlreadyApplied.contains(getUuid())) {
        if (log.isDebugEnabled()) log.debug(String.format("Step already applied: %s", this));
        return false;
      }

      // If it specifies OnlyWith, we need to check recursively the referenced upgrade step for its applicability
      if (getOnlyWithUuid() != null) {
        if (!candidateSteps.containsKey(getOnlyWithUuid())) {
          throw new IllegalStateException(String.format("Upgrade step %s references non-existing upgrade step with UUID [%s].", this, getOnlyWithUuid()));
        }

        if (!candidateSteps.get(getOnlyWithUuid()).isApplicable(stepsAlreadyApplied, candidateSteps)) {
          if (log.isDebugEnabled()) log.debug(String.format("Skipping step %s. It is marked to be executed only with step with UUID [%s].", this, getOnlyWithUuid()));
          return false;
        }
      }

      log.info(String.format("Step to apply: %s", this));
      return true;
    }


    /**
     * Uses reflection to instantiate an upgrade step.
     * @return An upgrade step instance.
     */
    public UpgradeStep createStep() {
      try {
        Constructor<? extends UpgradeStep> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true); // Permit package-protected classes
        return constructor.newInstance();
      }
      catch (Exception e) {
        throw new RuntimeException("Error instantiating upgrade step [" + this + "]", e);
      }
    }


    @Override
    public String toString() {
      return getName() + " [" + getUuid() + "]";
    }
  }
}
