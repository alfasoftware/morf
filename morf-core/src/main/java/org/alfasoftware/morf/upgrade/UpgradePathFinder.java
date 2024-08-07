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

import com.google.common.collect.Lists;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

  private final UpgradeConfigAndContext upgradeConfigAndContext;

  private final UpgradeGraph upgradeGraph;

  private final Set<java.util.UUID> stepsAlreadyApplied;

  private final List<CandidateStep> stepsToApply;

  private static final String APPLICATION_SCHEMA = "expected schema based on application binaries";

  private static final String UPGRADED_SCHEMA = "database schema with upgrades applied";

  private static final String REVERSED_SCHEMA = "upgraded database schema with upgrades reversed";

  private static final String CURRENT_SCHEMA = "current database schema";

  /**
   * @param availableUpgradeSteps Steps that are available for building a path.
   * @param stepsAlreadyApplied The UUIDs of steps which have already been applied.
   */
  public UpgradePathFinder(Collection<Class<? extends UpgradeStep>> availableUpgradeSteps, Set<java.util.UUID> stepsAlreadyApplied) {
    this(new UpgradeConfigAndContext(), availableUpgradeSteps, stepsAlreadyApplied);
  }


  /**
   * @param availableUpgradeSteps Steps that are available for building a path.
   * @param stepsAlreadyApplied The UUIDs of steps which have already been applied.
   */
  public UpgradePathFinder(UpgradeConfigAndContext upgradeConfigAndContext, Collection<Class<? extends UpgradeStep>> availableUpgradeSteps, Set<java.util.UUID> stepsAlreadyApplied) {
    this.upgradeConfigAndContext = upgradeConfigAndContext;
    this.upgradeGraph = new UpgradeGraph(availableUpgradeSteps);
    this.stepsAlreadyApplied = stepsAlreadyApplied;

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
    return new SchemaChangeSequence(upgradeConfigAndContext, upgradeSteps);
  }


  /**
   * Determines the upgrade path between two schemas. If no path can be found an
   * {@link IllegalStateException} is thrown.
   *
   * @param current The current schema to be upgraded.
   * @param target The target schema to upgrade to.
   * @param exceptionRegexes Regular exceptions for the table exceptions.
   * @return An ordered list of upgrade steps between the two schemas.
   * @throws NoUpgradePathExistsException if no upgrade path exists between the database schema and the application schema.
   */
  public SchemaChangeSequence determinePath(Schema current, Schema target, Collection<String> exceptionRegexes) throws NoUpgradePathExistsException {

    SchemaChangeSequence schemaChangeSequence = getSchemaChangeSequence();

    // We have changes to make. Apply them against the current schema to see whether they get us the right position
    Schema trialUpgradedSchema = schemaChangeSequence.applyToSchema(current);
    if (schemasNotMatch(target, trialUpgradedSchema, APPLICATION_SCHEMA, UPGRADED_SCHEMA, exceptionRegexes)) {
      throw new NoUpgradePathExistsException();
    }

    // Now reverse-apply those changes to see whether they get us back to where we started
    Schema reversal = schemaChangeSequence.applyInReverseToSchema(trialUpgradedSchema);
    if (schemasNotMatch(reversal, current, REVERSED_SCHEMA, CURRENT_SCHEMA, exceptionRegexes)) {
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

    return candidateSteps.values().stream()
            .filter(step -> step.isApplicable(stepsAlreadyApplied, candidateSteps))
            .collect(Collectors.toList());
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
   * @return True if the schemas don't match.
   */
  private boolean schemasNotMatch(Schema targetSchema, Schema trialSchema, String firstSchemaContext, String secondScehmaContext, Collection<String> exceptionRegexes) {
    log.info("Comparing schema [" + firstSchemaContext + "] to [" + secondScehmaContext + "]");
    DifferenceWriter differenceWriter = log::warn;

    SchemaHomology homology = new SchemaHomology(differenceWriter, firstSchemaContext, secondScehmaContext );
    if (homology.schemasMatch(targetSchema, trialSchema, exceptionRegexes)) {
      log.info("Schemas match");
      return false;
    } else {
      log.info("Schema differences found");
      return true;
    }
  }

  /**
   * Finds discrepancies between a provided upgrade audit map and the list of steps to apply.
   * This method compares the UUIDs associated with each upgrade step in the provided list with the corresponding UUIDs
   * in the upgrade audit map. If discrepancies are found, it logs an error message and throws an IllegalStateException.
   *
   * @param upgradeAudit A Map representing the upgrade audit information, where keys are step names and
   *                     values are corresponding UUIDs.
   * @throws IllegalStateException If discrepancies are found between upgrade steps and their associated UUIDs in the
   *                               provided map.
   */
  public void findDiscrepancies(Map<String, String> upgradeAudit) {

    List<String[]> discrepancies = stepsToApply.stream()
            .filter(step -> isStepDuplicated(step, upgradeAudit))
            .map(step -> extractDetails(step, upgradeAudit))
            .collect(Collectors.toList());

    if (!discrepancies.isEmpty()) {
      String message = String.format("Some upgrade steps could have run before with different UUID. The following have been detected:%n%s",
              discrepancies.stream()
                      .map(d -> String.format("Name: %s, Old UUID: %s, Current UUID: %s", d[0], d[1], d[2]))
                      .collect(Collectors.joining("\n")));
      throw new IllegalStateException(message);
    }
  }

  private String[] extractDetails(CandidateStep step, Map<String, String> upgradeAudit) {
    String name = step.getName();
    return new String[]{name, step.getUuid().toString(), upgradeAudit.get(name)};
  }

  /**
   * Checks if a given CandidateStep is duplicated based on its name in the upgradeAudit map.
   * A CandidateStep is considered duplicated if it has the same name but a different UUID
   * compared to the entries in the upgradeAudit map.
   *
   * @param step The CandidateStep to be checked for duplication.
   * @param upgradeAudit A Map<String, String> representing the upgrade audit information, where keys are step names,
   *                     and values are corresponding UUIDs.
   * @return {@code true} if the CandidateStep is duplicated, {@code false} otherwise.
   */
  private boolean isStepDuplicated(CandidateStep step, Map<String, String> upgradeAudit) {
    String name = step.getName();
    String auditUUID = upgradeAudit.get(name);
    return auditUUID != null && !Objects.equals(auditUUID, step.getUuid().toString());
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

    public String getDescription() {
      return this.createStep().getDescription();
    }

    @Override
    public String toString() {
      return getName() + " [" + getUuid() + "]";
    }
  }


  /**
   * Represents the illegal state that no upgrade path has been found. This means
   * that the database and the runtime schema are in an inconsistent state: either the
   * wrong version of the application is being run; the database is in a partially
   * upgraded state; or the database has been modified outside of the application.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public class NoUpgradePathExistsException extends IllegalStateException {

    /**
     * Constructor that defaults the message.
     */
    public NoUpgradePathExistsException() {
      super("No upgrade path exists");
    }
  }
}
