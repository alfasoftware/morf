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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.alfasoftware.morf.changelog.EntityHumanReadableStatementConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

/**
 * Class which produces human readable statements from a
 * supplied list of upgrade steps.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class HumanReadableStatementProducer {

  private static final Log log = LogFactory.getLog(HumanReadableStatementProducer.class);

  /**
   * The graph of upgrade steps.
   */
  private final UpgradeGraph upgradeGraph;


  /**
   * Control flag for whether to report data transformation descriptions from the upgrade steps.
   */
  private final boolean reportDataChanges;


  /**
   * Control flag for the preferred database dialect to use when reporting data transformation
   * descriptions that are written in pure SQL. This is ignored if {@link #reportDataChanges} is
   * false.
   */
  private final String preferredSQLDialect;

  /**
   * Contains utility functions used by producer.
   */
  private final HumanReadableStatementProducerUtils utils =  new HumanReadableStatementProducerUtils();


  /**
   * Constructs a new {@link HumanReadableStatementProducer}.
   *
   * <p>When constructed in this form the producer will only describe structural changes and not include
   * any data transformations.</p>
   *
   * @param upgradeSteps a list of upgrade steps in any order.
   */
  public HumanReadableStatementProducer(final Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    this(upgradeSteps, false, null);
  }


  /**
   * Constructs a new {@link HumanReadableStatementProducer}.
   *
   * @param upgradeSteps a list of upgrade steps in any order.
   * @param includeDataChanges {@code true} to report on both structural and data changes, {@code false}
   *          to report on only structural changes.
   * @param preferredSQLDialect the SQL dialect to use when reporting on data changes that cannot be
   *          expressed in human readable form.
   */
  public HumanReadableStatementProducer(final Collection<Class<? extends UpgradeStep>> upgradeSteps,
                                        final boolean includeDataChanges,
                                        final String preferredSQLDialect) {
    super();
    this.upgradeGraph = new UpgradeGraph(upgradeSteps);
    this.reportDataChanges = includeDataChanges;
    this.preferredSQLDialect = preferredSQLDialect;
  }

  /**
   * @deprecated use produceFor(HumanReadableStatementConsumer, EntityHumanReadableStatementConsumer)
   * @param consumer the consumer to consume the events.
   */
  @Deprecated
  public void produceFor(final HumanReadableStatementConsumer consumer) {
    produceFor(consumer,
        new EntityHumanReadableStatementConsumer("", new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))));
  }

  /**
   * Produces output via the supplied consumer.
   *
   * @param consumer the consumer to consume the events.
   * @param entityConsumer the EntityConsumer to consumer the events.
   */
  public void produceFor(final HumanReadableStatementConsumer consumer, final EntityHumanReadableStatementConsumer entityConsumer) {

    // Ensure the upgrade steps are in the correct order
    final Collection<Class<? extends UpgradeStep>> upgradeSteps = upgradeGraph.orderedSteps();
    final boolean populateEntityBasedChangelog = entityConsumer.getVersionStart() != null && !entityConsumer.getVersionStart().isBlank();
    //Create a Multimap which has version ordered keys but insertion ordered values
    ListMultimap<String, UpgradeStep> orderedUpgradeSteps = Multimaps.newListMultimap(
      Maps.<String, Collection<UpgradeStep>>newTreeMap(new TreeMap<String, Collection<UpgradeStep>>(
          new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
              return utils.versionCompare(o1, o2);
            }
          })
        ),
      new Supplier<List<UpgradeStep>>() {
        @Override
        public List<UpgradeStep> get() {
          return Lists.newLinkedList();
        }
      });


    // Iterate over the upgrade steps initializing them, and reordering by version and then sequence
    for (Class<? extends UpgradeStep> currentStepClass : upgradeSteps) {
      try {
        // Create an instance of the upgrade step
        Constructor<? extends UpgradeStep> constructor = currentStepClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        UpgradeStep step = constructor.newInstance();
        orderedUpgradeSteps.put(utils.getUpgradeStepVersion(step), step);

      } catch (Exception e) {
        throw new IllegalStateException("Cannot instantiate upgrade step [" + currentStepClass.getName() + "]", e);
      }
    }

    // Create a proxy schema editor to pass through the consumer events
    HumanReadableStatementSchemaEditor schemaEditor = new HumanReadableStatementSchemaEditor(consumer);
    //Similarly, we need a proxy DataEditor
    HumanReadableStatementDataEditor dataEditor = new HumanReadableStatementDataEditor(consumer, reportDataChanges, preferredSQLDialect);
    // Create entityKnowledgeBuilder for populating entity based changelogs
    EntityKnowledgeMapBuilder entityKnowledgeMapBuilder =new EntityKnowledgeMapBuilder(preferredSQLDialect);
    // Iterate over versions, then over the ordered upgrade steps
    log.debug("Populate EntityBasedChangelog: [ "
        + populateEntityBasedChangelog + " ], "
        + "Entity Start version sanitised : [ "
        + utils.sanitise(entityConsumer.getVersionStart()) + " ], ");

    for (String version : orderedUpgradeSteps.keySet()) {
      consumer.versionStart("ALFA " + version);
      for (UpgradeStep currentStep : orderedUpgradeSteps.get(version)) {
        // Indicate to the consumer that the upgrade step has started
        consumer.upgradeStepStart(currentStep.getClass().getSimpleName(), currentStep.getDescription(), currentStep.getJiraId());

        // Fire all the actual schema change events
        currentStep.execute(schemaEditor, dataEditor);
        // Indicate to the consumer that the upgrade step has ended
        consumer.upgradeStepEnd(currentStep.getClass().getSimpleName());


        log.debug("Upgrade step version sanitised [ " + utils.sanitise(version) + " ], comparison: [ "
            + utils.versionCompare(utils.sanitise(entityConsumer.getVersionStart()), utils.sanitise(version)) + " ]");
        if (populateEntityBasedChangelog && utils.versionCompare(utils.sanitise(entityConsumer.getVersionStart()), utils.sanitise(version)) >= 0){
          log.debug("Upgrade Step [" + currentStep.getClass().getSimpleName() + "] was added to entity based knowledge map");
          // Populate entityKnowledgeMapBuilder
          entityKnowledgeMapBuilder.upgradeStepStart(currentStep.getClass().getSimpleName(), currentStep.getDescription(), currentStep.getJiraId());
          currentStep.execute(entityKnowledgeMapBuilder, entityKnowledgeMapBuilder);
        }
      }
      consumer.versionEnd("ALFA " + version);
    }

    //now handle our knowledgeMap and place into consumer...
    //Iterate over knowledge, then each entity within, then descriptions split by new line
    entityKnowledgeMapBuilder.getKnowledgeMultimap().forEach((entity, upgradeStep) -> {
      log.debug("Printing information for entity [ " + entity + " ]");
      entityConsumer.entityStart(entity);
      upgradeStep.forEach((upgradeStepID, schemaChanges) -> {
        log.debug("Printing information for upgrade step [ " + upgradeStepID.getName() + " ]");
        entityConsumer.upgradeStepStart(upgradeStepID.getName(), upgradeStepID.getDescription(), upgradeStepID.getJiraID());
        schemaChanges.forEach(entityConsumer::schemaChange);
        entityConsumer.upgradeStepEnd("");
      });
      entityConsumer.entityEnd("");
    });


  }


}

