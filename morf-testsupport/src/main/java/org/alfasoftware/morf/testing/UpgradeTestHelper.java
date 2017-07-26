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

package org.alfasoftware.morf.testing;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.upgrade.InlineTableUpgrader;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.alfasoftware.morf.upgrade.SchemaChangeSequence;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.SqlStatementWriter;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeGraph;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Helper which does basic testing of upgrade steps:
 *
 * <ol>
 *   <li>Apply the steps in reverse to establish a start schema</li>
 *   <li>Get the DB to that state</li>
 *   <li>Build the SQL upgrade script</li>
 *   <li>Apply the script to the DB</li>
 *   <li>Verify the final schema matches</li>
 * </ol>
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class UpgradeTestHelper {
  @Inject private Provider<DatabaseSchemaManager> schemaManager;
  @Inject private ConnectionResources connectionResources;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private Provider<DatabaseDataSetProducer> databaseDataSetProducer;

  /**
   *  Store UUIDs that have been encountered to check for uniqueness. This is only within the module, but it's better than nothing.
   */
  private final Set<String> uuids = Sets.newHashSet();

  /**
   * Test the upgrade step.
   * @param finalSchema The resulting schema
   * @param upgradeSteps The sequence of upgrade steps
   */
  public void testUpgrades(Schema finalSchema, Iterable<Class<? extends UpgradeStep>> upgradeSteps) {
    Collection<Class<? extends UpgradeStep>> orderedSteps = new UpgradeGraph(upgradeSteps).orderedSteps();

    // Build the change sequence, and the "from" schema (the start point for the upgrade)
    SchemaChangeSequence schemaChangeSequence = new SchemaChangeSequence(instantiateAndValidateUpgradeSteps(orderedSteps));
    Schema fromSchema = schemaChangeSequence.applyInReverseToSchema(finalSchema);

    // Apply the changes forwards to prime the sequence.
    schemaChangeSequence.applyToSchema(fromSchema);

    // We need a fully clean sheet since we are going to inspect the DB at the end
    schemaManager.get().dropAllViews();
    schemaManager.get().dropAllTables();

    // Set the database to the start point
    schemaManager.get().mutateToSupportSchema(fromSchema, TruncationBehavior.ALWAYS);

    // Capture the SQL as a script
    final LinkedList<String> sqlScript = Lists.newLinkedList();

    // Upgrader, which captures the SQL as a script
    InlineTableUpgrader inlineTableUpgrader = new InlineTableUpgrader(fromSchema, connectionResources.sqlDialect(), new SqlStatementWriter() {
      @Override
      public void writeSql(Collection<String> sql) {
        sqlScript.addAll(sql);
      }
    }, SqlDialect.IdTable.withPrefix(connectionResources.sqlDialect(), "temp_id_"));

    // Apply the steps to the upgrader
    inlineTableUpgrader.preUpgrade();
    schemaChangeSequence.applyTo(
      inlineTableUpgrader
    );
    inlineTableUpgrader.postUpgrade();

    // Run the script
    try {
      sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor()).execute(sqlScript);

      // Compare the actual final schema to the expected final schema.
      final List<String> differences = Lists.newLinkedList();
      SchemaHomology schemaHomology = new SchemaHomology(new SchemaHomology.DifferenceWriter() {
        @Override
        public void difference(String message) {
          differences.add(message);
        }
      }, "expected", "actual");

      DatabaseDataSetProducer producer = databaseDataSetProducer.get();
      producer.open();
      try {
        Schema actual = producer.getSchema();
        boolean match = schemaHomology.schemasMatch(finalSchema, actual, new HashSet<String>());

        if (!match) {
          fail("Differences:\n"+Joiner.on('\n').join(differences));
        }
      } finally {
        producer.close();
      }
    } finally {
      // We've messed with the database structure, so invalidate the cache
      schemaManager.get().invalidateCache();
    }
  }


  /**
   * Validate that the upgrades are package-visible.
   * @param upgradeSteps The sequence of upgrade steps
   */
  public void validateStepsArePackageVisible(Iterable<Class<? extends UpgradeStep>> upgradeSteps) {
    for (Class<? extends UpgradeStep> upgradeStepClass : upgradeSteps) {
      // Upgrade steps classes should be package-visible (default) - not public
      if (Modifier.isPublic(upgradeStepClass.getModifiers())) {
        fail(String.format("Upgrade class [%s] is public and should be package visible", upgradeStepClass.getSimpleName()));
      }
    }
  }


  /**
   * Validate that the upgrade step meets the basic requirements.
   */
  private void validateUpgradeStep(UpgradeStep upgradeStep) {
    Class<? extends UpgradeStep> upgradeStepClass = upgradeStep.getClass();

    // Check the upgrade step has a Sequence
    if (upgradeStepClass.getAnnotation(Sequence.class) == null) {
      fail("Upgrade step [" + upgradeStepClass.getName() + "] should have a Sequence set. How about ["
          + System.currentTimeMillis() / 1000 + "]");
    }
    // Check the upgrade step has a UUID
    UUID uuidAnnotation = upgradeStepClass.getAnnotation(UUID.class);
    String currentUuid = uuidAnnotation == null ? null : uuidAnnotation.value();

    if (StringUtils.isBlank(currentUuid) || !uuids.add(currentUuid)) {
      fail("Upgrade step [" + upgradeStepClass + "] should have a non blank, unique UUID set. How about ["
          + java.util.UUID.randomUUID().toString() + "]");
    }

    // verify we can parse the UUID
    try {
      assertNotNull(java.util.UUID.fromString(currentUuid));
    } catch (Exception e) {
      throw new RuntimeException("Could not parse UUID [" + currentUuid + "] from [" + upgradeStepClass+"]", e);
    }

    // Check the upgrade step has a description
    final String description = upgradeStep.getDescription();
    assertTrue("Should have a description", StringUtils.isNotEmpty(description));
    assertTrue("Description must not be more than 200 characters", description.length() <= 200);

    // Descriptions should not end with full-stops
    if (description.endsWith(".")) {
      fail(String.format("Description for [%s] should not end with full stop - [%s]", upgradeStepClass.getSimpleName(),
        description));
    }

    assertTrue("Should have a JIRA ID", StringUtils.isNotEmpty(upgradeStep.getJiraId()));

    for (String jiraId : StringUtils.split(upgradeStep.getJiraId(), ',')) {
      assertTrue("Should be a valid JIRA ID [" + upgradeStep.getJiraId() + "]", jiraIdIsValid(jiraId));
    }
  }


  /**
   * Checks that a supplied JIRA ID is valid.
   *
   * @param jiraId the JIRA ID to check
   * @return true if the JIRA ID is valid, false otherwise
   */
  private boolean jiraIdIsValid(final String jiraId) {
    return
        jiraId.matches("WEB-\\d+") ||
        jiraId.matches("PDT-\\d+");
  }


  /**
   * Turn the list of classes into a list of objects.
   */
  private List<UpgradeStep> instantiateAndValidateUpgradeSteps(Collection<Class<? extends UpgradeStep>> stepClasses) {
    return FluentIterable.from(stepClasses).transform(new Function<Class<? extends UpgradeStep>, UpgradeStep>() {
      @Override public UpgradeStep apply(Class<? extends UpgradeStep> stepClass) {
        UpgradeStep upgradeStep;
        try {
          Constructor<? extends UpgradeStep> constructor = stepClass.getDeclaredConstructor();
          // Permit package-protected classes
          constructor.setAccessible(true);
          upgradeStep = constructor.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        validateUpgradeStep(upgradeStep);

        return upgradeStep;
      }
    }).toList();
  }
}
