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

package org.alfasoftware.morf.upgrade.db;

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradePathFinder;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.upgrade.CreateDeferredIndexes;
import org.alfasoftware.morf.upgrade.upgrade.UpgradeSteps;
import org.junit.Test;

/**
 * Consistency between the tables Morf's own upgrade steps create and the tables
 * Morf contributes to an adopter's target schema.
 *
 * <p>These are two halves of one contract. {@link DatabaseUpgradeTableContribution}
 * is bound into a {@code Multibinder<TableContribution>} in {@code MorfModule}, so
 * {@link DatabaseUpgradeTableContribution#tables()} is how an infrastructure table
 * reaches the target schema an adopter builds from its binaries. If a Morf upgrade
 * step creates a table that {@code tables()} does not declare, then after that step
 * runs the trial-upgraded schema contains a table the target schema does not, and
 * {@link UpgradePathFinder#determinePath} rejects the path.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDatabaseUpgradeTableContribution {

  private final DatabaseUpgradeTableContribution contribution = new DatabaseUpgradeTableContribution();


  /**
   * {@code CreateDeferredIndexes} is a registered Morf upgrade step that creates the
   * DeferredIndexes table, so the contribution must declare that table.
   */
  @Test
  public void testTablesIncludesEveryTableCreatedByAMorfUpgradeStep() {
    // given
    List<String> contributed = contribution.tables().stream()
        .map(Table::getName)
        .collect(Collectors.toList());

    // then
    assertTrue("CreateDeferredIndexes is in UpgradeSteps.LIST and creates "
        + DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME
        + ", so tables() must contribute it to the target schema. Contributed: " + contributed,
        contributed.stream()
            .anyMatch(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME::equalsIgnoreCase));
  }


  /**
   * The adopter's-eye view: an existing deployment picks up a Morf build whose
   * infrastructure upgrade steps are not all applied yet. The pending steps must take
   * the database schema to exactly the schema Morf contributes — otherwise the
   * application cannot start, regardless of any feature flag.
   */
  @Test
  public void testPendingMorfUpgradeStepsReachTheContributedSchema() {
    // given -- the database as it stands before the newest Morf infrastructure step
    Schema current = schema(
        DatabaseUpgradeTableContribution.deployedViewsTable(),
        DatabaseUpgradeTableContribution.upgradeAuditTable());

    // and -- the target schema an adopter assembles from Morf's contribution
    Schema target = schema(contribution.tables());

    // and -- every Morf step already applied except the newest one
    Set<java.util.UUID> alreadyApplied = new HashSet<>();
    for (Class<? extends UpgradeStep> step : UpgradeSteps.LIST) {
      if (step.equals(CreateDeferredIndexes.class)) {
        continue;
      }
      alreadyApplied.add(java.util.UUID.fromString(
          step.getAnnotation(org.alfasoftware.morf.upgrade.UUID.class).value()));
    }

    // when / then -- a path must exist
    try {
      new UpgradePathFinder(UpgradeSteps.LIST, alreadyApplied)
          .determinePath(current, target, java.util.Collections.emptySet());
    } catch (UpgradePathFinder.NoUpgradePathExistsException e) {
      fail("No upgrade path exists after applying Morf's own pending upgrade steps. "
          + "The steps create a table that DatabaseUpgradeTableContribution.tables() does not "
          + "declare, so the upgraded schema can never match the application's target schema. "
          + "An adopter picking up this build cannot start.");
    }
  }
}
