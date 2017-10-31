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

import static org.alfasoftware.morf.metadata.SchemaUtils.copy;
import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaValidator;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.ExistingViewStateLoader.Result;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactoryImpl;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

/**
 * Entry point for upgrade processing.
 *
 * @author Copyright (c) Alfa Financial Software 2010 - 2013
 */
public class Upgrade {

  private final UpgradePathFactory factory;
  private final ConnectionResources connectionResources;
  private final DataSource dataSource;


  /**
   * Injected Constructor.
   */
  @Inject
  Upgrade(ConnectionResources connectionResources, DataSource dataSource, UpgradePathFactory factory) {
    super();
    this.connectionResources = connectionResources;
    this.dataSource = dataSource;
    this.factory = factory;
  }


  /**
   * Static convenience method which creates the required {@link UpgradePath} to take the specified
   * database and upgrade it to the target schema, using the upgrade steps supplied which have not
   * already been applied.
   *
   * @param targetSchema The target database schema.
   * @param upgradeSteps All upgrade steps which should be deemed to have already run.
   * @param connectionResources Connection details for the database.
   *
   * @return The required upgrade path.
   */
  public static UpgradePath createPath(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, ConnectionResources connectionResources) {
    return new Upgrade(connectionResources, connectionResources.getDataSource(),
        new UpgradePathFactoryImpl(Collections.<UpgradeScriptAddition> emptySet())).findPath(targetSchema, upgradeSteps,
          Collections.<String> emptySet());
  }


  /**
   * Find an upgrade path.
   *
   * @param targetSchema Target schema to upgrade to.
   * @param upgradeSteps All available upgrade steps.
   * @param exceptionRegexes Regular expression for table exclusions.
   * @return The upgrade path available
   */
  public UpgradePath findPath(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, Collection<String> exceptionRegexes) {
    final List<String> upgradeStatements = new ArrayList<>();

    // -- Validate the target schema...
    //
    new SchemaValidator().validate(targetSchema);

    // Get access to the schema we are starting from
    Schema sourceSchema = copySourceSchema(connectionResources, dataSource, exceptionRegexes);
    SqlDialect dialect = connectionResources.sqlDialect();

    // -- Get the current UUIDs and deployed views...
    //
    ExistingViewStateLoader existingViewState = new ExistingViewStateLoader(dialect, new ExistingViewHashLoader(dataSource, dialect));
    Result viewChangeInfo = existingViewState.viewChanges(sourceSchema, targetSchema);
    ViewChanges viewChanges = new ViewChanges(targetSchema.views(), viewChangeInfo.getViewsToDrop(), viewChangeInfo.getViewsToDeploy());

    // -- Determine if an upgrade path exists between the two schemas...
    //
    ExistingTableStateLoader existingTableState = new ExistingTableStateLoader(dataSource, dialect);
    UpgradePathFinder pathFinder = new UpgradePathFinder(upgradeSteps, existingTableState.loadAppliedStepUUIDs());
    SchemaChangeSequence schemaChangeSequence = pathFinder.determinePath(sourceSchema, targetSchema, exceptionRegexes);

    // -- Only run the upgrader if there are any steps to apply...
    //
    if (!schemaChangeSequence.getUpgradeSteps().isEmpty()) {
      // Run the upgrader over all the ElementarySchemaChanges in the upgrade steps
      InlineTableUpgrader upgrader = new InlineTableUpgrader(sourceSchema, dialect, new SqlStatementWriter() {
        @Override
        public void writeSql(Collection<String> sql) {
          upgradeStatements.addAll(sql);
        }
      }, SqlDialect.IdTable.withPrefix(dialect, "temp_id_"));
      upgrader.preUpgrade();
      schemaChangeSequence.applyTo(upgrader);
      upgrader.postUpgrade();
    }

    // -- Upgrade path...
    //
    List<UpgradeStep> upgradesToApply = new ArrayList<>(schemaChangeSequence.getUpgradeSteps());

    // Placeholder upgrade step if no other upgrades - or we drop & recreate everything
    if (upgradesToApply.isEmpty() && !viewChanges.isEmpty()) {
      upgradesToApply.add(new UpgradeStep() {
        @Override public String getJiraId() { return "\u2014"; }
        @Override public String getDescription() { return "Update database views"; }
        @Override public void execute(SchemaEditor schema, DataEditor data) { /* No changes */ }
      });

    } else if (!upgradesToApply.isEmpty()) {
      viewChanges = viewChanges.droppingAlso(sourceSchema.views()).deployingAlso(targetSchema.views());
    }

    // Build the actual upgrade path
    return buildUpgradePath(dialect, sourceSchema, targetSchema, upgradeStatements, viewChanges, upgradesToApply);
  }


  /**
   * Turn the information gathered so far into an {@code UpgradePath}.
   *
   * @param dialect Database dialect.
   * @param sourceSchema Source schema.
   * @param targetSchema Target schema.
   * @param upgradeStatements Upgrade statements identified.
   * @param viewChanges Changes needed to the views.
   * @param upgradesToApply Upgrade steps identified.
   * @return An upgrade path.
   */
  private UpgradePath buildUpgradePath(SqlDialect dialect, Schema sourceSchema, Schema targetSchema,
      final List<String> upgradeStatements, ViewChanges viewChanges, List<UpgradeStep> upgradesToApply) {

    UpgradePath path = factory.create(upgradesToApply, dialect);
    for (View view : viewChanges.getViewsToDrop()) {
      path.writeSql(dialect.dropStatements(view));
      if (sourceSchema.tableExists(DEPLOYED_VIEWS_NAME) && targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        path.writeSql(ImmutableList.of(
          dialect.convertStatementToSQL(
            delete(tableRef(DEPLOYED_VIEWS_NAME)).where(Criterion.eq(field("name"), view.getName().toUpperCase()))
          )));
      }
    }

    path.writeSql(upgradeStatements);

    for (View view : viewChanges.getViewsToDeploy()) {
      path.writeSql(dialect.viewDeploymentStatements(view));
      if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        path.writeSql(ImmutableList.of(
          dialect.convertStatementToSQL(
            insert().into(tableRef(DEPLOYED_VIEWS_NAME))
                                 .fields(literal(view.getName().toUpperCase()).as("name"),
                                         literal(dialect.convertStatementToHash(view.getSelectStatement())).as("hash")),
          targetSchema)));
      }
    }

    // Since Oracle is not able to re-map schema references in trigger code, we need to rebuild all triggers
    // for id column autonumbering when exporting and importing data between environments.
    // We will drop-and-recreate triggers whenever there are upgrade steps to execute. Ideally we'd want to do
    // this step once, however there's no easy way to do that with our upgrade framework.
    if (!upgradesToApply.isEmpty()) {
      for (Table table : targetSchema.tables()) {
        path.writeSql(dialect.rebuildTriggers(table));
      }
    }

    return path;
  }


  /**
   * Gets the source schema from the {@code database}.
   *
   * @param database the database to connect to.
   * @param dataSource the dataSource to use.
   * @param exceptionRegexes
   * @return the schema.
   */
  private Schema copySourceSchema(ConnectionResources database, DataSource dataSource, Collection<String> exceptionRegexes) {
    SchemaResource databaseSchemaResource = database.openSchemaResource(dataSource);
    try {
      return copy(databaseSchemaResource, exceptionRegexes);
    } finally {
      databaseSchemaResource.close();
    }
  }
}