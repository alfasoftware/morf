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
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Function.count;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.NONE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaValidator;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.ExistingViewStateLoader.Result;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeBuilder.GraphBasedUpgradeBuilderFactory;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactoryImpl;
import org.alfasoftware.morf.upgrade.UpgradePathFinder.NoUpgradePathExistsException;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;


/**
 * Entry point for upgrade processing.
 *
 * @author Copyright (c) Alfa Financial Software 2010 - 2013
 */
public class Upgrade {
  private static final Log log = LogFactory.getLog(Upgrade.class);

  private final UpgradePathFactory factory;
  private final ConnectionResources connectionResources;
  private final UpgradeStatusTableService upgradeStatusTableService;
  private final ViewChangesDeploymentHelper viewChangesDeploymentHelper;
  private final ViewDeploymentValidator viewDeploymentValidator;
  private final GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory;
  private final DatabaseUpgradePathValidationService databaseUpgradePathValidationService;


  public Upgrade(
      ConnectionResources connectionResources,
      UpgradePathFactory factory,
      UpgradeStatusTableService upgradeStatusTableService,
      ViewChangesDeploymentHelper viewChangesDeploymentHelper,
      ViewDeploymentValidator viewDeploymentValidator,
      GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory,
      DatabaseUpgradePathValidationService databaseUpgradePathValidationService) {
    super();
    this.connectionResources = connectionResources;
    this.factory = factory;
    this.upgradeStatusTableService = upgradeStatusTableService;
    this.viewChangesDeploymentHelper = viewChangesDeploymentHelper;
    this.viewDeploymentValidator = viewDeploymentValidator;
    this.graphBasedUpgradeBuilderFactory = graphBasedUpgradeBuilderFactory;
    this.databaseUpgradePathValidationService = databaseUpgradePathValidationService;
  }


  /**
   * Static convenience method which takes the specified database and upgrades it to the target
   * schema, using the upgrade steps supplied which have not already been applied.
   * <b>This static context does not support Graph Based Upgrade.</b>
   *
   * @param targetSchema The target database schema.
   * @param upgradeSteps All upgrade steps which should be deemed to have already run.
   * @param connectionResources Connection details for the database.
   * @param viewDeploymentValidator External view deployment validator.
   */
  public static void performUpgrade(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, ConnectionResources connectionResources, ViewDeploymentValidator viewDeploymentValidator) {
    SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
    UpgradeStatusTableService upgradeStatusTableService = new UpgradeStatusTableServiceImpl(sqlScriptExecutorProvider, connectionResources.sqlDialect());
    DatabaseUpgradePathValidationService databaseUpgradePathValidationService = new DatabaseUpgradePathValidationServiceImpl(connectionResources, upgradeStatusTableService);
    try {
      UpgradePath path = Upgrade.createPath(targetSchema, upgradeSteps, connectionResources, upgradeStatusTableService, viewDeploymentValidator, databaseUpgradePathValidationService);
      if (path.hasStepsToApply()) {
        sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor()).execute(path.getSql());
      }
    } finally {
      upgradeStatusTableService.tidyUp(connectionResources.getDataSource());
    }
  }


  /**
   * Static convenience method which creates the required {@link UpgradePath} to take the specified
   * database and upgrade it to the target schema, using the upgrade steps supplied which have not
   * already been applied. <b>This static context does not support Graph Based Upgrade.</b>

   *
   * @param targetSchema The target database schema.
   * @param upgradeSteps All upgrade steps which should be deemed to have already run.
   * @param connectionResources Connection details for the database.
   * @param upgradeStatusTableService Service used to manage the upgrade status coordination table.
   * @param viewDeploymentValidator External view deployment validator.
   * @return The required upgrade path.
   */
  public static UpgradePath createPath(
      Schema targetSchema,
      Collection<Class<? extends UpgradeStep>> upgradeSteps,
      ConnectionResources connectionResources,
      UpgradeStatusTableService upgradeStatusTableService,
      ViewDeploymentValidator viewDeploymentValidator,
      DatabaseUpgradePathValidationService databaseUpgradePathValidationService) {
    Upgrade upgrade = new Upgrade(
      connectionResources,
      new UpgradePathFactoryImpl(new UpgradeScriptAdditionsProvider.NoOpScriptAdditions(), UpgradeStatusTableServiceImpl::new),
      upgradeStatusTableService, new ViewChangesDeploymentHelper(connectionResources.sqlDialect()), viewDeploymentValidator, null, databaseUpgradePathValidationService);
    return upgrade.findPath(targetSchema, upgradeSteps, Collections.<String> emptySet(), connectionResources.getDataSource());
  }


  /**
   * Find an upgrade path.
   *
   * @param targetSchema Target schema to upgrade to.
   * @param upgradeSteps All available upgrade steps.
   * @param exceptionRegexes Regular expression for table exclusions.
   * @param dataSource The data source to use to find the upgrade path.
   * @return The upgrade path available
   */
  public UpgradePath findPath(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, Collection<String> exceptionRegexes, DataSource dataSource) {
    return findPath(targetSchema, upgradeSteps, exceptionRegexes, new HashSet<>(), dataSource);
  }


  /**
   * Find an upgrade path.
   *
   * @param targetSchema            Target schema to upgrade to.
   * @param upgradeSteps            All available upgrade steps.
   * @param exceptionRegexes        Regular expression for table exclusions.
   * @param exclusiveExecutionSteps names of the upgrade step classes which should
   *                                  be executed in an exclusive way
   * @param dataSource              The data source to use to find the upgrade path
   * @return The upgrade path available
   */
  public UpgradePath findPath(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, Collection<String> exceptionRegexes, Set<String> exclusiveExecutionSteps, DataSource dataSource) {
    final List<String> upgradeStatements = new ArrayList<>();

    ResultSetProcessor<Long> upgradeAuditRowProcessor = resultSet -> {resultSet.next(); return resultSet.getLong(1);};
    long upgradeAuditCount = getUpgradeAuditRowCount(upgradeAuditRowProcessor); //fetch a number of upgrade steps applied previously to do optimistic locking check later
    //Return an upgradePath with the current upgrade status if one is in progress
    UpgradeStatus status = upgradeStatusTableService.getStatus(Optional.of(dataSource));
    if (status != NONE) {
      return new UpgradePath(status);
    }

    // -- Validate the target schema...
    //
    new SchemaValidator().validate(targetSchema);

    // Get access to the schema we are starting from
    log.info("Reading current schema");
    Schema sourceSchema = readSourceDatabaseSchema(connectionResources, dataSource, exceptionRegexes, upgradeStatements);
    SqlDialect dialect = connectionResources.sqlDialect();

    // -- Get the current UUIDs and deployed views...
    log.info("Examining current views");    //
    ExistingViewStateLoader existingViewState = new ExistingViewStateLoader(dialect, new ExistingViewHashLoader(dataSource, dialect), viewDeploymentValidator);
    Result viewChangeInfo = existingViewState.viewChanges(sourceSchema, targetSchema);
    ViewChanges viewChanges = new ViewChanges(targetSchema.views(), viewChangeInfo.getViewsToDrop(), viewChangeInfo.getViewsToDeploy());

    // -- Determine if an upgrade path exists between the two schemas...
    //
    log.info("Searching for upgrade path from [" + sourceSchema + "] to [" + targetSchema + "]");
    ExistingTableStateLoader existingTableState = new ExistingTableStateLoader(dataSource, dialect);
    UpgradePathFinder pathFinder = new UpgradePathFinder(upgradeSteps, existingTableState.loadAppliedStepUUIDs());
    pathFinder.findDiscrepancies(getUpgradeAuditRecords());

    SchemaChangeSequence schemaChangeSequence;
    status = upgradeStatusTableService.getStatus(Optional.of(dataSource));
    if (status != NONE) {
      return new UpgradePath(status);
    }
    try {
      schemaChangeSequence = pathFinder.determinePath(sourceSchema, targetSchema, exceptionRegexes);
    } catch (NoUpgradePathExistsException e) {
      log.debug("No upgrade path found - checking upgrade status", e);
      status = upgradeStatusTableService.getStatus(Optional.of(dataSource));
      if (status != NONE) {
        log.info("Schema differences found, but upgrade in progress - no action required until upgrade is complete");
        return new UpgradePath(status);
      }
      else if (upgradeAuditCount != getUpgradeAuditRowCount(upgradeAuditRowProcessor)) {
        //In the meantime another node managed to finish the upgrade steps and flip the status back to NONE. Assuming the upgrade was in progress on another node.
        log.info("Schema differences found, but upgrade was progressed on another node - no action required");
        return new UpgradePath(UpgradeStatus.IN_PROGRESS);
      }
      else {
        throw e;
      }
    }

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

    // Prepare GraphBasedUpgradeBuilder, not supported in the static context (graphBasedUpgradeBuilderFactory = null).
    // The builder should be created even when there are no steps to run, for the view rebuild case.
    GraphBasedUpgradeBuilder graphBasedUpgradeBuilder = null;
    if (graphBasedUpgradeBuilderFactory != null) {
      graphBasedUpgradeBuilder = graphBasedUpgradeBuilderFactory.create(
        sourceSchema,
        targetSchema,
        connectionResources,
        exclusiveExecutionSteps,
        schemaChangeSequence,
        viewChanges);
    }

    // Build the actual upgrade path
    return buildUpgradePath(connectionResources, sourceSchema, targetSchema, upgradeStatements, viewChanges, upgradesToApply, graphBasedUpgradeBuilder, upgradeAuditCount);
  }


  /**
   * Turn the information gathered so far into an {@code UpgradePath}.
   *
   * @param connectionResources Database connection resources.
   * @param sourceSchema Source schema.
   * @param targetSchema Target schema.
   * @param upgradeStatements Upgrade statements identified.
   * @param viewChanges Changes needed to the views.
   * @param upgradesToApply Upgrade steps identified.
   * @param graphBasedUpgradeBuilder Builder for the Graph Based Upgrade
   * @param upgradeAuditCount Number of already applied upgrade steps
   * @return An upgrade path.
   */
  private UpgradePath buildUpgradePath(
      ConnectionResources connectionResources, Schema sourceSchema, Schema targetSchema,
      List<String> upgradeStatements, ViewChanges viewChanges,
      List<UpgradeStep> upgradesToApply,
      GraphBasedUpgradeBuilder graphBasedUpgradeBuilder,
      long upgradeAuditCount) {

    List<String> pathValidationSql = databaseUpgradePathValidationService.getPathValidationSql(upgradeAuditCount);

    UpgradePath path = factory.create(upgradesToApply, connectionResources, graphBasedUpgradeBuilder, pathValidationSql);

    path.writeSql(UpgradeHelper.preSchemaUpgrade(new UpgradeSchemas(sourceSchema, targetSchema), viewChanges, viewChangesDeploymentHelper));

    path.writeSql(upgradeStatements);

    path.writeSql(UpgradeHelper.postSchemaUpgrade(new UpgradeSchemas(sourceSchema, targetSchema), viewChanges, viewChangesDeploymentHelper));

    // Since Oracle is not able to re-map schema references in trigger code, we need to rebuild all triggers
    // for id column autonumbering when exporting and importing data between environments.
    // We will drop-and-recreate triggers whenever there are upgrade steps to execute. Ideally we'd want to do
    // this step once, however there's no easy way to do that with our upgrade framework.
    if (!upgradesToApply.isEmpty()) {

      AtomicBoolean first = new AtomicBoolean(true);
      targetSchema.tables().stream()
        .map(t -> connectionResources.sqlDialect().rebuildTriggers(t))
        .filter(sql -> !sql.isEmpty())
        .peek(sql -> {
            if (first.compareAndSet(true, false)) {
              path.writeSql(ImmutableList.of(
                connectionResources.sqlDialect().convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
              ));
            }
        })
        .forEach(path::writeSql);
    }

    return path;
  }


  /**
   * Gets a copy of the source schema from the {@code database}.
   * Also gathers up any schema consistency statements.
   *
   * @param database the database to connect to.
   * @param dataSource the dataSource to use.
   * @param exclusionRegExes collection of regular expressions describing tables/views to exclude from the schema.
   * @param upgradeStatements adds schema consistency statements to this list.
   * @return the schema.
   */
  private static Schema readSourceDatabaseSchema(ConnectionResources database, DataSource dataSource, Collection<String> exclusionRegExes, List<String> upgradeStatements) {
    try (SchemaResource databaseSchemaResource = database.openSchemaResource(dataSource)) {
      List<String> schemaConsistencyStatements = database.sqlDialect().getSchemaConsistencyStatements(databaseSchemaResource);
      if (!schemaConsistencyStatements.isEmpty()) {
        log.warn("Auto-healing statements have been generated (" + schemaConsistencyStatements.size() + " statements in total); this usually implies auto-healing being carried out."
            + " It this is shown on each subsequent start-up, it can be a symptom of auto-healing failing to achieve an acceptable stable healthful state."
            + " Examine the upgrade statements (the upgrade script, or the upgrade logs below) to investigate further.");
      }
      upgradeStatements.addAll(schemaConsistencyStatements);
      return copy(databaseSchemaResource, exclusionRegExes);
    }
  }


  /**
   * Provides a number of already applied upgrade steps.
   * @return the number of upgrade steps from the UpgradeAudit table
   */
  long getUpgradeAuditRowCount(ResultSetProcessor<Long> processor) {
    SelectStatement selectStatement = selectUpgradeAuditTableCount();
    long appliedUpgradeStepsCount = -1;
    try {
      SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
      appliedUpgradeStepsCount = sqlScriptExecutorProvider.get()
              .executeQuery(connectionResources.sqlDialect().convertStatementToSQL(selectStatement), processor);
    }
    catch (Exception e) {
      log.warn("Unable to read from UpgradeAudit table", e);
    }
    log.debug("Returning number of applied upgrade steps [" + appliedUpgradeStepsCount + "]");
    return appliedUpgradeStepsCount;
  }

  /**
   * This method queries the database for upgrade audit information, including
   * upgrade UUIDs and their corresponding descriptions.
   *
   * @return A Map<String, String> containing upgrade audit information.
   *         The keys are upgrade descriptions and the values are corresponding UUIDs.
   *         If an error occurs during the retrieval, an empty map is returned.
   */
  private Map<String, String> getUpgradeAuditRecords() {
    Map<String, String> upgradeAuditMap = new HashMap<>();
    try {
      TableReference upgradeAuditTable = tableRef(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME);

      SelectStatement selectStatement = select(
              upgradeAuditTable.field("upgradeUUID"),
              upgradeAuditTable.field("description")
      )
              .from(upgradeAuditTable)
              .build();

      SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
      upgradeAuditMap = sqlScriptExecutorProvider.get().executeQuery(
              connectionResources.sqlDialect().convertStatementToSQL(selectStatement),
              resultSetProcessor()
      );
    } catch (Exception e) {
      log.warn("Unable to read from UpgradeAudit table", e);
    }
    return upgradeAuditMap;
  }

  /**
   * Returns a ResultSetProcessor that processes a ResultSet into a Map<String, String>.
   *
   * @return A ResultSetProcessor that converts a ResultSet into a Map<String, String>.
   */
  private ResultSetProcessor<Map<String, String>> resultSetProcessor() {
    return resultSet -> {
      Map<String, String> upgradeAudit = new HashMap<>();
      try {
        while(resultSet.next()) {
          String description = resultSet.getString("description");
          String upgradeUUID = resultSet.getString("upgradeUUID");
          upgradeAudit.put(description, upgradeUUID);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
      return upgradeAudit;
    };
  }


  /**
   * Creates a select statement which can be used to count the number of upgrade steps that have already been run
   */
  private SelectStatement selectUpgradeAuditTableCount() {
    TableReference upgradeAuditTable = tableRef(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME);
    return select(count(upgradeAuditTable.field("upgradeUUID")))
        .from(upgradeAuditTable)
        .build();
  }


  /**
   * Factory that can be used to create {@link Upgrade}s.
   *
   * @author Copyright (c) Alfa Financial Software 2022
   */
  public static class Factory  {
    private final UpgradePathFactory upgradePathFactory;
    private final GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory;
    private final UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory;
    private final ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory;
    private final ViewDeploymentValidator.Factory viewDeploymentValidatorFactory;
    private final DatabaseUpgradePathValidationService.Factory databaseUpgradeLockServiceFactory;


    @Inject
    public Factory(UpgradePathFactory upgradePathFactory,
                   UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory,
                   GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory,
                   ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory,
                   ViewDeploymentValidator.Factory viewDeploymentValidatorFactory,
                   DatabaseUpgradePathValidationService.Factory databaseUpgradeLockServiceFactory) {
      this.upgradePathFactory = upgradePathFactory;
      this.graphBasedUpgradeBuilderFactory = graphBasedUpgradeBuilderFactory;
      this.upgradeStatusTableServiceFactory =  upgradeStatusTableServiceFactory;
      this.viewChangesDeploymentHelperFactory = viewChangesDeploymentHelperFactory;
      this.viewDeploymentValidatorFactory = viewDeploymentValidatorFactory;
      this.databaseUpgradeLockServiceFactory = databaseUpgradeLockServiceFactory;
    }

    public Upgrade create(ConnectionResources connectionResources) {
      return new Upgrade(connectionResources,
                         upgradePathFactory,
                         upgradeStatusTableServiceFactory.create(connectionResources),
                         viewChangesDeploymentHelperFactory.create(connectionResources),
                         viewDeploymentValidatorFactory.createViewDeploymentValidator(connectionResources),
                         graphBasedUpgradeBuilderFactory,
                         databaseUpgradeLockServiceFactory.create(connectionResources));
    }
  }
}