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

import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.NullFieldLiteral;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactoryImpl;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/**
 * Deploys a full database schema. Once the deployment is complete, the status of
 * the system will be left in {@link UpgradeStatus#DATA_TRANSFER_REQUIRED}. It is the
 * responsibility of the application to perform any remaining setup the database
 * at this point (for example, transferring in a start position using
 * {@link DatabaseDataSetConsumer}) and, once complete, call {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}.
 *
 * <h3>Usage</h3>
 * <pre><code>
 * deployment.deploy(targetSchema);
 * if (upgradeStatusTableService.writeStatusFromStatus(DATA_TRANSFER_REQUIRED, DATA_TRANSFER_IN_PROGRESS) == 1) {
 *   //
 *   // Set up the database start position
 *   //
 *   upgradeStatusTableService.writeStatusFromStatus(DATA_TRANSFER_IN_PROGRESS, COMPLETED);
 * }
 * // Normal start up
 * </code></pre>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class Deployment {

  /**
   * Provides {@link SqlScriptExecutor}s.
   */
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;

  /**
   * The connection resources.
   */

  private final ConnectionResources connectionResources;

  private final UpgradePathFactory upgradePathFactory;

  private final ViewChangesDeploymentHelper viewChangesDeploymentHelper;

  /**
   * Constructor.
   */
  @Inject
  Deployment(ConnectionResources connectionResources, SqlScriptExecutorProvider sqlScriptExecutorProvider, UpgradePathFactory upgradePathFactory, ViewChangesDeploymentHelper viewChangesDeploymentHelper) {
    super();
    this.connectionResources = connectionResources;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.upgradePathFactory = upgradePathFactory;
    this.viewChangesDeploymentHelper = viewChangesDeploymentHelper;
  }


  /**
   * Constructor for use by {@link DeploymentFactory}.
   */
  private Deployment(UpgradePathFactory upgradePathFactory, ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory, @Assisted ConnectionResources connectionResources) {
    super();
    this.sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
    this.connectionResources = connectionResources;
    this.upgradePathFactory = upgradePathFactory;
    this.viewChangesDeploymentHelper = viewChangesDeploymentHelperFactory.create(connectionResources);
  }


  /**
   * Creates deployment statements using the supplied source meta data.
   *
   * @param targetSchema Schema that is to be deployed.
   * @param sqlStatementWriter Recipient for the deployment statements.
   */
  private void writeStatements(Schema targetSchema, SqlStatementWriter sqlStatementWriter) {
    // Sort the tables by foreign key dependency order
    // TODO Implement table sorting by dependency for deployment.
    List<String> tableNames = new ArrayList<>(targetSchema.tableNames());

    // Iterate through all the tables and deploy them
    for (String tableName : tableNames) {
      Table table = targetSchema.getTable(tableName);
      sqlStatementWriter.writeSql(connectionResources.sqlDialect().tableDeploymentStatements(table));
    }

    // Iterate through all the views and deploy them - will deploy in dependency order.
    final boolean updateDeloyedViews = targetSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);
    ViewChanges viewChanges = new ViewChanges(targetSchema.views(), new HashSet<>(), targetSchema.views());
    for (View view : viewChanges.getViewsToDeploy()) {
      sqlStatementWriter.writeSql(viewChangesDeploymentHelper.createView(view, updateDeloyedViews));
    }
  }


  /**
   * Creates deployment statements using the supplied source meta data.
   *
   * @param targetSchema Schema that is to be deployed.
   */
  public void deploy(Schema targetSchema) {
    UpgradePath path = getPath(targetSchema, new ArrayList<>());
    sqlScriptExecutorProvider.get().execute(path.getSql());
  }


  /**
   * Creates deployment statements using the supplied source meta data.
   *
   * @param upgradeSteps All available upgrade steps.
   * @param targetSchema Schema that is to be deployed.
   */
  public void deploy(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    UpgradePath path = getPath(targetSchema, upgradeSteps);
    sqlScriptExecutorProvider.get().execute(path.getSql());
  }


  /**
   * Static convenience method which deploys the specified database schema, prepopulating
   * the upgrade step table with all pre-existing upgrade information. Assumes no initial
   * start position manipulation is required and the application can start with an empty
   * database.
   *
   * @param targetSchema The target database schema.
   * @param upgradeSteps All upgrade steps which should be deemed to have already run.
   * @param connectionResources Connection details for the database.
   */
  public static void deploySchema(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps, ConnectionResources connectionResources) {
    UpgradeStatusTableServiceImpl upgradeStatusTableService = new UpgradeStatusTableServiceImpl(
      new SqlScriptExecutorProvider(connectionResources), connectionResources.sqlDialect());
    try {
      new Deployment(
        new UpgradePathFactoryImpl(Collections.<UpgradeScriptAddition>emptySet(), UpgradeStatusTableServiceImpl::new),
        new ViewChangesDeploymentHelper.Factory(new CreateViewListener.Factory.NoOpFactory(), new DropViewListener.Factory.NoOpFactory()),
        connectionResources
      ).deploy(targetSchema, upgradeSteps);
    } finally {
      upgradeStatusTableService.tidyUp(connectionResources.getDataSource());
    }
  }


  /**
   * Return an "upgrade" path corresponding to a full database deployment, matching
   * the given schema.
   *
   * <p>This method adds all upgrade steps after creating the tables/views.</p>
   *
   * @param targetSchema Schema that is to be deployed.
   * @param upgradeSteps All available upgrade steps.
   * @return A path which can be executed to make {@code database} match {@code targetSchema}.
   */
  public UpgradePath getPath(Schema targetSchema, Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    final UpgradePath path = upgradePathFactory.create(connectionResources);
    writeStatements(targetSchema, path);
    writeUpgradeSteps(upgradeSteps, path);
    return path;
  }


  /**
   * Add an upgrade step for each upgrade step.
   *
   * <p>The {@link Deployment} class ensures that all contributed domain tables are written in the database, but we need to
   * ensure that the upgrade steps that were used to create are in written so that next time the application is run the upgrade
   * doesn't try to recreate/upgrade them.</p>
   *
   * @param upgradeSteps All available upgrade steps.
   * @param upgradePath Recipient for the deployment statements.
   */
  private void writeUpgradeSteps(Collection<Class<? extends UpgradeStep>> upgradeSteps, UpgradePath upgradePath) {
    for(Class<? extends UpgradeStep> upgradeStep : upgradeSteps) {
      UUID uuid = UpgradePathFinder.readUUID(upgradeStep);
      InsertStatement insertStatement = AuditRecordHelper.createAuditInsertStatement(uuid, upgradeStep.getName(), 0);
      upgradePath.writeSql(connectionResources.sqlDialect().convertStatementToSQL(insertStatement));
    }
  }


  /**
   * Factory interface that can be used to create {@link Deployment}s.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  @ImplementedBy(DeploymentFactoryImpl.class)
  public interface DeploymentFactory {

    /**
     * Creates an instance of {@link Deployment}.
     *
     * @param connectionResources The connection to use.
     * @return The resulting deployment.
     */
    Deployment create(ConnectionResources connectionResources);
  }


  /**
   * Implementation of {@link DeploymentFactory}.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  static final class DeploymentFactoryImpl implements DeploymentFactory {

    private final UpgradePathFactory upgradePathFactory;

    private final ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory;

    @Inject
    public DeploymentFactoryImpl(UpgradePathFactory upgradePathFactory, ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory) {
      this.upgradePathFactory = upgradePathFactory;
      this.viewChangesDeploymentHelperFactory = viewChangesDeploymentHelperFactory;
    }

    @Override
    public Deployment create(ConnectionResources connectionResources) {
      return new Deployment(upgradePathFactory, viewChangesDeploymentHelperFactory,  connectionResources);
    }
  }
}