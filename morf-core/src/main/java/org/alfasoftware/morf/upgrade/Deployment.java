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

import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;

import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * Deploys a full database schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class Deployment {

  /**
   * Provides {@link SqlScriptExecutor}s.
   */
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;

  /**
   * The SQL dialect.
   */
  private final SqlDialect sqlDialect;

  private final UpgradePathFactory upgradePathFactory;


  /**
   * Constructor.
   *
   * @param sqlScriptExecutorProvider a provider of {@link SqlScriptExecutor}s.
   */
  @Inject
  Deployment(SqlDialect sqlDialect, SqlScriptExecutorProvider sqlScriptExecutorProvider, UpgradePathFactory upgradePathFactory) {
    super();
    this.sqlDialect = sqlDialect;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.upgradePathFactory = upgradePathFactory;
  }


  /**
   * Constructor for use by {@link DeploymentFactory}.
   */
  private Deployment(UpgradePathFactory upgradePathFactory, @Assisted ConnectionResources connectionResources) {
    super();
    this.sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources);
    this.sqlDialect = connectionResources.sqlDialect();
    this.upgradePathFactory = upgradePathFactory;
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
      sqlStatementWriter.writeSql(sqlDialect.tableDeploymentStatements(table));
    }

    // Iterate through all the views and deploy them - will deploy in
    // dependency order.
    ViewChanges viewChanges = new ViewChanges(targetSchema.views(), new HashSet<View>(), targetSchema.views());
    for (View view : viewChanges.getViewsToDeploy()) {
      sqlStatementWriter.writeSql(sqlDialect.viewDeploymentStatements(view));
      if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        sqlStatementWriter.writeSql(
          sqlDialect.convertStatementToSQL(
            insert().into(tableRef(DEPLOYED_VIEWS_NAME))
                                 .values(literal(view.getName().toUpperCase()).as("name"),
                                         literal(sqlDialect.convertStatementToHash(view.getSelectStatement())).as("hash"))
          )
        );
      }
    }
  }


  /**
   * Creates deployment statements using the supplied source meta data.
   *
   * @param targetSchema Schema that is to be deployed.
   */
  public void deploy(Schema targetSchema) {
    UpgradePath path = getPath(targetSchema, new ArrayList<Class<? extends UpgradeStep>>());
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
    final UpgradePath path = upgradePathFactory.create(sqlDialect);
    writeStatements(targetSchema, path);
    writeUpgradeSteps(upgradeSteps, path);

    return path;
  }


  /**
   * Add an upgrade step for each upgrade step.
   *
   * <p>The {@link Deployment} class ensures that all contributed domain tables are written in the database, but we need to
   * ensure that the upgrade steps that were used to create are in written so that next time the application is run the upgrader
   * doesn't try to recreate/upgrade them.</p>
   *
   * @param upgradeSteps All available upgrade steps.
   * @param upgradePath Recipient for the deployment statements.
   */
  private void writeUpgradeSteps(Collection<Class<? extends UpgradeStep>> upgradeSteps, UpgradePath upgradePath) {
    for(Class<? extends UpgradeStep> upgradeStep : upgradeSteps) {
      UUID uuid = UpgradePathFinder.readUUID(upgradeStep);
      InsertStatement insertStatement = AuditRecordHelper.createAuditInsertStatement(uuid, upgradeStep.getName());
      upgradePath.writeSql(sqlDialect.convertStatementToSQL(insertStatement));
    }
  }


  /**
   * Factory interface that can be used to create {@link Deployment}s.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  @ImplementedBy(DeploymentFactoryImpl.class)
  public static interface DeploymentFactory {

    /**
     * Creates an instance of {@link Deployment}.
     *
     * @param connectionResources The connection to use.
     * @return The resulting deployment.
     */
    public Deployment create(ConnectionResources connectionResources);
  }


  /**
   * Implementation of {@link DeploymentFactory}.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  static final class DeploymentFactoryImpl implements DeploymentFactory {

    private final UpgradePathFactory upgradePathFactory;

    @Inject
    public DeploymentFactoryImpl(UpgradePathFactory upgradePathFactory) {
      this.upgradePathFactory = upgradePathFactory;
    }

    @Override
    public Deployment create(ConnectionResources connectionResources) {
      return new Deployment(upgradePathFactory, connectionResources);
    }
  }
}