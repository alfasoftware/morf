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
package org.alfasoftware.morf.guicesupport;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.upgrade.DatabaseUpgradePathValidationService;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeBuilder.GraphBasedUpgradeBuilderFactory;
import org.alfasoftware.morf.upgrade.TableContribution;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfiguration;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradeStatusTableService;
import org.alfasoftware.morf.upgrade.ViewChangesDeploymentHelper;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice bindings for Morf module.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MorfModule extends AbstractModule {
  /**
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), UpgradeScriptAddition.class);

    Multibinder<TableContribution> tableMultibinder = Multibinder.newSetBinder(binder(), TableContribution.class);
    tableMultibinder.addBinding().to(DatabaseUpgradeTableContribution.class);
  }


  /**
   * Singleton provider creating an instance of {@link Upgrade}.
   *
   * @param connectionResources the connection resources
   * @param factory the upgrade path factory
   * @param upgradeStatusTableService the service class for managing the status of temporary upgrade tables
   * @param viewChangesDeploymentHelper the view deployment helper
   * @param viewDeploymentValidator the view deployment validator
   * @param graphBasedUpgradeBuilderFactory the graph based upgrade builder
   * @return the singleton instance of {@link Upgrade}.
   */
  @Provides
  @Singleton
  public Upgrade provideUpgrade(ConnectionResources connectionResources,
      UpgradePathFactory factory,
      UpgradeStatusTableService upgradeStatusTableService,
      ViewChangesDeploymentHelper viewChangesDeploymentHelper,
      ViewDeploymentValidator viewDeploymentValidator,
      GraphBasedUpgradeBuilderFactory graphBasedUpgradeBuilderFactory,
      DatabaseUpgradePathValidationService databaseUpgradePathValidationService,
      UpgradeConfiguration upgradeConfiguration) {
    return new Upgrade(connectionResources, factory, upgradeStatusTableService, viewChangesDeploymentHelper,
      viewDeploymentValidator, graphBasedUpgradeBuilderFactory, databaseUpgradePathValidationService,
      upgradeConfiguration);
  }
}

