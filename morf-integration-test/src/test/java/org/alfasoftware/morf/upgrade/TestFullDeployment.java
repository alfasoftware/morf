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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.guicesupport.MorfModule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.Deployment.DeploymentFactory;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.alfasoftware.morf.upgrade.upgrade.AddExtraLoggingToUpgradeAuditTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Tests that domain classes can be deployed to a database.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@NotThreadSafe
public class TestFullDeployment {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private DataSource dataSource;
  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager databaseSchemaManager;
  @Inject private UpgradeStatusTableService upgradeStatusTableService;

  @Before
  public void before() {
    databaseSchemaManager.dropTablesIfPresent(ImmutableSet.of("FirstTestBean", "SecondTestBean"));
  }

  @After
  public void after() {
    databaseSchemaManager.invalidateCache();
  }

  /**
   * Tests full deployment with two simple domain classes
   * @throws SQLException If database access fails.
   */
  @Test
  public void testTwoClassDeployment() throws SQLException {
    Schema targetSchema = schema(
      table("FirstTestBean").columns(
        column("identifier", DataType.DECIMAL, 10).nullable(),
        column("stringColumn", DataType.STRING, 10).nullable(),
        column("doubleColumn", DataType.DECIMAL, 13, 2)
      ),
      table("SecondTestBean").columns(
        column("identifier", DataType.DECIMAL, 10).nullable(),
        column("intColumn", DataType.DECIMAL, 10).nullable()
      )
    );

    // Try accessing the new database
    Connection connection = dataSource.getConnection();
    try {
      // -- Set up the database...
      //
      DeploymentFactory deploymentFactory = Guice.createInjector(new MorfModule(), new AbstractModule() {
        @Override
        protected void configure() {
          bind(SqlDialect.class).toInstance(connectionResources.sqlDialect());
          bind(DataSource.class).toInstance(connectionResources.getDataSource()); // TODO Need to discuss more widely about what we want to do here
          bind(ConnectionResources.class).toInstance(connectionResources);
        }
      }).getInstance(DeploymentFactory.class);

      deploymentFactory.create(connectionResources).deploy(targetSchema);

      String schemaNamePrefix = connectionResources.sqlDialect().schemaNamePrefix();

      // A simple query
      Statement statement = connection.createStatement();
      assertFalse("Empty select results", statement.executeQuery("select * from "+schemaNamePrefix+"FirstTestBean").next());

      // An insert followed by a read
      statement.execute("insert into "+schemaNamePrefix+"SecondTestBean values(0, 33)");
      ResultSet resultSet = statement.executeQuery("select * from "+schemaNamePrefix+"SecondTestBean");
      assertTrue("Second result set has a record", resultSet.next());
      assertEquals("Column value", 33, resultSet.getInt("intColumn"));
      assertFalse("Second result set has exactly one record", resultSet.next());
    } finally {
      upgradeStatusTableService.tidyUp(connectionResources.getDataSource());
      connection.close();
    }
  }



  /**
   * Tests full deployment with two simple domain classes
   * @throws SQLException If database access fails.
   */
  @Test
  public void testTwoClassDeploymentWithUpgradeSteps() throws SQLException {
    Schema targetSchema = schema(
            table("UpgradeAudit").columns(
                    column("upgradeUUID",      DataType.STRING, 100).primaryKey(),
                    column("description",      DataType.STRING, DatabaseUpgradeTableContribution.UPGRADE_STEP_DESCRIPTION_LENGTH).nullable(),
                    column("appliedTime",      DataType.DECIMAL, 14).nullable(),
                    column("status",           DataType.STRING,  10).nullable(),
                    column("server",           DataType.STRING,  100).nullable(),
                    column("processingTimeMs", DataType.DECIMAL, 14).nullable(),
                    column("startTimeMs",      DataType.DECIMAL, 18).nullable()
            ),
            table("FirstTestBean").columns(
                    column("identifier", DataType.DECIMAL, 10).nullable(),
                    column("stringColumn", DataType.STRING, 10).nullable(),
                    column("doubleColumn", DataType.DECIMAL, 13, 2)
            ),
            table("SecondTestBean").columns(
                    column("identifier", DataType.DECIMAL, 10).nullable(),
                    column("intColumn", DataType.DECIMAL, 10).nullable()
            )
    );

    final List<Class<? extends UpgradeStep>> upgradeStepList = ImmutableList.of(
            AddExtraLoggingToUpgradeAuditTable.class
    );

    // Try accessing the new database
    Connection connection = dataSource.getConnection();
    try {
      // -- Set up the database...
      //
      DeploymentFactory deploymentFactory = Guice.createInjector(new MorfModule(), new AbstractModule() {
        @Override
        protected void configure() {
          bind(SqlDialect.class).toInstance(connectionResources.sqlDialect());
          bind(DataSource.class).toInstance(connectionResources.getDataSource()); // TODO Need to discuss more widely about what we want to do here
          bind(ConnectionResources.class).toInstance(connectionResources);
        }
      }).getInstance(DeploymentFactory.class);

      deploymentFactory.create(connectionResources).deploy(targetSchema);


      Upgrade.performUpgrade(targetSchema, upgradeStepList, connectionResources, new ViewDeploymentValidator.AlwaysValidate());

      String schemaNamePrefix = connectionResources.sqlDialect().schemaNamePrefix();

      // A simple query
      Statement statement = connection.createStatement();
      assertFalse("Empty select results", statement.executeQuery("select * from "+schemaNamePrefix+"FirstTestBean").next());

      // An insert followed by a read
      statement.execute("insert into "+schemaNamePrefix+"SecondTestBean values(0, 33)");
      ResultSet resultSet = statement.executeQuery("select * from "+schemaNamePrefix+"SecondTestBean");
      assertTrue("Second result set has a record", resultSet.next());
      assertEquals("Column value", 33, resultSet.getInt("intColumn"));
      assertFalse("Second result set has exactly one record", resultSet.next());

      ResultSet resultSetUpgradeAudit = statement.executeQuery("select * from "+schemaNamePrefix+"UpgradeAudit");
      ResultSetMetaData resultSetMetaData = resultSetUpgradeAudit.getMetaData();

      assertEquals("Column Count", 7, resultSetMetaData.getColumnCount());
    } finally {
      upgradeStatusTableService.tidyUp(connectionResources.getDataSource());
      connection.close();
    }
  }



}
