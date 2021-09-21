/* Copyright 2018 Alfa Financial Software
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

package org.alfasoftware.morf.examples;

import static com.google.common.collect.Iterables.size;
import static org.alfasoftware.morf.metadata.DataType.BIG_INTEGER;
import static org.alfasoftware.morf.metadata.DataType.STRING;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.function.Consumer;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.ConnectionResourcesBean;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.Deployment;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.ViewChangesDeploymentHelper;
import org.junit.Test;

import com.google.common.collect.FluentIterable;

/**
 * Tests that the "Start Here" example at https://github.com/alfasoftware/morf/wiki/Start-Here
 * works correctly.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2018
 */
public class TestStartHere {

  /**
   * Tests the full example, checking that the result is correct.
   */
  @Test
  public void testStartHereFullExample() {

    // Define the target database schema
    Schema targetSchema = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Test1").columns(
        column("id", BIG_INTEGER).autoNumbered(1),
        column("val", STRING, 100)
      ),
      table("Test2").columns(
        column("id", BIG_INTEGER).autoNumbered(1),
        column("val", STRING, 100)
      )
    );

    // Any upgrade steps which lead to the baseline.  These will be
    // assumed to have already run.
    Collection<Class<? extends UpgradeStep>> upgradeSteps = new HashSet<>();
    upgradeSteps.add(CreateTest1.class);
    upgradeSteps.add(CreateTest2.class);

    // And the database
    ConnectionResourcesBean connectionResources = new ConnectionResourcesBean();
    connectionResources.setDatabaseType("H2");
    connectionResources.setHostName("localhost");
    connectionResources.setDatabaseName(TestStartHere.class.getName());
    connectionResources.setUserName("test");
    connectionResources.setPassword("test");

    // Deploy the schema
    Deployment.deploySchema(targetSchema, upgradeSteps, connectionResources, new ViewChangesDeploymentHelper(connectionResources.sqlDialect()));

    // Confirm that the database has been correctly initialised
    withCurrentDatabase(connectionResources, dataSetProducer -> {
      Schema schema = dataSetProducer.getSchema();
      assertThat(
        FluentIterable.from(schema.tables()).transform(Table::getName).transform(String::toLowerCase),
        containsInAnyOrder(deployedViewsTable().getName().toLowerCase(), upgradeAuditTable().getName().toLowerCase(), "test1", "test2")
      );
    });

    // Now let's extend the schema, with an extra table and a view
    targetSchema = schema(
      targetSchema,
      schema(
        table("Test3").columns(
          column("id", BIG_INTEGER).autoNumbered(1),
          column("val", STRING, 100)
        )
      ),
      schema(
        view("Test3View", select().from("Test3"))
      )
    );

    // Add the upgrade to create it
    upgradeSteps.add(CreateTest3.class);

    // Run the upgrade
    Upgrade.performUpgrade(targetSchema, upgradeSteps, connectionResources, new ViewChangesDeploymentHelper(connectionResources.sqlDialect()));

    // Confirm that the database has been correctly upgraded
    withCurrentDatabase(connectionResources, dataSetProducer -> {
      Schema schema = dataSetProducer.getSchema();
      assertThat(
        FluentIterable.from(schema.tables()).transform(Table::getName).transform(String::toLowerCase),
        containsInAnyOrder(deployedViewsTable().getName().toLowerCase(), upgradeAuditTable().getName().toLowerCase(), "test1", "test2", "test3")
      );
      assertThat(
        size(dataSetProducer.records("Test3")),
        equalTo(1)
      );
    });
  }

  private void withCurrentDatabase(ConnectionResources connectionResources, Consumer<? super DataSetProducer> consumer) {
    DatabaseDataSetProducer databaseDataSetProducer = new DatabaseDataSetProducer(connectionResources);
    databaseDataSetProducer.open();
    try {
      consumer.accept(databaseDataSetProducer);
    } finally {
      databaseDataSetProducer.close();
    }
  }

  @Sequence(1496853575)
  @UUID("e3667c15-74b5-4bed-b87f-36a5f686e8db")
  @Version("0.0.1")
  static final class CreateTest1 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "FOO-1";
    }

    @Override
    public String getDescription() {
      return "Create table Test1";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(
        table("Test1").columns(
          column("id", BIG_INTEGER).autoNumbered(1),
          column("val", STRING, 100)
        )
      );
    }
  }

  @Sequence(1496853658)
  @UUID("94a97d5a-2ac6-46fd-9249-dc61b1b8c90a")
  @Version("0.0.1")
  static final class CreateTest2 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "FOO-1";
    }

    @Override
    public String getDescription() {
      return "Create table Test2";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(
        table("Test2").columns(
          column("id", BIG_INTEGER).autoNumbered(1),
          column("val", STRING, 100)
        )
      );
    }
  }

  @Sequence(1496853841)
  @UUID("d962f6d0-6bfe-4c9f-847b-6319ad99ba54")
  @Version("0.0.1")
  static final class CreateTest3 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "FOO-1";
    }

    @Override
    public String getDescription() {
      return "Create table Test3";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(
        table("Test3").columns(
          column("id", BIG_INTEGER).autoNumbered(1),
          column("val", STRING, 100)
        )
      );
      data.executeStatement(
        insert()
        .into(tableRef("Test1"))
        .values(literal("Foo").as("val"))
      );
      data.executeStatement(
        insert()
        .into(tableRef("Test3"))
        .from(
          select(
            field("id"),
            field("val"))
          .from("Test1")
        )
      );
    }
  }
}
