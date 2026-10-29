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

package org.alfasoftware.morf.examples;

import static org.alfasoftware.morf.metadata.DataType.BIG_INTEGER;
import static org.alfasoftware.morf.metadata.DataType.STRING;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;

import java.util.Arrays;
import java.util.Collection;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.jdbc.ConnectionResourcesBean;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.Deployment;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;

/**
 * The smallest useful Morf program: create a database from scratch, then
 * evolve it with upgrade steps and say hello.
 *
 * <p>Run {@link #main(String[])} to see it work. It uses an in-memory H2
 * database, so there is nothing to install or clean up.</p>
 */
public class HelloMorf {

  public static void main(String[] args) {

    ConnectionResourcesBean connection = new ConnectionResourcesBean();
    connection.setDatabaseType("H2");
    connection.setHostName("localhost");
    connection.setDatabaseName("hello");
    connection.setUserName("test");
    connection.setPassword("test");

    // 1. Describe the database as you want it to be, and the steps which lead there.
    Schema schemaV1 = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Greeting").columns(
        column("id", BIG_INTEGER).autoNumbered(1),
        column("message", STRING, 100)
      )
    );
    Collection<Class<? extends UpgradeStep>> stepsV1 = Arrays.asList(CreateGreeting.class);

    // 2. Morf builds the whole database from scratch. No SQL, no scripts.
    Deployment.deploySchema(schemaV1, stepsV1, connection);

    // 3. Later, you want to record a language and say hello. Describe the new target and add some steps...
    Schema schemaV2 = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Greeting").columns(
        column("id", BIG_INTEGER).autoNumbered(1),
        column("message", STRING, 100),
        column("language", STRING, 10)
      )
    );
    Collection<Class<? extends UpgradeStep>> stepsV2 = Arrays.asList(CreateGreeting.class, AddLanguage.class, SayHello.class);

    // 4. ...and Morf runs whichever ones are new, then checks the result matches your target.
    Upgrade.performUpgrade(schemaV2, stepsV2, connection, new UpgradeConfigAndContext(), new ViewDeploymentValidator.AlwaysValidate());
    printGreetings(connection);
  }


  private static void printGreetings(ConnectionResourcesBean connection) {
    DatabaseDataSetProducer database = new DatabaseDataSetProducer(connection);
    database.open();
    try {
      for (Record record : database.records("Greeting")) {
        System.out.println(record.getString("message") + " (" + record.getString("language") + ")");
      }
    } finally {
      database.close();
    }
  }


  @Sequence(1761700000)
  @UUID("5a1f0c3e-8d2b-4f6a-9b47-2c1e7d90a3f1")
  @Version("1.0.0")
  static final class CreateGreeting implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "HELLO-1";
    }

    @Override
    public String getDescription() {
      return "Create the Greeting table";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(
        table("Greeting").columns(
          column("id", BIG_INTEGER).autoNumbered(1),
          column("message", STRING, 100)
        )
      );
    }
  }


  @Sequence(1761700100)
  @UUID("c7b2e914-3a58-4d0f-86e1-b5f4a9d2706c")
  @Version("1.0.0")
  static final class AddLanguage implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "HELLO-2";
    }

    @Override
    public String getDescription() {
      return "Record which language each greeting is in";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addColumn("Greeting", column("language", STRING, 10), literal("en"));
    }
  }


  @Sequence(1761700200)
  @UUID("0e9d4b72-61c3-4a8e-b2f5-d83a7c15e946")
  @Version("1.0.0")
  static final class SayHello implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "HELLO-3";
    }

    @Override
    public String getDescription() {
      return "Say hello";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      data.executeStatement(
        insert()
          .into(tableRef("Greeting"))
          .values(literal("Hello, Morf!").as("message"), literal("en").as("language"))
      );
    }
  }
}
