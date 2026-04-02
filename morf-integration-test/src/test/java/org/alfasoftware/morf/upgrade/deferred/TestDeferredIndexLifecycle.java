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

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.AddSecondDeferredIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * End-to-end lifecycle integration tests for the deferred index mechanism.
 * Exercises upgrade, restart, and execute cycles through the real
 * {@link Upgrade#performUpgrade} path using the comments-based model.
 *
 * <p>In the comments-based model, deferred indexes are declared in table
 * comments and the MetaDataProvider includes them as virtual indexes with
 * {@code isDeferred()=true}. After the executor physically builds them,
 * the physical index takes precedence and {@code isDeferred()} returns
 * {@code false}.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexLifecycle {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private UpgradeConfigAndContext upgradeConfigAndContext;

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );


  /** Create a fresh schema before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
    upgradeConfigAndContext.setDeferredIndexCreationEnabled(true);
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // =========================================================================
  // Happy path
  // =========================================================================

  /** Upgrade defers index, execute builds it, restart finds schema correct. */
  @Test
  public void testHappyPath_upgradeExecuteRestart() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertDeferredIndexPending("Product", "Product_Name_1");

    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // Restart -- same steps, nothing new to do
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    // Should pass without error
  }


  // =========================================================================
  // No-upgrade restart -- pending indexes left for execute()
  // =========================================================================

  /** No-upgrade restart with pending deferred indexes should pass. */
  @Test
  public void testNoUpgradeRestart_pendingIndexesVisible() {
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertDeferredIndexPending("Product", "Product_Name_1");

    // Restart with same schema -- no new upgrade steps
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);

    // Index should still be deferred (not physically built)
    assertDeferredIndexPending("Product", "Product_Name_1");

    // Execute builds it
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Two sequential upgrades
  // =========================================================================

  /** Two upgrades, both executed -- third restart passes. */
  @Test
  public void testTwoSequentialUpgrades() {
    // First upgrade
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // Second upgrade adds another deferred index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_IdName_1");

    // Third restart -- everything clean
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));
  }


  /** Two upgrades, first index not built -- first still deferred, second also deferred. */
  @Test
  public void testTwoUpgrades_firstIndexNotBuilt() {
    // First upgrade -- don't execute
    performUpgrade(schemaWithFirstIndex(), AddDeferredIndex.class);
    assertDeferredIndexPending("Product", "Product_Name_1");

    // Second upgrade
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(AddDeferredIndex.class, AddSecondDeferredIndex.class));

    // Execute builds both
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
  }


  // =========================================================================
  // Helpers
  // =========================================================================

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> step) {
    performUpgradeWithSteps(targetSchema, Collections.singletonList(step));
  }


  private void performUpgradeWithSteps(Schema targetSchema,
                                        List<Class<? extends UpgradeStep>> steps) {
    Upgrade.performUpgrade(targetSchema, steps, connectionResources,
        upgradeConfigAndContext, viewDeploymentValidator);
  }


  private void executeDeferred() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(1);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider,
        config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();
  }


  private Schema schemaWithFirstIndex() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
  }


  private Schema schemaWithBothIndexes() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );
  }


  private void assertDeferredIndexPending(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Deferred index " + indexName + " should be present with isDeferred()=true",
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName()) && idx.isDeferred()));
    }
  }


  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
