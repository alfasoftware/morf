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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * End-to-end lifecycle integration tests for the deferred index mechanism.
 * Exercises upgrade, restart, force-build, and execute cycles through the
 * real {@link Upgrade#performUpgrade} path using replay-based discovery.
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


  // =========================================================================
  // Inner upgrade step classes
  // =========================================================================

  /** Adds a deferred index on Product.name. */
  @Sequence(90001)
  @UUID("d1f00001-0001-0001-0001-000000000001")
  @Version("1.0.0")
  public static class StepAddFirstIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add first deferred index"; }
  }


  /** Adds a second deferred index on Product(id, name). */
  @Sequence(90002)
  @UUID("d1f00002-0002-0002-0002-000000000002")
  @Version("1.0.0")
  public static class StepAddSecondIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_IdName_1").columns("id", "name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add second deferred index"; }
  }


  /** Create fresh schema before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
  }


  /** Invalidate schema manager cache after each test. */
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
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    executeDeferredViaService(Collections.singletonList(StepAddFirstIndex.class));
    assertIndexExists("Product", "Product_Name_1");

    // Restart with same steps — no new upgrade, should pass cleanly
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
  }


  // =========================================================================
  // No-upgrade restart — pending indexes left for execute()
  // =========================================================================

  /** No-upgrade restart with pending indexes passes (schema augmented via replay). */
  @Test
  public void testNoUpgradeRestart_pendingIndexesAugmented() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Restart with same schema — no new upgrade steps
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);

    // Index should NOT exist yet — no force-build on no-upgrade restart
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Execute builds it
    executeDeferredViaService(Collections.singletonList(StepAddFirstIndex.class));
    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Upgrade with pending indexes — force-built before proceeding
  // =========================================================================

  /** Upgrade with pending indexes from previous upgrade force-builds them. */
  @Test
  public void testUpgrade_pendingIndexesForceBuiltBeforeProceeding() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Second upgrade — readiness check should force-build first index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    assertIndexExists("Product", "Product_Name_1");

    // Execute builds second index
    executeDeferredViaService(List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    assertIndexExists("Product", "Product_IdName_1");
  }


  // =========================================================================
  // Crash recovery
  // =========================================================================

  /** Index not in DB after crash; execute() discovers and builds it via replay. */
  @Test
  public void testCrashRecovery_missingIndexRebuiltOnExecute() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Simulate: executor never ran (or crashed before building)
    executeDeferredViaService(Collections.singletonList(StepAddFirstIndex.class));
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Index already in DB; execute() discovers nothing missing. */
  @Test
  public void testExecuteSkipsAlreadyBuiltIndex() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);

    // Manually build the index to simulate a completed build
    buildIndexManually("Product", "Product_Name_1", "name");
    assertIndexExists("Product", "Product_Name_1");

    List<DeferredAddIndex> missing = findMissing(Collections.singletonList(StepAddFirstIndex.class));
    assertEquals("No missing indexes when already built", 0, missing.size());
  }


  // =========================================================================
  // Schema augmentation
  // =========================================================================

  /** Augmentation adds missing deferred indexes to source schema. */
  @Test
  public void testAugmentSchemaWithMissingDeferredIndexes() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);

    Schema source;
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      source = SchemaUtils.copy(sr);
    }

    assertFalse("Source should not have index",
        source.getTable("Product").indexes().stream()
            .anyMatch(idx -> "Product_Name_1".equalsIgnoreCase(idx.getName())));

    DeferredIndexReadinessCheck rc = DeferredIndexReadinessCheck.create(connectionResources);
    Schema augmented = rc.augmentSchemaWithPendingIndexes(source,
        Collections.singletonList(StepAddFirstIndex.class));

    assertTrue("Augmented schema should have index",
        augmented.getTable("Product").indexes().stream()
            .anyMatch(idx -> "Product_Name_1".equalsIgnoreCase(idx.getName())));
  }


  // =========================================================================
  // Two sequential upgrades
  // =========================================================================

  /** Two upgrades, both executed — third restart passes. */
  @Test
  public void testTwoSequentialUpgrades() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    executeDeferredViaService(Collections.singletonList(StepAddFirstIndex.class));
    assertIndexExists("Product", "Product_Name_1");

    // Second upgrade adds another deferred index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    executeDeferredViaService(List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    assertIndexExists("Product", "Product_IdName_1");

    // Third restart — everything clean
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
  }


  /** Two upgrades, first index not built — force-built before second. */
  @Test
  public void testTwoUpgrades_firstIndexNotBuilt_forceBuiltBeforeSecond() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    // Second upgrade — readiness check should force-build first index
    performUpgradeWithSteps(schemaWithBothIndexes(),
        List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    assertIndexExists("Product", "Product_Name_1");

    // Execute builds second index
    executeDeferredViaService(List.of(StepAddFirstIndex.class, StepAddSecondIndex.class));
    assertIndexExists("Product", "Product_IdName_1");
  }


  // =========================================================================
  // DeferredIndexService end-to-end
  // =========================================================================

  /** Full service lifecycle: setUpgradeSteps, execute, awaitCompletion, getProgress. */
  @Test
  public void testServiceEndToEnd() {
    performUpgrade(schemaWithFirstIndex(), StepAddFirstIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    config.setMaxRetries(1);
    DeferredIndexReadinessCheck readinessCheck = DeferredIndexReadinessCheck.create(connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, new SqlScriptExecutorProvider(connectionResources),
        config, new DeferredIndexExecutorServiceFactory.Default());
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(executor, config, readinessCheck);
    service.setUpgradeSteps(Collections.singletonList(StepAddFirstIndex.class));
    service.execute();
    assertTrue("Should complete", service.awaitCompletion(60));

    assertIndexExists("Product", "Product_Name_1");

    DeferredIndexProgress progress = service.getProgress();
    assertEquals(1, progress.getTotal());
    assertEquals(1, progress.getCompleted());
    assertEquals(0, progress.getFailed());
    assertEquals(0, progress.getRemaining());
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


  private void executeDeferredViaService(Collection<Class<? extends UpgradeStep>> steps) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    config.setMaxRetries(1);
    DeferredIndexReadinessCheck readinessCheck = DeferredIndexReadinessCheck.create(connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, new SqlScriptExecutorProvider(connectionResources),
        config, new DeferredIndexExecutorServiceFactory.Default());
    DeferredIndexServiceImpl service = new DeferredIndexServiceImpl(executor, config, readinessCheck);
    service.setUpgradeSteps(steps);
    service.execute();
    service.awaitCompletion(60);
  }


  private List<DeferredAddIndex> findMissing(Collection<Class<? extends UpgradeStep>> steps) {
    DeferredIndexReadinessCheck rc = DeferredIndexReadinessCheck.create(connectionResources);
    return rc.findMissingDeferredIndexes(steps);
  }


  private Schema schemaWithFirstIndex() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
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


  private void buildIndexManually(String tableName, String indexName, String columnName) {
    sqlScriptExecutorProvider.get().execute(
        List.of("CREATE INDEX " + indexName + " ON " + tableName + " (" + columnName + ")")
    );
  }


  private void assertIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private void assertIndexDoesNotExist(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("Index " + indexName + " should not exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
