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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for the {@link DeferredIndexService} facade, verifying
 * the full lifecycle through a real H2 database: upgrade steps declare
 * deferred indexes, then the service discovers missing indexes by replaying
 * the steps against the live schema and builds them.
 *
 * <p>The test sets up the Product table directly (without running the upgrade
 * framework), then invokes the service which replays the upgrade step classes
 * to discover deferred indexes that are missing from the database.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexService {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;

  private static final Schema INITIAL_SCHEMA = schema(
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );


  /** Create a fresh schema with the Product table before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // =========================================================================
  // Upgrade steps (inner static classes)
  // =========================================================================

  /**
   * Base class providing default jiraId and description for test steps.
   */
  abstract static class AbstractTestStep implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "DEFERRED-TEST";
    }

    @Override
    public String getDescription() {
      return "";
    }
  }


  /**
   * Declares a single deferred index on Product.name.
   */
  @Sequence(80001)
  @UUID("e2f00001-0001-0001-0001-000000000001")
  @Version("1.0.0")
  public static class AddSingleDeferredIndex extends AbstractTestStep {

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
    }
  }


  /**
   * Declares two deferred indexes on Product in a single step.
   */
  @Sequence(80002)
  @UUID("e2f00001-0001-0001-0001-000000000002")
  @Version("1.0.0")
  public static class AddTwoDeferredIndexes extends AbstractTestStep {

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
      schema.addIndexDeferred("Product", index("Product_IdName_1").columns("id", "name"));
    }
  }


  /**
   * Declares an immediate (non-deferred) index on Product.name.
   */
  @Sequence(80003)
  @UUID("e2f00001-0001-0001-0001-000000000003")
  @Version("1.0.0")
  public static class AddImmediateIndex extends AbstractTestStep {

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndex("Product", index("Product_Name_1").columns("name"));
    }
  }


  // =========================================================================
  // Tests
  // =========================================================================

  /** Verify that execute() discovers and builds a single deferred index end-to-end. */
  @Test
  public void testExecuteBuildsIndexEndToEnd() {
    assertIndexMissing("Product", "Product_Name_1");

    DeferredIndexService service = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    service.execute();
    service.awaitCompletion(60L);

    assertIndexExists("Product", "Product_Name_1");
  }


  /** Verify that execute() handles multiple deferred indexes in a single run. */
  @Test
  public void testExecuteBuildsMultipleIndexes() {
    assertIndexMissing("Product", "Product_Name_1");
    assertIndexMissing("Product", "Product_IdName_1");

    DeferredIndexService service = createAndConfigureService(
        Collections.singletonList(AddTwoDeferredIndexes.class));
    service.execute();
    service.awaitCompletion(60L);

    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  /** Verify that execute() with no deferred indexes in steps completes immediately. */
  @Test
  public void testExecuteWithEmptyQueue() {
    // AddImmediateIndex uses addIndex, not addIndexDeferred -- no deferred indexes to discover
    DeferredIndexService service = createAndConfigureService(
        Collections.singletonList(AddImmediateIndex.class));
    service.execute();

    assertTrue("Should complete immediately when no deferred indexes are missing",
        service.awaitCompletion(5L));
  }


  /** Verify that awaitCompletion() throws when called before execute(). */
  @Test(expected = IllegalStateException.class)
  public void testAwaitCompletionThrowsWhenNoExecution() {
    DeferredIndexService service = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    service.awaitCompletion(5L);
  }


  /** Verify that awaitCompletion() returns true after all indexes are built. */
  @Test
  public void testAwaitCompletionReturnsTrueWhenAllCompleted() {
    // First service builds the index
    DeferredIndexService firstService = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    firstService.execute();
    firstService.awaitCompletion(60L);
    assertIndexExists("Product", "Product_Name_1");

    // Second service discovers nothing missing and should return immediately
    DeferredIndexService secondService = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    secondService.execute();
    assertTrue("Should return true when all indexes already exist",
        secondService.awaitCompletion(5L));
  }


  /** Verify that a second execute() after index already exists is a safe no-op. */
  @Test
  public void testExecuteIdempotent() {
    // First run builds the index
    DeferredIndexService firstService = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    firstService.execute();
    firstService.awaitCompletion(60L);
    assertIndexExists("Product", "Product_Name_1");

    // Second run should discover nothing missing and complete immediately
    DeferredIndexService secondService = createAndConfigureService(
        Collections.singletonList(AddSingleDeferredIndex.class));
    secondService.execute();
    assertTrue("Second execute should be a no-op",
        secondService.awaitCompletion(5L));

    // Progress should show zero total since nothing was submitted
    DeferredIndexProgress progress = secondService.getProgress();
    assertEquals("No indexes should have been submitted", 0, progress.getTotal());

    // Index still exists
    assertIndexExists("Product", "Product_Name_1");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void assertIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private void assertIndexMissing(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should NOT exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .noneMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private DeferredIndexService createAndConfigureService(
      Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);

    SqlScriptExecutorProvider executorProvider = new SqlScriptExecutorProvider(connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, executorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    DeferredIndexReadinessCheck readinessCheck = new DeferredIndexReadinessCheckImpl(
        executor, config, connectionResources);
    DeferredIndexService service = new DeferredIndexServiceImpl(executor, config, readinessCheck);
    service.setUpgradeSteps(upgradeSteps);
    return service;
  }
}
