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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeferredIndexReadinessCheckImpl}.
 *
 * <p>Tests use replay-based discovery: inner static {@link UpgradeStep}
 * classes define tables and deferred indexes. The readiness check replays
 * these steps, discovers surviving deferred indexes, compares against
 * the live database, and builds any that are missing.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexReadinessCheck {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;

  private DeferredIndexExecutionConfig config;


  /**
   * Drop and recreate the required schema before each test.
   */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    config = new DeferredIndexExecutionConfig();
    config.setMaxRetries(0);
    config.setRetryBaseDelayMs(10L);
  }


  /**
   * Invalidate the schema manager cache after each test.
   */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * Replay finds no surviving deferred indexes when no upgrade steps use addIndexDeferred, so forceBuildAllPending is a no-op.
   */
  @Test
  public void testReplayFindsNoDeferredIndexesIsNoOp() {
    // Create a table with no deferred indexes
    Schema dbSchema = schema(
        table("Fruit").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50)
        )
    );
    schemaManager.mutateToSupportSchema(dbSchema, TruncationBehavior.ALWAYS);

    DeferredIndexReadinessCheck readinessCheck = createReadinessCheck();

    // Pass the step that only adds a table (no deferred index) — must not throw
    readinessCheck.forceBuildAllPending(stepClasses(CreateFruitTable.class));
  }


  /**
   * Replay finds a missing deferred index and forceBuildAllPending builds it in the database.
   */
  @Test
  public void testReplayFindsMissingIndexAndForceBuildsIt() {
    // Create the table in the DB but NOT the index
    Schema dbSchema = schema(
        table("Fruit").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50)
        )
    );
    schemaManager.mutateToSupportSchema(dbSchema, TruncationBehavior.ALWAYS);

    DeferredIndexReadinessCheck readinessCheck = createReadinessCheck();

    // Replay will find CreateFruitTable + AddDeferredFruitIndex, discover the index is missing, and build it
    readinessCheck.forceBuildAllPending(stepClasses(CreateFruitTable.class, AddDeferredFruitIndex.class));

    // Verify the index now exists in the database
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Fruit_Name_1 index should exist after force-build",
          sr.getTable("Fruit").indexes().stream()
              .anyMatch(idx -> "Fruit_Name_1".equalsIgnoreCase(idx.getName())));
    }
  }


  /**
   * Replay finds multiple missing deferred indexes and forceBuildAllPending builds all of them.
   */
  @Test
  public void testReplayFindsMultipleMissingIndexesAndForceBuildsAll() {
    // Create the table in the DB but none of the indexes
    Schema dbSchema = schema(
        table("Fruit").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 50)
        )
    );
    schemaManager.mutateToSupportSchema(dbSchema, TruncationBehavior.ALWAYS);

    DeferredIndexReadinessCheck readinessCheck = createReadinessCheck();

    // Replay will find both deferred indexes as missing and build them
    readinessCheck.forceBuildAllPending(
        stepClasses(CreateFruitTable.class, AddDeferredFruitIndex.class, AddSecondDeferredFruitIndex.class));

    // Verify both indexes now exist
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      List<String> indexNames = new java.util.ArrayList<>();
      for (Index idx : sr.getTable("Fruit").indexes()) {
        indexNames.add(idx.getName().toUpperCase());
      }
      assertTrue("Fruit_Name_1 index should exist", indexNames.contains("FRUIT_NAME_1"));
      assertTrue("Fruit_IdName_1 index should exist", indexNames.contains("FRUIT_IDNAME_1"));
    }
  }


  /**
   * Force-build fails when the deferred index targets a non-existent table and throws IllegalStateException.
   */
  @Test
  public void testForceBuildFailsForNonExistentTableTarget() {
    // Do NOT create any tables — the deferred index targets "Ghost" which does not exist
    Schema dbSchema = schema(
        table("Placeholder").columns(
            column("id", DataType.BIG_INTEGER).primaryKey()
        )
    );
    schemaManager.mutateToSupportSchema(dbSchema, TruncationBehavior.ALWAYS);

    DeferredIndexReadinessCheck readinessCheck = createReadinessCheck();

    try {
      readinessCheck.forceBuildAllPending(
          stepClasses(CreateGhostTableAndDeferIndex.class));
      fail("Expected IllegalStateException for failed forced execution");
    } catch (IllegalStateException e) {
      assertTrue("exception message should mention failed count",
          e.getMessage().contains("1 index operation(s) could not be built"));
    }
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private DeferredIndexReadinessCheck createReadinessCheck() {
    SqlScriptExecutorProvider executorProvider = new SqlScriptExecutorProvider(connectionResources);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, executorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    return new DeferredIndexReadinessCheckImpl(executor, config, connectionResources);
  }


  @SafeVarargs
  private static Collection<Class<? extends UpgradeStep>> stepClasses(Class<? extends UpgradeStep>... classes) {
    return Collections.unmodifiableList(Arrays.asList(classes));
  }


  // -------------------------------------------------------------------------
  // Inner upgrade step classes for replay-based discovery
  // -------------------------------------------------------------------------

  /**
   * Creates the Fruit table with id and name columns.
   */
  @Sequence(80001)
  @UUID("f0f00001-0001-0001-0001-000000000001")
  public static class CreateFruitTable implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "READINESS-001";
    }

    @Override
    public String getDescription() {
      return "Create Fruit table";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(table("Fruit").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 50)
      ));
    }
  }


  /**
   * Adds a deferred index on Fruit.name.
   */
  @Sequence(80002)
  @UUID("f0f00001-0001-0001-0001-000000000002")
  public static class AddDeferredFruitIndex implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "READINESS-002";
    }

    @Override
    public String getDescription() {
      return "Add deferred index on Fruit.name";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Fruit", index("Fruit_Name_1").columns("name"));
    }
  }


  /**
   * Adds a second deferred index on Fruit(id, name).
   */
  @Sequence(80003)
  @UUID("f0f00001-0001-0001-0001-000000000003")
  public static class AddSecondDeferredFruitIndex implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "READINESS-003";
    }

    @Override
    public String getDescription() {
      return "Add deferred index on Fruit(id, name)";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Fruit", index("Fruit_IdName_1").columns("id", "name"));
    }
  }


  /**
   * Creates a Ghost table and defers an index on it. Used to test force-build
   * failure when the table does not actually exist in the database.
   */
  @Sequence(80004)
  @UUID("f0f00001-0001-0001-0001-000000000004")
  public static class CreateGhostTableAndDeferIndex implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "READINESS-004";
    }

    @Override
    public String getDescription() {
      return "Create Ghost table with deferred index";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(table("Ghost").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("col", DataType.STRING, 30)
      ));
      schema.addIndexDeferred("Ghost", index("Ghost_Col_1").columns("col"));
    }
  }
}
