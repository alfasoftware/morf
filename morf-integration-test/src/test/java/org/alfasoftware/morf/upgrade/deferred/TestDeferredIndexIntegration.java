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
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
import org.alfasoftware.morf.upgrade.SchemaChangeSequence;
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
 * End-to-end integration tests for the deferred index mechanism. Exercises the
 * full {@link Upgrade#performUpgrade} pipeline, replay-based discovery via
 * {@link SchemaChangeSequence#findSurvivingDeferredIndexes()}, and the executor.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexIntegration {

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
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // =========================================================================
  // Inner upgrade step classes
  // =========================================================================

  /** Adds a deferred index on Product.name. */
  @Sequence(90001)
  @UUID("d1f00001-0001-0001-0001-000000000001")
  @Version("1.0.0")
  public static class StepAddDeferredIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred index"; }
  }


  /** Adds a deferred index then removes it in the same step. */
  @Sequence(90002)
  @UUID("d1f00001-0001-0001-0001-000000000002")
  @Version("1.0.0")
  public static class StepAddDeferredThenRemove implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
      schema.removeIndex("Product", index("Product_Name_1").columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred then remove"; }
  }


  /** Defers an index then immediately changes it. */
  @Sequence(90009)
  @UUID("d1f00001-0001-0001-0001-000000000009")
  @Version("1.0.0")
  public static class StepAddDeferredThenChange implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
      schema.changeIndex("Product", index("Product_Name_1").columns("name"),
          index("Product_Name_2").columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred then change"; }
  }


  /** Defers an index then immediately renames it. */
  @Sequence(90010)
  @UUID("d1f00001-0001-0001-0001-000000000010")
  @Version("1.0.0")
  public static class StepAddDeferredThenRename implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
      schema.renameIndex("Product", "Product_Name_1", "Product_Name_Renamed");
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred then rename"; }
  }


  /** Defers index on description, renames column, then removes index. */
  @Sequence(90011)
  @UUID("d1f00001-0001-0001-0001-000000000011")
  @Version("1.0.0")
  public static class StepAddDeferredThenRenameColumnThenRemove implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addColumn("Product", column("description", DataType.STRING, 200).nullable());
      schema.addIndexDeferred("Product", index("Product_Desc_1").columns("description"));
      schema.changeColumn("Product",
          column("description", DataType.STRING, 200).nullable(),
          column("summary", DataType.STRING, 200).nullable());
      schema.removeIndex("Product", index("Product_Desc_1").columns("summary"));
      schema.removeColumn("Product", column("summary", DataType.STRING, 200).nullable());
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred, rename col, remove"; }
  }


  /** Adds a deferred unique index on Product.name. */
  @Sequence(90005)
  @UUID("d1f00001-0001-0001-0001-000000000005")
  @Version("1.0.0")
  public static class StepAddDeferredUniqueIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_UQ").unique().columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred unique index"; }
  }


  /** Adds a deferred multi-column index on Product(id, name). */
  @Sequence(90006)
  @UUID("d1f00001-0001-0001-0001-000000000006")
  @Version("1.0.0")
  public static class StepAddDeferredMultiColumnIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_IdName_1").columns("id", "name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add deferred multi-column index"; }
  }


  /** Creates a new table and immediately defers an index on it. */
  @Sequence(90007)
  @UUID("d1f00001-0001-0001-0001-000000000007")
  @Version("1.0.0")
  public static class StepAddTableWithDeferredIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(table("Category").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("label", DataType.STRING, 50)
      ));
      schema.addIndexDeferred("Category", index("Category_Label_1").columns("label"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add table with deferred index"; }
  }


  /** Defers two indexes on Product in a single upgrade step. */
  @Sequence(90008)
  @UUID("d1f00001-0001-0001-0001-000000000008")
  @Version("1.0.0")
  public static class StepAddTwoDeferredIndexes implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndexDeferred("Product", index("Product_Name_1").columns("name"));
      schema.addIndexDeferred("Product", index("Product_IdName_1").columns("id", "name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add two deferred indexes"; }
  }


  /** Adds an immediate (non-deferred) index on Product.name. */
  @Sequence(90012)
  @UUID("d1f00001-0001-0001-0001-000000000012")
  @Version("1.0.0")
  public static class StepAddImmediateIndex implements UpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndex("Product", index("Product_Name_1").columns("name"));
    }

    @Override public String getJiraId() { return "DEFERRED-000"; }
    @Override public String getDescription() { return "Add immediate index"; }
  }


  // =========================================================================
  // Tests: deferred index NOT built during upgrade
  // =========================================================================

  /** Deferred index is not built in the DB immediately after upgrade. */
  @Test
  public void testDeferredAddDoesNotBuildIndexImmediately() {
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddDeferredIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");
  }


  /** Replay discovers the deferred index, executor builds it. */
  @Test
  public void testExecutorCompletesAndIndexExistsInSchema() {
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddDeferredIndex.class);
    assertIndexDoesNotExist("Product", "Product_Name_1");

    List<DeferredAddIndex> missing = findMissing(StepAddDeferredIndex.class);
    assertEquals("Should find 1 missing deferred index", 1, missing.size());

    executeDeferred(missing);
    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Tests: cascade resolution via replay
  // =========================================================================

  /** addIndexDeferred followed by removeIndex leaves no surviving deferred indexes. */
  @Test
  public void testAutoCancelDeferredAddFollowedByRemove() {
    performUpgrade(INITIAL_SCHEMA, StepAddDeferredThenRemove.class);
    List<DeferredAddIndex> surviving = replaySurviving(StepAddDeferredThenRemove.class);
    assertEquals("No surviving deferred indexes after add+remove", 0, surviving.size());
    assertIndexDoesNotExist("Product", "Product_Name_1");
  }


  /** addIndexDeferred followed by changeIndex cascades correctly: old cancelled, new deferred. */
  @Test
  public void testDeferredAddFollowedByChangeIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_2").columns("name"))
    );
    performUpgrade(target, StepAddDeferredThenChange.class);

    List<DeferredAddIndex> surviving = replaySurviving(StepAddDeferredThenChange.class);
    assertEquals("One surviving deferred index after change", 1, surviving.size());
    assertEquals("Product_Name_2", surviving.get(0).getNewIndex().getName());
    assertIndexDoesNotExist("Product", "Product_Name_1");
    // Product_Name_2 is deferred — not built yet
    assertIndexDoesNotExist("Product", "Product_Name_2");

    executeDeferred(findMissing(StepAddDeferredThenChange.class));
    assertIndexExists("Product", "Product_Name_2");
  }


  /** addIndexDeferred followed by renameIndex yields the renamed index via replay. */
  @Test
  public void testDeferredAddFollowedByRenameIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );
    performUpgrade(target, StepAddDeferredThenRename.class);

    List<DeferredAddIndex> surviving = replaySurviving(StepAddDeferredThenRename.class);
    assertEquals(1, surviving.size());
    assertEquals("Product_Name_Renamed", surviving.get(0).getNewIndex().getName());

    executeDeferred(findMissing(StepAddDeferredThenRename.class));
    assertIndexExists("Product", "Product_Name_Renamed");
  }


  /** Cascade through column rename then remove cancels the deferred index. */
  @Test
  public void testDeferredAddFollowedByRenameColumnThenRemove() {
    performUpgrade(INITIAL_SCHEMA, StepAddDeferredThenRenameColumnThenRemove.class);

    List<DeferredAddIndex> surviving = replaySurviving(StepAddDeferredThenRenameColumnThenRemove.class);
    assertEquals("Deferred operation should be cancelled", 0, surviving.size());
  }


  // =========================================================================
  // Tests: unique, multi-column, new table, populated table, multiple indexes
  // =========================================================================

  /** Deferred unique index preserves the unique constraint through the full pipeline. */
  @Test
  public void testDeferredUniqueIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(target, StepAddDeferredUniqueIndex.class);

    List<DeferredAddIndex> missing = findMissing(StepAddDeferredUniqueIndex.class);
    assertEquals(1, missing.size());
    assertTrue("Index should be unique", missing.get(0).getNewIndex().isUnique());

    executeDeferred(missing);
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index should be unique in database",
          sr.getTable("Product").indexes().stream()
              .filter(idx -> "Product_Name_UQ".equalsIgnoreCase(idx.getName()))
              .findFirst().orElseThrow().isUnique());
    }
  }


  /** Deferred multi-column index preserves column ordering. */
  @Test
  public void testDeferredMultiColumnIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );
    performUpgrade(target, StepAddDeferredMultiColumnIndex.class);

    executeDeferred(findMissing(StepAddDeferredMultiColumnIndex.class));
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = sr.getTable("Product").indexes().stream()
          .filter(i -> "Product_IdName_1".equalsIgnoreCase(i.getName()))
          .findFirst().orElseThrow();
      assertEquals("id", idx.columnNames().get(0).toLowerCase());
      assertEquals("name", idx.columnNames().get(1).toLowerCase());
    }
  }


  /** Creating a new table and deferring an index on it in the same step. */
  @Test
  public void testNewTableWithDeferredIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ),
        table("Category").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 50)
        ).indexes(index("Category_Label_1").columns("label"))
    );
    performUpgrade(target, StepAddTableWithDeferredIndex.class);

    assertIndexDoesNotExist("Category", "Category_Label_1");

    executeDeferred(findMissing(StepAddTableWithDeferredIndex.class));
    assertIndexExists("Category", "Category_Label_1");
  }


  /** Deferring an index on a table with existing data builds correctly. */
  @Test
  public void testDeferredIndexOnPopulatedTable() {
    insertProductRow(1L, "Widget");
    insertProductRow(2L, "Gadget");

    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddDeferredIndex.class);
    executeDeferred(findMissing(StepAddDeferredIndex.class));
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Two deferred indexes in one step are both discovered and built. */
  @Test
  public void testMultipleIndexesDeferredInOneStep() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );
    performUpgrade(target, StepAddTwoDeferredIndexes.class);

    List<DeferredAddIndex> missing = findMissing(StepAddTwoDeferredIndexes.class);
    assertEquals(2, missing.size());

    executeDeferred(missing);
    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  /** Executing twice is idempotent: second run finds indexes already built. */
  @Test
  public void testExecutorIdempotencyOnCompletedQueue() {
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddDeferredIndex.class);

    List<DeferredAddIndex> missing1 = findMissing(StepAddDeferredIndex.class);
    assertEquals(1, missing1.size());
    executeDeferred(missing1);
    assertIndexExists("Product", "Product_Name_1");

    List<DeferredAddIndex> missing2 = findMissing(StepAddDeferredIndex.class);
    assertEquals("Second run finds 0 missing", 0, missing2.size());
  }


  // =========================================================================
  // Tests: forceImmediateIndexes / forceDeferredIndexes / dialect fallback
  // =========================================================================

  /** forceImmediateIndexes builds the index during upgrade, no deferral. */
  @Test
  public void testForceImmediateIndexBypassesDeferral() {
    upgradeConfigAndContext.setForceImmediateIndexes(Set.of("Product_Name_1"));
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddDeferredIndex.class);

    assertIndexExists("Product", "Product_Name_1");

    List<DeferredAddIndex> missing = findMissing(StepAddDeferredIndex.class);
    assertEquals("No missing indexes after force-immediate", 0, missing.size());
  }


  /** forceDeferredIndexes overrides an immediate addIndex to defer it. */
  @Test
  public void testForceDeferredIndexOverridesImmediateCreation() {
    upgradeConfigAndContext.setForceDeferredIndexes(Set.of("Product_Name_1"));
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), StepAddImmediateIndex.class);

    assertIndexDoesNotExist("Product", "Product_Name_1");

    // findMissing must replay with the same config to see the force-deferred
    List<DeferredAddIndex> missing = findMissingWithConfig(upgradeConfigAndContext, StepAddImmediateIndex.class);
    assertEquals(1, missing.size());

    executeDeferred(missing);
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Unsupported dialect falls back to immediate index creation. */
  @Test
  public void testUnsupportedDialectFallsBackToImmediateIndex() {
    org.alfasoftware.morf.jdbc.SqlDialect realDialect = connectionResources.sqlDialect();
    org.alfasoftware.morf.jdbc.SqlDialect spyDialect = org.mockito.Mockito.spy(realDialect);
    org.mockito.Mockito.when(spyDialect.supportsDeferredIndexCreation()).thenReturn(false);

    ConnectionResources spyConn = org.mockito.Mockito.spy(connectionResources);
    org.mockito.Mockito.when(spyConn.sqlDialect()).thenReturn(spyDialect);

    Upgrade.performUpgrade(schemaWithIndex("Product_Name_1", "name"),
        Collections.singletonList(StepAddDeferredIndex.class),
        spyConn, new UpgradeConfigAndContext(), viewDeploymentValidator);

    assertIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Helpers
  // =========================================================================

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> step) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(step),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  private Schema schemaWithIndex(String indexName, String... columns) {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index(indexName).columns(columns))
    );
  }


  @SuppressWarnings("unchecked")
  private List<DeferredAddIndex> replaySurviving(Class<? extends UpgradeStep>... stepClasses) {
    List<UpgradeStep> instances = new ArrayList<>();
    for (Class<? extends UpgradeStep> cls : stepClasses) {
      try {
        instances.add(cls.getDeclaredConstructor().newInstance());
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }
    return new SchemaChangeSequence(instances).findSurvivingDeferredIndexes();
  }


  @SuppressWarnings("unchecked")
  private List<DeferredAddIndex> findMissing(Class<? extends UpgradeStep>... stepClasses) {
    DeferredIndexReadinessCheck rc = DeferredIndexReadinessCheck.create(connectionResources);
    return rc.findMissingDeferredIndexes(List.of(stepClasses));
  }


  @SuppressWarnings("unchecked")
  private List<DeferredAddIndex> findMissingWithConfig(UpgradeConfigAndContext config,
                                                        Class<? extends UpgradeStep>... stepClasses) {
    List<UpgradeStep> instances = new ArrayList<>();
    for (Class<? extends UpgradeStep> cls : stepClasses) {
      try {
        instances.add(cls.getDeclaredConstructor().newInstance());
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }
    List<DeferredAddIndex> surviving = new SchemaChangeSequence(config, instances)
        .findSurvivingDeferredIndexes();
    List<DeferredAddIndex> missing = new ArrayList<>();
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      for (DeferredAddIndex dai : surviving) {
        if (!sr.tableExists(dai.getTableName())) {
          missing.add(dai);
          continue;
        }
        boolean exists = sr.getTable(dai.getTableName()).indexes().stream()
            .anyMatch(idx -> idx.getName().equalsIgnoreCase(dai.getNewIndex().getName()));
        if (!exists) {
          missing.add(dai);
        }
      }
    }
    return missing;
  }


  private void executeDeferred(List<DeferredAddIndex> missing) {
    DeferredIndexExecutionConfig config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L);
    config.setMaxRetries(1);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, new SqlScriptExecutorProvider(connectionResources),
        config, new DeferredIndexExecutorServiceFactory.Default());
    executor.execute(missing).join();
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


  private void insertProductRow(long id, String name) {
    sqlScriptExecutorProvider.get().execute(
        connectionResources.sqlDialect().convertStatementToSQL(
            insert().into(tableRef("Product"))
                .values(literal(id).as("id"), literal(name).as("name"))
        )
    );
  }
}
