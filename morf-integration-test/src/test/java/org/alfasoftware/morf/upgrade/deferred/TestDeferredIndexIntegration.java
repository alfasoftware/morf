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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
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
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenChange;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRemove;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRename;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.ChangeDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RemoveDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RenameDeferredIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * End-to-end integration tests for the deferred index lifecycle using the
 * comments-based model. Exercises the full upgrade framework path: upgrade
 * step adds a deferred index (recorded in table comments), the executor
 * scans for unbuilt deferred indexes, and builds them.
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

  private final UpgradeConfigAndContext upgradeConfigAndContext = new UpgradeConfigAndContext();
  { upgradeConfigAndContext.setDeferredIndexCreationEnabled(true); }

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
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * After upgrade, the deferred index should appear in the schema as a
   * virtual index (isDeferred=true) but not yet physically built. After
   * the executor runs, the physical index should exist.
   */
  @Test
  public void testDeferredIndexLifecycle() {
    // when -- upgrade step defers an index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // then -- virtual index present
    assertDeferredIndexPending("Product", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * A deferred unique index should preserve the unique constraint
   * through the full pipeline.
   */
  @Test
  public void testDeferredUniqueIndex() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredUniqueIndex.class);

    // when
    executeDeferred();

    // then
    assertPhysicalIndexExists("Product", "Product_Name_UQ");
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index should be unique",
          sr.getTable("Product").indexes().stream()
              .filter(idx -> "Product_Name_UQ".equalsIgnoreCase(idx.getName()))
              .findFirst().get().isUnique());
    }
  }


  /**
   * A deferred multi-column index should preserve column ordering.
   */
  @Test
  public void testDeferredMultiColumnIndex() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );
    performUpgrade(targetSchema, AddDeferredMultiColumnIndex.class);

    // when
    executeDeferred();

    // then
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = sr.getTable("Product").indexes().stream()
          .filter(i -> "Product_IdName_1".equalsIgnoreCase(i.getName()))
          .findFirst().orElseThrow(() -> new AssertionError("Index not found"));
      assertTrue("First column should be id", idx.columnNames().get(0).equalsIgnoreCase("id"));
      assertTrue("Second column should be name", idx.columnNames().get(1).equalsIgnoreCase("name"));
    }
  }


  /**
   * Creating a new table and deferring an index on it in the same
   * upgrade step should work end-to-end.
   */
  @Test
  public void testNewTableWithDeferredIndex() {
    // given
    Schema targetSchema = schema(
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

    // when
    performUpgrade(targetSchema, AddTableWithDeferredIndex.class);

    // then -- deferred index present
    assertDeferredIndexPending("Category", "Category_Label_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built
    assertPhysicalIndexExists("Category", "Category_Label_1");
  }


  /**
   * Deferring an index on a table that already contains rows should
   * build the index correctly over existing data.
   */
  @Test
  public void testDeferredIndexOnPopulatedTable() {
    // given -- table with existing rows
    insertProductRow(1L, "Widget");
    insertProductRow(2L, "Gadget");
    insertProductRow(3L, "Doohickey");

    // when
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    executeDeferred();

    // then
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Deferring two indexes in a single upgrade step should queue both
   * and the executor should build them both.
   */
  @Test
  public void testMultipleIndexesDeferredInOneStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );

    // when
    performUpgrade(targetSchema, AddTwoDeferredIndexes.class);
    executeDeferred();

    // then
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
  }


  /**
   * Running the executor a second time on an already-built set of
   * indexes should be a safe no-op.
   */
  @Test
  public void testExecutorIdempotencyOnBuiltIndexes() {
    // given -- deferred index already built
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when -- second run
    executeDeferred();

    // then -- still exists, no error
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * When forceImmediateIndexes is configured for an index name, the
   * deferred index should be built immediately during the upgrade step
   * (not deferred to the executor).
   */
  @Test
  public void testForceImmediateIndexBypassesDeferral() {
    // given
    upgradeConfigAndContext.setForceImmediateIndexes(Set.of("Product_Name_1"));
    try {
      // when
      performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

      // then -- index built immediately, no executor needed
      assertPhysicalIndexExists("Product", "Product_Name_1");
    } finally {
      upgradeConfigAndContext.setForceImmediateIndexes(Set.of());
    }
  }


  /**
   * When forceDeferredIndexes is configured for an index name, addIndex()
   * should defer the index instead of building it immediately.
   */
  @Test
  public void testForceDeferredIndexOverridesImmediateCreation() {
    // given
    upgradeConfigAndContext.setForceDeferredIndexes(Set.of("Product_Name_1"));
    try {
      // when -- addIndex() with force-deferred override
      performUpgrade(schemaWithIndex(), AddImmediateIndex.class);

      // then -- index deferred, not built yet
      assertDeferredIndexPending("Product", "Product_Name_1");

      // when -- execute deferred
      executeDeferred();

      // then -- physical index built
      assertPhysicalIndexExists("Product", "Product_Name_1");
    } finally {
      upgradeConfigAndContext.setForceDeferredIndexes(Set.of());
    }
  }


  /**
   * When deferredIndexCreationEnabled is false (the default), a deferred
   * index should be built immediately during the upgrade step.
   */
  @Test
  public void testDisabledFeatureBuildsDeferredIndexImmediately() {
    // given -- kill switch disabled (default)
    UpgradeConfigAndContext disabledConfig = new UpgradeConfigAndContext();

    // when
    Upgrade.performUpgrade(schemaWithIndex(), Collections.singletonList(AddDeferredIndex.class),
        connectionResources, disabledConfig, viewDeploymentValidator);

    // then -- index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * When the dialect does not support deferred index creation,
   * the deferred index should be built immediately.
   */
  @Test
  public void testUnsupportedDialectFallsBackToImmediateIndex() {
    // given -- dialect that does not support deferred creation
    org.alfasoftware.morf.jdbc.SqlDialect realDialect = connectionResources.sqlDialect();
    org.alfasoftware.morf.jdbc.SqlDialect spyDialect = org.mockito.Mockito.spy(realDialect);
    org.mockito.Mockito.when(spyDialect.supportsDeferredIndexCreation()).thenReturn(false);
    ConnectionResources spyConn = org.mockito.Mockito.spy(connectionResources);
    org.mockito.Mockito.when(spyConn.sqlDialect()).thenReturn(spyDialect);

    // when
    Upgrade.performUpgrade(schemaWithIndex(), Collections.singletonList(AddDeferredIndex.class),
        spyConn, upgradeConfigAndContext, viewDeploymentValidator);

    // then -- index built immediately during upgrade
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Same-step operations: add deferred then modify in the same upgrade step
  // =========================================================================

  /**
   * Add a deferred index then remove it in the same step. After upgrade,
   * neither the physical index nor a deferred declaration should exist.
   */
  @Test
  public void testAddDeferredThenRemoveInSameStep() {
    // when -- add deferred then remove in same step
    performUpgrade(INITIAL_SCHEMA, AddDeferredIndexThenRemove.class);

    // then
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  /**
   * Add a deferred index then change it to a non-deferred index in the same
   * step. The new index should be built immediately during upgrade.
   */
  @Test
  public void testAddDeferredThenChangeInSameStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_2").columns("name"))
    );

    // when -- add deferred then change to non-deferred in same step
    performUpgrade(targetSchema, AddDeferredIndexThenChange.class);

    // then -- changed index built immediately, original gone
    assertPhysicalIndexExists("Product", "Product_Name_2");
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  /**
   * Add a deferred index then rename it in the same step. The renamed index
   * should still be deferred and buildable by the executor.
   */
  @Test
  public void testAddDeferredThenRenameInSameStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );

    // when -- add deferred then rename in same step
    performUpgrade(targetSchema, AddDeferredIndexThenRename.class);

    // then -- renamed index still deferred
    assertDeferredIndexPending("Product", "Product_Name_Renamed");
    assertIndexNotPresent("Product", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built with renamed name
    assertPhysicalIndexExists("Product", "Product_Name_Renamed");
  }


  // =========================================================================
  // Cross-step operations: deferred index NOT YET BUILT, modified in later step
  // ("change the plan" — no force-build needed, comment is simply updated)
  // =========================================================================

  /**
   * Step A defers an index. Before the executor runs, step B removes it.
   * The deferred index is never built — the comment is cleaned up.
   */
  @Test
  public void testRemoveUnbuiltDeferredIndexInLaterStep() {
    // given -- step A defers an index
    performUpgradeSteps(schemaWithIndex(), AddDeferredIndex.class);
    assertDeferredIndexPending("Product", "Product_Name_1");

    // when -- step B removes it before executor runs
    performUpgradeSteps(INITIAL_SCHEMA, AddDeferredIndex.class, RemoveDeferredIndex.class);

    // then -- index never built
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  /**
   * Step A defers an index. Before the executor runs, step B changes it
   * to a non-deferred multi-column index. The old deferred definition is
   * never built — the "plan" changes from comment-only to immediate CREATE INDEX.
   */
  @Test
  public void testChangeUnbuiltDeferredIndexInLaterStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("id", "name"))
    );

    // when -- step B changes the unbuilt deferred index to non-deferred
    performUpgradeSteps(targetSchema, AddDeferredIndex.class, ChangeDeferredIndex.class);

    // then -- changed index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Step A defers an index. Before the executor runs, step B renames it.
   * The renamed deferred index is built by the executor under the new name.
   */
  @Test
  public void testRenameUnbuiltDeferredIndexInLaterStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );

    // when -- step B renames the unbuilt deferred index
    performUpgradeSteps(targetSchema, AddDeferredIndex.class, RenameDeferredIndex.class);

    // then -- renamed deferred index present
    assertDeferredIndexPending("Product", "Product_Name_Renamed");
    assertIndexNotPresent("Product", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built with new name
    assertPhysicalIndexExists("Product", "Product_Name_Renamed");
  }


  // =========================================================================
  // Cross-step operations: deferred index ALREADY BUILT, modified in later step
  // =========================================================================

  /**
   * Step A defers an index. Executor builds it. Step B removes it.
   * The physical index should be dropped.
   */
  @Test
  public void testRemoveBuiltDeferredIndexInLaterStep() {
    // given -- deferred index already built
    performUpgradeSteps(schemaWithIndex(), AddDeferredIndex.class);
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when -- step B removes it
    performUpgradeSteps(INITIAL_SCHEMA, AddDeferredIndex.class, RemoveDeferredIndex.class);

    // then
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  /**
   * Step A defers an index. Executor builds it. Step B renames it.
   * The physical index should be renamed.
   */
  @Test
  public void testRenameBuiltDeferredIndexInLaterStep() {
    // given -- deferred index already built
    Schema renamedSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );
    performUpgradeSteps(schemaWithIndex(), AddDeferredIndex.class);
    executeDeferred();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when -- step B renames it
    performUpgradeSteps(renamedSchema, AddDeferredIndex.class, RenameDeferredIndex.class);

    // then
    assertPhysicalIndexExists("Product", "Product_Name_Renamed");
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  // =========================================================================
  // Cross-step: column and table modifications affecting deferred indexes
  // =========================================================================

  /**
   * Step A defers an index on column "name". Step B renames "name" to "label".
   * The deferred index comment should be updated with the new column name,
   * and the executor should build the index using the renamed column.
   */
  @Test
  public void testCrossStepColumnRenameUpdatesDeferredIndex() {
    // given -- target schema with column renamed from "name" to "label"
    Schema renamedColSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("label"))
    );

    // when -- step 1 defers index on "name", step 2 renames "name" to "label"
    performUpgradeSteps(renamedColSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RenameColumnWithDeferredIndex.class);

    // then -- deferred index references new column
    assertDeferredIndexPending("Product", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built using renamed column
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Step A defers an index on column "name". Step B removes the index and
   * column "name". The deferred index should be cleaned up — no ghost
   * entries in the comment, and the column removal should not trip over
   * stale deferred index declarations.
   */
  @Test
  public void testCrossStepColumnRemovalCleansDeferredIndex() {
    // given
    Schema noNameColSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey()
        )
    );

    // when -- step 1 defers index on "name", step 2 removes index and column
    performUpgradeSteps(noNameColSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RemoveColumnWithDeferredIndex.class);

    // then
    assertIndexNotPresent("Product", "Product_Name_1");
  }


  /**
   * Step A defers an index on table "Product". Step B renames table to "Item".
   * The deferred index comment should migrate to the new table and the executor
   * should build the index under the new table name.
   */
  @Test
  public void testCrossStepTableRenamePreservesDeferredIndex() {
    // given
    Schema renamedTableSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Item").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );

    // when -- step 1 defers index on "Product", step 2 renames table to "Item"
    performUpgradeSteps(renamedTableSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RenameTableWithDeferredIndex.class);

    // then -- deferred index migrates to new table
    assertDeferredIndexPending("Item", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- physical index built on renamed table
    assertPhysicalIndexExists("Item", "Product_Name_1");
  }


  // =========================================================================
  // Additional edge cases
  // =========================================================================

  /**
   * Adding a deferred index to a table that already has a non-deferred index
   * should preserve the existing index.
   */
  @Test
  public void testDeferredIndexOnTableWithExistingIndex() {
    // given -- table already has a non-deferred index
    Schema schemaWithExisting = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Id_1").columns("id", "name"))
    );
    schemaManager.mutateToSupportSchema(schemaWithExisting, DatabaseSchemaManager.TruncationBehavior.ALWAYS);
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Id_1").columns("id", "name"),
            index("Product_Name_1").columns("name")
        )
    );

    // when -- add deferred index to same table
    performUpgrade(targetSchema, AddDeferredIndex.class);

    // then -- existing index preserved, deferred index pending
    assertPhysicalIndexExists("Product", "Product_Id_1");
    assertDeferredIndexPending("Product", "Product_Name_1");

    // when -- execute deferred
    executeDeferred();

    // then -- both indexes present
    assertPhysicalIndexExists("Product", "Product_Id_1");
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * getMissingDeferredIndexStatements should return valid SQL for unbuilt deferred indexes.
   */
  @Test
  public void testGetMissingDeferredIndexStatements() {
    // given -- one unbuilt deferred index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());

    // when
    java.util.List<String> statements = executor.getMissingDeferredIndexStatements();

    // then
    assertFalse("Should return at least one statement", statements.isEmpty());
    assertTrue("Statement should reference the index name",
        statements.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
  }


  /**
   * Deferred indexes on multiple tables should all be found and built by the executor.
   */
  @Test
  public void testDeferredIndexesOnMultipleTables() {
    // given -- deferred indexes on two different tables
    Schema multiTableSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name")),
        table("Category").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 50)
        ).indexes(index("Category_Label_1").columns("label"))
    );

    // when
    performUpgradeSteps(multiTableSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTableWithDeferredIndex.class);

    // then -- both deferred
    assertDeferredIndexPending("Product", "Product_Name_1");
    assertDeferredIndexPending("Category", "Category_Label_1");

    // when -- execute deferred
    executeDeferred();

    // then -- both physical indexes built
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Category", "Category_Label_1");
  }


  /**
   * A deferred unique index on a table with duplicate data should fail gracefully.
   */
  @Test
  public void testDeferredUniqueIndexWithDuplicateDataFailsGracefully() {
    // given -- table with duplicate values in the indexed column
    insertProductRow(1L, "Widget");
    insertProductRow(2L, "Widget");
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(targetSchema, AddDeferredUniqueIndex.class);

    // when -- executor attempts to build (should not throw)
    executeDeferred();

    // then -- index not built, deferred declaration remains
    assertDeferredIndexPending("Product", "Product_Name_UQ");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> upgradeStep) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(upgradeStep),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  @SafeVarargs
  private void performUpgradeSteps(Schema targetSchema, Class<? extends UpgradeStep>... upgradeSteps) {
    List<Class<? extends UpgradeStep>> steps = Arrays.asList(upgradeSteps);
    Upgrade.performUpgrade(targetSchema, steps,
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  private void executeDeferred() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    executor.execute().join();
  }


  private Schema schemaWithIndex() {
    return schema(
        deployedViewsTable(),
        upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name")
        )
    );
  }


  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private void assertDeferredIndexPending(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Deferred index " + indexName + " should be present with isDeferred()=true on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName()) && idx.isDeferred()));
    }
  }


  private void assertIndexNotPresent(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("Index " + indexName + " should not be present on " + tableName,
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
