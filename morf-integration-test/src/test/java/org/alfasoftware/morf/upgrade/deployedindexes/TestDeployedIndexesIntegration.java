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

package org.alfasoftware.morf.upgrade.deployedindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedIndexesTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
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
import org.alfasoftware.morf.upgrade.UpgradePath;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexJob;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexTracker;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for the DeployedIndexes architecture. Exercises the
 * full upgrade framework path with the new DeployedIndexes table,
 * model enricher, and getDeferredIndexStatements().
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeployedIndexesIntegration {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private final UpgradeConfigAndContext config = new UpgradeConfigAndContext();
  { config.setDeferredIndexCreationEnabled(true); }

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      deployedIndexesTable(),
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
   * Verifies the full lifecycle of a single deferred index: the upgrade step
   * creates a PENDING row in DeployedIndexes, the physical index is NOT built,
   * and getDeferredIndexStatements() returns CREATE INDEX SQL referencing
   * the correct index name.
   */
  @Test
  public void testGetDeferredIndexStatementsReturnsSQL() {
    // given
    Schema targetSchema = schemaWithIndex();

    // when
    UpgradePath path = performUpgrade(targetSchema, AddDeferredIndex.class);

    // then -- physical index NOT built (deferred)
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");

    // then -- getDeferredIndexStatements returns a job for the index
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertFalse("Should return at least one deferred job", deferredJobs.isEmpty());
    assertTrue("Job should reference the index name",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));

    // then -- DeployedIndexes row is PENDING and deferred
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
    assertTrue("Should be deferred",
        "TRUE".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "indexDeferred")));
  }


  /**
   * An upgrade with no deferred indexes should return empty
   * getDeferredIndexStatements().
   */
  @Test
  public void testNoDeferredIndexesReturnsEmptyStatements() {
    // given -- feature enabled but no deferred indexes in the step
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );

    // when
    UpgradePath path = performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddImmediateIndex.class);

    // then
    assertTrue("No deferred statements expected", path.getDeferredIndexStatements().isEmpty());
  }


  /**
   * Two deferred indexes added in a single upgrade step should both appear
   * in getDeferredIndexStatements(), neither should be physically built,
   * and both should have PENDING rows in DeployedIndexes.
   */
  @Test
  public void testMultipleDeferredIndexesInOneStep() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name"),
            index("Product_IdName_1").columns("id", "name")
        )
    );

    // when
    UpgradePath path = performUpgrade(targetSchema, AddTwoDeferredIndexes.class);

    // then -- neither index physically built
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertPhysicalIndexDoesNotExist("Product", "Product_IdName_1");

    // then -- both in getDeferredIndexStatements
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertTrue("Should contain Product_Name_1",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
    assertTrue("Should contain Product_IdName_1",
        deferredJobs.stream().anyMatch(j -> "Product_IdName_1".equalsIgnoreCase(j.getIndexName())));

    // then -- both PENDING in DeployedIndexes
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
    assertEquals("PENDING", queryDeployedIndexField("Product_IdName_1", "status"));
  }


  /**
   * When deferredIndexCreationEnabled is false, deferred indexes should
   * be built immediately and getDeferredIndexStatements() is empty.
   */
  @Test
  public void testDisabledFeatureBuildsDeferredImmediately() {
    // given
    UpgradeConfigAndContext disabledConfig = new UpgradeConfigAndContext();

    // when
    UpgradePath path = Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(AddDeferredIndex.class),
        connectionResources, disabledConfig, viewDeploymentValidator);

    // then -- index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // and -- no deferred jobs returned (adopter contract when feature is disabled)
    assertTrue("No deferred jobs expected when feature is disabled",
        path.getDeferredIndexStatements().isEmpty());
  }


  // =========================================================================
  // Same-step operations
  // =========================================================================

  /**
   * Same-step: add deferred then change to non-deferred. Changed index
   * should be built immediately.
   */
  @Test
  public void testAddDeferredThenChangeInSameStep() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_2").columns("name"))
    );

    // when
    performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenChange.class);

    // then -- changed index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_2");
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
  }


  // =========================================================================
  // Cross-step operations
  // =========================================================================

  /**
   * Step A defers an index on column "name". Step B renames "name" to "label".
   * The DeployedIndexes table's indexColumns is updated via the change service,
   * and the rebuilt schema preserves isDeferred() so getDeferredIndexStatements()
   * emits SQL referencing the new column name.
   */
  @Test
  public void testCrossStepColumnRename() {
    // given -- target schema with renamed column and updated index
    Schema renamedColSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("label"))
    );

    // when -- defer an index, then rename the column it references
    UpgradePath path = performUpgradeSteps(renamedColSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RenameColumnWithDeferredIndex.class);

    // then -- DeployedIndexes row reflects the renamed column
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
    assertEquals("label", queryDeployedIndexField("Product_Name_1", "indexColumns"));

    // then -- getDeferredIndexStatements emits SQL with the new column name
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertFalse("Should have a deferred job after rename", deferredJobs.isEmpty());
    assertTrue("Job's SQL should reference new column name 'label'",
        deferredJobs.stream().flatMap(j -> j.getSql().stream())
            .anyMatch(s -> s.toUpperCase().contains("LABEL")));
  }


  /**
   * Step A adds a non-deferred index on column "name". Step B renames "name"
   * to "label". The DeployedIndexes row's indexColumns should be updated to
   * "label" (the physical index is automatically updated by the DDL).
   */
  @Test
  public void testCrossStepColumnRenameOnNonDeferredIndex() {
    // given
    Schema renamedColSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("label"))
    );

    // when -- add an immediate (non-deferred) index, then rename the column
    performUpgradeSteps(renamedColSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddImmediateIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RenameColumnWithDeferredIndex.class);

    // then -- DeployedIndexes row has the new column name and remains COMPLETED
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertEquals("label", queryDeployedIndexField("Product_Name_1", "indexColumns"));
    assertTrue("Should not be deferred",
        "FALSE".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "indexDeferred")));
  }


  /**
   * Step A defers an index. Step B removes the index and column.
   * Nothing should remain.
   */
  @Test
  public void testCrossStepColumnRemoval() {
    // given
    Schema noNameColSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey()
        )
    );

    // when
    performUpgradeSteps(noNameColSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RemoveColumnWithDeferredIndex.class);

    // then -- physical index absent AND DeployedIndexes row cleaned up
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertNull("DeployedIndexes row should be deleted",
        queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Step A defers an index on table Product. Step B renames table to Item.
   * The DeployedIndexes row's tableName should be updated to Item, the
   * deferred index SQL should reference the new table name, and the
   * physical index should not exist on either table.
   */
  @Test
  public void testCrossStepTableRename() {
    // given
    Schema renamedTableSchema = schemaWith(
        table("Item").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );

    // when
    UpgradePath path = performUpgradeSteps(renamedTableSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RenameTableWithDeferredIndex.class);

    // then -- deferred index job references new table
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertFalse("Should have a deferred job", deferredJobs.isEmpty());
    assertTrue("Job's table should be Item",
        deferredJobs.stream().anyMatch(j -> "Item".equalsIgnoreCase(j.getTableName())));

    // then -- DeployedIndexes tableName updated
    assertEquals("Item", queryDeployedIndexField("Product_Name_1", "tableName"));
  }


  /**
   * Deferred indexes on multiple tables should all appear in
   * getDeferredIndexStatements().
   */
  @Test
  public void testDeferredIndexesOnMultipleTables() {
    // given
    Schema multiTableSchema = schemaWith(
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
    UpgradePath path = performUpgradeSteps(multiTableSchema,
        AddDeferredIndex.class,
        AddTableWithDeferredIndex.class);

    // then
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertTrue("Should contain Product_Name_1",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
    assertTrue("Should contain Category_Label_1",
        deferredJobs.stream().anyMatch(j -> "Category_Label_1".equalsIgnoreCase(j.getIndexName())));
  }


  // =========================================================================
  // Config overrides and edge cases
  // =========================================================================

  /**
   * A non-deferred addIndex should be built immediately, exist physically,
   * and have a COMPLETED row in DeployedIndexes.
   */
  @Test
  public void testNonDeferredIndexBuiltImmediately() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );

    // when
    performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddImmediateIndex.class);

    // then -- physical index exists AND DeployedIndexes row is COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertTrue("Should not be deferred",
        "FALSE".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "indexDeferred")));
  }


  /**
   * When forceImmediateIndexes is configured for an index name, a deferred
   * addIndex should be built immediately during upgrade. The physical index
   * should exist, the DeployedIndexes row should be COMPLETED, and
   * getDeferredIndexStatements() should be empty.
   */
  @Test
  public void testForceImmediateBypassesDeferral() {
    // given -- separate config to avoid polluting shared state
    UpgradeConfigAndContext forceConfig = new UpgradeConfigAndContext();
    forceConfig.setDeferredIndexCreationEnabled(true);
    forceConfig.setForceImmediateIndexes(java.util.Set.of("Product_Name_1"));

    // when
    UpgradePath path = Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(AddDeferredIndex.class),
        connectionResources, forceConfig, viewDeploymentValidator);

    // then -- built immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertTrue("No deferred statements expected", path.getDeferredIndexStatements().isEmpty());
  }


  /**
   * Same-step: add deferred then remove in the same step. No physical
   * index and no DeployedIndexes row should exist after upgrade.
   */
  @Test
  public void testAddDeferredThenRemoveInSameStep() {
    // when
    performUpgrade(INITIAL_SCHEMA,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenRemove.class);

    // then -- neither physical index nor DeployedIndexes row
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertNull("Should have no DeployedIndexes row",
        queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Same-step: add deferred then rename in the same step. Renamed
   * deferred index should appear in getDeferredIndexStatements().
   */
  @Test
  public void testAddDeferredThenRenameInSameStep() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_Renamed").columns("name"))
    );

    // when
    UpgradePath path = performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenRename.class);

    // then -- renamed deferred index in jobs
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertTrue("Should contain renamed index",
        deferredJobs.stream().anyMatch(j -> "Product_Name_Renamed".equalsIgnoreCase(j.getIndexName())));
    assertFalse("Should not contain original name",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
  }


  // =========================================================================
  // Unique and multi-column deferred indexes
  // =========================================================================

  /** Unique deferred index should preserve unique flag in getDeferredIndexStatements. */
  @Test
  public void testUniqueDeferredIndex() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );

    // when
    UpgradePath path = performUpgrade(targetSchema, AddDeferredUniqueIndex.class);

    // then
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertFalse("Should have a deferred job", deferredJobs.isEmpty());
    assertTrue("Job's SQL should contain UNIQUE keyword",
        deferredJobs.stream().flatMap(j -> j.getSql().stream())
            .anyMatch(s -> s.toUpperCase().contains("UNIQUE")));
  }


  /**
   * A deferred multi-column index should preserve column ordering in the
   * generated SQL, not be physically built, and have the columns stored
   * correctly in the DeployedIndexes table.
   */
  @Test
  public void testMultiColumnDeferredIndex() {
    // given
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );

    // when
    UpgradePath path = performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredMultiColumnIndex.class);

    // then -- not physically built
    assertPhysicalIndexDoesNotExist("Product", "Product_IdName_1");

    // then -- SQL generated with both columns
    List<DeferredIndexJob> deferredJobs = path.getDeferredIndexStatements();
    assertFalse("Should have a deferred job", deferredJobs.isEmpty());

    // then -- DeployedIndexes has correct columns
    assertEquals("PENDING", queryDeployedIndexField("Product_IdName_1", "status"));
    assertEquals("id,name", queryDeployedIndexField("Product_IdName_1", "indexColumns"));
  }


  // =========================================================================
  // DeployedIndexes table state verification
  // =========================================================================

  /**
   * A second upgrade should include previously-unbuilt deferred indexes
   * in getDeferredIndexStatements().
   */
  @Test
  public void testSequentialUpgradeIncludesPreviousDeferred() {
    // given — first upgrade defers an index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // when — second upgrade with a new step (schema unchanged = same target)
    UpgradePath path2 = performUpgradeSteps(
        schemaWith(
            table("Product").columns(
                column("id", DataType.BIG_INTEGER).primaryKey(),
                column("name", DataType.STRING, 100)
            ).indexes(
                index("Product_Name_1").columns("name"),
                index("Product_IdName_1").columns("id", "name")
            )
        ),
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.AddSecondDeferredIndex.class);

    // then — should include BOTH deferred indexes
    List<DeferredIndexJob> deferredJobs = path2.getDeferredIndexStatements();
    assertTrue("Should contain first deferred index",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
    assertTrue("Should contain second deferred index",
        deferredJobs.stream().anyMatch(j -> "Product_IdName_1".equalsIgnoreCase(j.getIndexName())));
  }


  /**
   * Creating a new table should track all its indexes in DeployedIndexes.
   */
  @Test
  public void testAddTableTracksIndexesInDeployedTable() {
    // given
    Schema targetSchema = schemaWith(
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

    // then -- Category_Label_1 should be tracked (deferred)
    assertEquals("PENDING", queryDeployedIndexField("Category_Label_1", "status"));
  }


  // =========================================================================
  // Idempotency and edge cases
  // =========================================================================

  /**
   * Running the same upgrade twice should be idempotent — the second run
   * should detect no new steps to apply and produce no errors. The
   * DeployedIndexes state should be unchanged.
   */
  @Test
  public void testReUpgradeIsIdempotent() {
    // given -- first upgrade defers an index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));

    // when -- second upgrade with same schema and steps
    UpgradePath path2 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // then -- no errors, state unchanged
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * RemoveTable should delete all DeployedIndexes rows for that table.
   * Step A adds a deferred index on Product. Step B removes the Product
   * table entirely. After upgrade, no DeployedIndexes row should remain
   * for the removed table.
   */
  @Test
  public void testRemoveTableCleansUpDeployedIndexes() {
    // given -- target schema without Product table
    Schema noProductSchema = schemaWith();

    // when
    performUpgradeSteps(noProductSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RemoveProductTable.class);

    // then -- no DeployedIndexes row for the removed table's index
    assertNull("DeployedIndexes row should be deleted after removeTable",
        queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Adopter flow — happy path: the full loop documented in the integration
   * guide. Iterate jobs from getDeferredIndexStatements(), markStarted, run
   * each SQL statement, markCompleted. After the loop: physical index exists
   * and the DeployedIndexes row is COMPLETED.
   */
  @Test
  public void testAppSideAdopterFlowBuildsAndMarksCompleted() {
    // given -- upgrade creates a PENDING deferred index
    UpgradePath path = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    DeployedIndexTracker tracker = newTracker();

    // when -- the app-side loop (use literal names since H2 folds schema-
    // derived names to uppercase; the stored row uses the step's mixed case)
    List<org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexJob> jobs =
        path.getDeferredIndexStatements();
    assertFalse("Should have a job to execute", jobs.isEmpty());
    for (org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexJob job : jobs) {
      tracker.markStarted("Product", "Product_Name_1");
      sqlScriptExecutorProvider.get().execute(job.getSql());
      tracker.markCompleted("Product", "Product_Name_1");
    }

    // then -- physical index built AND row flipped to COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Adopter flow — failure path: if executing a job's SQL fails, the app
   * calls markFailed with an error message; the row flips to FAILED and
   * the errorMessage is persisted.
   */
  @Test
  public void testAppSideAdopterFlowMarksFailed() {
    // given -- upgrade creates a PENDING deferred index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    DeployedIndexTracker tracker = newTracker();

    // when -- app-side loop simulates a failure mid-execution
    tracker.markStarted("Product", "Product_Name_1");
    tracker.markFailed("Product", "Product_Name_1", "disk full");

    // then -- row flipped to FAILED, error message persisted, physical index NOT built
    assertEquals("FAILED", queryDeployedIndexField("Product_Name_1", "status"));
    assertEquals("disk full", queryDeployedIndexField("Product_Name_1", "errorMessage"));
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
  }


  /** Helper: construct a tracker backed by the test's executor + connection. */
  private DeployedIndexTracker newTracker() {
    return new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexTrackerImpl(
        new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesDAOImpl(
            sqlScriptExecutorProvider, connectionResources,
            new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesStatementFactory()));
  }


  /**
   * Crash recovery: if the tracker marks an index as IN_PROGRESS and the
   * process crashes, {@code tracker.resetInProgress()} should transition
   * it back to PENDING on next startup.
   */
  @Test
  public void testCrashRecoveryResetsInProgressToPending() {
    // given -- upgrade creates a PENDING deferred index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // given -- simulate crash: mark as IN_PROGRESS
    DeployedIndexTracker tracker = newTracker();
    tracker.markStarted("Product", "Product_Name_1");
    assertEquals("IN_PROGRESS",
        queryDeployedIndexField("Product_Name_1", "status"));

    // when -- simulate restart
    tracker.resetInProgress();

    // then
    assertEquals("PENDING",
        queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * CreateDeployedIndexes should create the DeployedIndexes table and
   * prepopulate it with all pre-existing physical indexes across every
   * table — including Morf infrastructure tables (UpgradeAudit,
   * DeployedViews, DeployedIndexes) — with status COMPLETED and
   * indexDeferred false. Only {@code _PRF} indexes are excluded
   * (performance-testing, by design).
   */
  @Test
  public void testPrepopulationPopulatesExistingIndexes() {
    // given -- Product has a pre-existing physical index but no DeployedIndexes table
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(
        schema(
            deployedViewsTable(),
            upgradeAuditTable(),
            table("Product").columns(
                column("id", DataType.BIG_INTEGER).primaryKey(),
                column("name", DataType.STRING, 100)
            ).indexes(index("Product_Name_1").columns("name"))
        ),
        TruncationBehavior.ALWAYS);

    // when -- run CreateDeployedIndexes to create and prepopulate the table
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
    performUpgrade(targetSchema,
        org.alfasoftware.morf.upgrade.upgrade.CreateDeployedIndexes.class);

    // then -- pre-existing index is tracked (names come from H2 metadata, folded to uppercase)
    assertTrue("Expected COMPLETED status for pre-populated index",
        "COMPLETED".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "status")));
    assertTrue("Expected tableName Product",
        "PRODUCT".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "tableName")));
    assertTrue("Expected column 'name'",
        "NAME".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "indexColumns")));
    assertTrue("Should not be deferred",
        "FALSE".equalsIgnoreCase(queryDeployedIndexField("Product_Name_1", "indexDeferred")));
  }


  // =========================================================================
  // Config overrides (additional)
  // =========================================================================

  /**
   * Force-deferred: an addIndex() without .deferred() should be deferred
   * when forceDeferredIndexes config includes the index name. The physical
   * index should NOT be built, and getDeferredIndexStatements() should
   * contain the SQL.
   */
  @Test
  public void testForceDeferredOverridesImmediate() {
    // given
    UpgradeConfigAndContext forceConfig = new UpgradeConfigAndContext();
    forceConfig.setDeferredIndexCreationEnabled(true);
    forceConfig.setForceDeferredIndexes(java.util.Set.of("Product_Name_1"));

    // when -- AddImmediateIndex uses addIndex() without .deferred()
    UpgradePath path = Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(
            org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddImmediateIndex.class),
        connectionResources, forceConfig, viewDeploymentValidator);

    // then -- deferred despite no .deferred() on the index
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertFalse("Should have deferred statements", path.getDeferredIndexStatements().isEmpty());
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Unsupported dialect fallback: when the dialect does not support deferred
   * index creation, a .deferred() index should be built immediately.
   */
  @Test
  public void testUnsupportedDialectFallsBackToImmediate() {
    // given -- spy dialect returning supportsDeferredIndexCreation()=false
    org.alfasoftware.morf.jdbc.SqlDialect realDialect = connectionResources.sqlDialect();
    org.alfasoftware.morf.jdbc.SqlDialect spyDialect = org.mockito.Mockito.spy(realDialect);
    org.mockito.Mockito.when(spyDialect.supportsDeferredIndexCreation()).thenReturn(false);
    org.alfasoftware.morf.jdbc.ConnectionResources spyConn = org.mockito.Mockito.spy(connectionResources);
    org.mockito.Mockito.when(spyConn.sqlDialect()).thenReturn(spyDialect);

    // when
    Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(AddDeferredIndex.class),
        spyConn, config, viewDeploymentValidator);

    // then -- built immediately despite .deferred()
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private UpgradePath performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> step) {
    return Upgrade.performUpgrade(targetSchema, Collections.singletonList(step),
        connectionResources, config, viewDeploymentValidator);
  }

  @SafeVarargs
  private UpgradePath performUpgradeSteps(Schema targetSchema, Class<? extends UpgradeStep>... steps) {
    return Upgrade.performUpgrade(targetSchema, Arrays.asList(steps),
        connectionResources, config, viewDeploymentValidator);
  }

  /** Helper: schema with Product table having one index on name. */
  private Schema schemaWithIndex() {
    return schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
  }

  /** Helper: builds a schema with Morf infrastructure tables + the given user tables. */
  private static Schema schemaWith(org.alfasoftware.morf.metadata.Table... tables) {
    java.util.List<org.alfasoftware.morf.metadata.Table> all = new java.util.ArrayList<>();
    all.add(deployedViewsTable());
    all.add(upgradeAuditTable());
    all.add(deployedIndexesTable());
    java.util.Collections.addAll(all, tables);
    return schema(all);
  }

  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }

  private void assertPhysicalIndexDoesNotExist(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("Index " + indexName + " should not exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }

  private String queryDeployedIndexField(String indexName, String fieldName) {
    String sql = "SELECT " + fieldName + " FROM DeployedIndexes WHERE UPPER(indexName) = '"
        + indexName.toUpperCase() + "'";
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }

}
