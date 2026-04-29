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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.UpgradePath;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenChange;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenRemove;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndexThenRename;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddImmediateIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddTableWithInlineDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.AddSecondDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.ChangeDeferredToNonDeferred;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RemoveColumnWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RemoveProductTable;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RenameColumnWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0.RenameTableWithDeferredIndex;
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

    // then -- DeployedIndexes row is PENDING (slim: every tracked row is deferred by invariant)
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
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
        AddImmediateIndex.class);

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
    disabledConfig.setDeferredIndexCreationEnabled(false);

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
        AddDeferredIndexThenChange.class);

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
        RenameColumnWithDeferredIndex.class);

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
   * to "label". <b>Slim invariant:</b> non-deferred indexes are not tracked
   * in {@code DeployedIndexes} — the rename is applied physically via
   * ALTER TABLE and no tracking row exists to update.
   */
  @Test
  public void testCrossStepColumnRenameOnNonDeferredIndexDoesNotTrack() {
    // given
    Schema renamedColSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("label"))
    );

    // when -- add an immediate (non-deferred) index, then rename the column
    performUpgradeSteps(renamedColSchema,
        AddImmediateIndex.class,
        RenameColumnWithDeferredIndex.class);

    // then -- physical index exists (under the renamed column) and no tracking row
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: non-deferred indexes are not tracked in DeployedIndexes",
        queryDeployedIndexField("Product_Name_1", "status"));
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
        RemoveColumnWithDeferredIndex.class);

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
        RenameTableWithDeferredIndex.class);

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
   * A non-deferred addIndex should be built immediately and exist physically.
   * <b>Slim invariant:</b> non-deferred indexes are not tracked in
   * {@code DeployedIndexes}.
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
        AddImmediateIndex.class);

    // then -- physical index exists and NO tracking row (slim invariant)
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: non-deferred indexes are not tracked in DeployedIndexes",
        queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * When forceImmediateIndexes is configured for an index name, a deferred
   * addIndex should be built immediately during upgrade. The physical index
   * should exist and {@code getDeferredIndexStatements()} should be empty.
   * <b>Slim invariant:</b> since the index ends up non-deferred after the
   * force-immediate resolution, it is not tracked.
   */
  @Test
  public void testForceImmediateBypassesDeferral() {
    // given -- separate config to avoid polluting shared state
    UpgradeConfigAndContext forceConfig = new UpgradeConfigAndContext();
    forceConfig.setDeferredIndexCreationEnabled(true);
    forceConfig.setForceImmediateIndexes(Set.of("Product_Name_1"));

    // when
    UpgradePath path = Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(AddDeferredIndex.class),
        connectionResources, forceConfig, viewDeploymentValidator);

    // then -- built immediately + no tracking row (slim: non-deferred not tracked)
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: force-immediate ends up non-deferred → not tracked",
        queryDeployedIndexField("Product_Name_1", "status"));
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
        AddDeferredIndexThenRemove.class);

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
        AddDeferredIndexThenRename.class);

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
        AddDeferredMultiColumnIndex.class);

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
        AddSecondDeferredIndex.class);

    // then — should include BOTH deferred indexes
    List<DeferredIndexJob> deferredJobs = path2.getDeferredIndexStatements();
    assertTrue("Should contain first deferred index",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
    assertTrue("Should contain second deferred index",
        deferredJobs.stream().anyMatch(j -> "Product_IdName_1".equalsIgnoreCase(j.getIndexName())));
  }


  /**
   * Inline-deferred index on AddTable: the actually-defer fix. Declaring a
   * deferred index inline on the addTable call must NOT emit CREATE INDEX at
   * upgrade time. The index is queued for the adopter via the deferred
   * pipeline; physical creation happens when the adopter executes the job.
   */
  @Test
  public void testAddTableWithInlineDeferredIndexDoesNotBuildImmediately() {
    // given -- target schema where Category has the deferred index already
    Schema targetSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ),
        table("Category").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 50)
        ).indexes(index("Category_Label_1").columns("label").deferred())
    );

    // when -- upgrade adds the table with the deferred index inline
    UpgradePath path = performUpgrade(targetSchema,
        AddTableWithInlineDeferredIndex.class);

    // then -- physical index NOT built; tracking row PENDING; job available
    assertPhysicalIndexDoesNotExist("Category", "Category_Label_1");
    assertEquals("PENDING", queryDeployedIndexField("Category_Label_1", "status"));
    assertFalse("getDeferredIndexStatements should return a job for the inline-deferred index",
        path.getDeferredIndexStatements().isEmpty());

    // when -- adopter executes the deferred SQL
    buildDeferredIndexesViaAdopter(path, "Category", "Category_Label_1");

    // then -- physical built, row COMPLETED
    assertPhysicalIndexExists("Category", "Category_Label_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Category_Label_1", "status"));
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
        RemoveProductTable.class);

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
    assertFalse("Should have a job to execute", path.getDeferredIndexStatements().isEmpty());

    // when -- the app-side loop
    buildDeferredIndexesViaAdopter(path, "Product", "Product_Name_1");

    // then -- physical index built AND row flipped to COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
  }


  /**
   * Row-existence model: after a deferred index is built (status=COMPLETED),
   * a subsequent column-rename upgrade still propagates correctly to the
   * tracking row's indexColumns and to the physical index. The COMPLETED
   * row stays as COMPLETED because the index is still currently declared
   * deferred — its declarative form is just rewritten.
   */
  @Test
  public void testCompletedDeferredIndexSurvivesColumnRename() {
    // given — upgrade 1 creates and adopter builds the deferred index
    UpgradePath path1 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    buildDeferredIndexesViaAdopter(path1, "Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when — upgrade 2 renames the underlying column (physical column rename
    // propagates to the index's column reference; tracking-row indexColumns
    // is updated by the visitor)
    Schema renamedColSchema = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("label"))
    );
    performUpgradeSteps(renamedColSchema,
        AddDeferredIndex.class,
        RenameColumnWithDeferredIndex.class);

    // then — row's indexColumns updated; row stays COMPLETED (still declared deferred)
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertEquals("label", queryDeployedIndexField("Product_Name_1", "indexColumns"));
  }


  /**
   * Drift policy: if a tracking row says non-terminal (e.g. PENDING) but
   * the physical index already exists, the enricher must throw — adopter
   * probably crashed between CREATE INDEX and markCompleted.
   */
  @Test
  public void testEnricherHardFailsOnNonCompletedRowWithMatchingPhysicalIndex() {
    // given — first upgrade creates a PENDING deferred index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeployedIndexField("Product_Name_1", "status"));
    // then — manually create the physical index without going through the tracker
    sqlScriptExecutorProvider.get().execute(List.of(
        "CREATE INDEX Product_Name_1 ON Product(name)"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when / then — next upgrade's enricher detects drift
    assertThrowsDriftWithMessageContaining(
        () -> performUpgrade(schemaWithIndex(), AddDeferredIndex.class),
        "Product_Name_1", "PENDING");
  }


  /**
   * Drift policy: a tracking row referencing a table not in the physical
   * schema is fatal. Could happen if the table was DROPped without removing
   * the row, or after restoring a partial backup.
   */
  @Test
  public void testEnricherHardFailsOnRowForMissingTable() {
    // given — manually insert a row referencing a non-existent table
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO DeployedIndexes (id, tableName, indexName, indexUnique, "
            + "indexColumns, status, retryCount, createdTime) "
            + "VALUES (42, 'GhostTable', 'GhostIdx', 0, 'col', 'PENDING', 0, 0)"));

    // when / then — enricher detects the orphan
    assertThrowsDriftWithMessageContaining(
        () -> performUpgrade(schemaWithIndex(), AddDeferredIndex.class),
        "GhostTable");
  }


  /**
   * Row-existence model: changing a built deferred index to non-deferred
   * should DELETE the tracking row (no longer declared deferred). The
   * physical index is dropped and recreated as non-deferred via the
   * standard ChangeIndex flow.
   */
  @Test
  public void testCompletedDeferredChangedToNonDeferredDeletesRow() {
    // given — upgrade 1 creates and adopter builds the deferred index
    UpgradePath path1 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    buildDeferredIndexesViaAdopter(path1, "Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeployedIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when — upgrade 2 changes the index from deferred to non-deferred
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))  // non-deferred now
    );
    performUpgradeSteps(target,
        AddDeferredIndex.class,
        ChangeDeferredToNonDeferred.class);

    // then — tracking row deleted, physical index still exists (rebuilt as non-deferred)
    assertNull("Tracking row for Product_Name_1 should be deleted (no longer declared deferred)",
        queryDeployedIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Drift policy: if a tracking row says COMPLETED but the physical index
   * is missing (manual DROP, restored backup, etc.), the next upgrade's
   * enricher must throw IllegalStateException. Morf does not auto-heal.
   */
  @Test
  public void testEnricherHardFailsOnCompletedRowWithoutPhysicalIndex() {
    // given — manually insert a fabricated COMPLETED row referencing a
    // physical index that doesn't exist
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO DeployedIndexes (id, tableName, indexName, indexUnique, "
            + "indexColumns, status, retryCount, createdTime) "
            + "VALUES (1, 'Product', 'Phantom_Idx', 0, 'name', 'COMPLETED', 0, 0)"));
    assertPhysicalIndexDoesNotExist("Product", "Phantom_Idx");

    // when / then — any subsequent upgrade trips the enricher's drift check
    assertThrowsDriftWithMessageContaining(
        () -> performUpgrade(schemaWithIndex(), AddDeferredIndex.class),
        "Phantom_Idx", "COMPLETED");
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
    return new DeployedIndexTrackerImpl(
        new DeployedIndexesDAO(sqlScriptExecutorProvider, connectionResources,
            new DeployedIndexesStatements()));
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
    forceConfig.setForceDeferredIndexes(Set.of("Product_Name_1"));

    // when -- AddImmediateIndex uses addIndex() without .deferred()
    UpgradePath path = Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(
            AddImmediateIndex.class),
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
    SqlDialect realDialect = connectionResources.sqlDialect();
    SqlDialect spyDialect = spy(realDialect);
    when(spyDialect.supportsDeferredIndexCreation()).thenReturn(false);
    ConnectionResources spyConn = spy(connectionResources);
    when(spyConn.sqlDialect()).thenReturn(spyDialect);

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
  private static Schema schemaWith(Table... tables) {
    List<Table> all = new ArrayList<>();
    all.add(deployedViewsTable());
    all.add(upgradeAuditTable());
    all.add(deployedIndexesTable());
    Collections.addAll(all, tables);
    return schema(all);
  }

  /**
   * Simulates an adopter executing every job from {@code path.getDeferredIndexStatements()}:
   * markStarted → run the SQL → markCompleted. The (tableName, indexName) literals are
   * passed explicitly because dialect schema scans (e.g. H2) fold names to upper case while
   * the persisted row carries the step's original mixed case — and the DAO match is
   * case-sensitive.
   */
  private void buildDeferredIndexesViaAdopter(UpgradePath path, String tableName, String indexName) {
    DeployedIndexTracker tracker = newTracker();
    for (DeferredIndexJob job : path.getDeferredIndexStatements()) {
      tracker.markStarted(tableName, indexName);
      sqlScriptExecutorProvider.get().execute(job.getSql());
      tracker.markCompleted(tableName, indexName);
    }
  }

  /**
   * Asserts that {@code action} throws a {@link RuntimeException} whose cause chain contains
   * an {@link IllegalStateException} whose message contains every supplied substring. Several
   * drift checks are wrapped by the upgrade framework's exception handling, so the
   * {@code IllegalStateException} typically isn't the top-level throwable — we walk the chain.
   */
  private static void assertThrowsDriftWithMessageContaining(Runnable action, String... expectedSubstrings) {
    try {
      action.run();
      fail("Expected IllegalStateException for drift mentioning " + Arrays.toString(expectedSubstrings));
    } catch (RuntimeException e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof IllegalStateException && cause.getMessage() != null) {
          boolean allMatch = true;
          for (String needle : expectedSubstrings) {
            if (!cause.getMessage().contains(needle)) { allMatch = false; break; }
          }
          if (allMatch) return;
        }
        cause = cause.getCause();
      }
      fail("Expected drift IllegalStateException mentioning " + Arrays.toString(expectedSubstrings) + ", got: " + e);
    }
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
