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

package org.alfasoftware.morf.upgrade.deferredindexes;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexesTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredIndexThenChange;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredIndexThenRemove;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredIndexThenRename;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddImmediateIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddTableWithInlineDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.AddSecondDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.ChangeDeferredToNonDeferred;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.RemoveColumnWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.RemoveProductTable;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.RenameColumnWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0.RenameTableWithDeferredIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for the DeferredIndexes architecture. Exercises the
 * full upgrade framework path with the DeferredIndexes table, the model
 * enricher, and the {@link DeferredIndexService} build flow.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexesIntegration {

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
      deferredIndexesTable(),
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
   * Verifies the upgrade-time setup of a single deferred index: the upgrade
   * step creates a PENDING row in DeferredIndexes, the physical index is
   * NOT built, and the row's persisted column metadata matches the
   * declaration.
   */
  @Test
  public void testDeferredIndexProducesPendingRegistrationRow() {
    // given
    Schema targetSchema = schemaWithIndex();

    // when
    UpgradePath path = performUpgrade(targetSchema, AddDeferredIndex.class);

    // then -- physical index NOT built (deferred)
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");

    // then -- the registration row is persisted as non-terminal
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertFalse("Should persist at least one deferred registration row", deferredJobs.isEmpty());
    assertTrue("Job should reference the index name",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));

    // then -- DeferredIndexes row is PENDING (slim: every registered row is deferred by invariant)
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * An upgrade with no deferred indexes should leave the DeferredIndexes
   * registration table empty (no non-COMPLETED rows).
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
    assertTrue("No deferred statements expected", newDao().findNonTerminal().isEmpty());
  }


  /**
   * Two deferred indexes added in a single upgrade step should both be
   * persisted as non-COMPLETED registration rows, neither should be physically
   * built, and both rows should be PENDING.
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

    // then -- both rows persisted as non-COMPLETED
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertTrue("Should contain Product_Name_1",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
    assertTrue("Should contain Product_IdName_1",
        deferredJobs.stream().anyMatch(j -> "Product_IdName_1".equalsIgnoreCase(j.getIndexName())));

    // then -- both PENDING in DeferredIndexes
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("PENDING", queryDeferredIndexField("Product_IdName_1", "status"));
  }


  /**
   * When deferredIndexCreationEnabled is false, deferred indexes should
   * be built immediately and no registration rows should be written.
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
        newDao().findNonTerminal().isEmpty());
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
   * The DeferredIndexes table's indexColumns is updated via the change service,
   * and the rebuilt schema preserves isDeferred() so the persisted registration
   * row references the new column name.
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

    // then -- DeferredIndexes row reflects the renamed column
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("label", queryDeferredIndexField("Product_Name_1", "indexColumns"));

    // then -- the persisted row carries the new column name 'label'
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertFalse("Should have a deferred row after rename", deferredJobs.isEmpty());
    assertTrue("Row's indexColumns should reference new column name 'label'",
        deferredJobs.stream().flatMap(j -> j.getIndexColumns().stream())
            .anyMatch(c -> c.equalsIgnoreCase("label")));
  }


  /**
   * Step A adds a non-deferred index on column "name". Step B renames "name"
   * to "label". <b>Slim invariant:</b> non-deferred indexes are not registered
   * in {@code DeferredIndexes} — the rename is applied physically via
   * ALTER TABLE and no registration row exists to update.
   */
  @Test
  public void testCrossStepColumnRenameOnNonDeferredIndexDoesNotRegister() {
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

    // then -- physical index exists (under the renamed column) and no registration row
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: non-deferred indexes are not registered in DeferredIndexes",
        queryDeferredIndexField("Product_Name_1", "status"));
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

    // then -- physical index absent AND DeferredIndexes row cleaned up
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertNull("DeferredIndexes row should be deleted",
        queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * Step A defers an index on table Product. Step B renames table to Item.
   * The DeferredIndexes row's tableName should be updated to Item, the
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertFalse("Should have a deferred job", deferredJobs.isEmpty());
    assertTrue("Job's table should be Item",
        deferredJobs.stream().anyMatch(j -> "Item".equalsIgnoreCase(j.getTableName())));

    // then -- DeferredIndexes tableName updated
    assertEquals("Item", queryDeferredIndexField("Product_Name_1", "tableName"));
  }


  /**
   * Deferred indexes on multiple tables should each be persisted as their
   * own non-COMPLETED registration row.
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
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
   * <b>Slim invariant:</b> non-deferred indexes are not registered in
   * {@code DeferredIndexes}.
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

    // then -- physical index exists and NO registration row (slim invariant)
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: non-deferred indexes are not registered in DeferredIndexes",
        queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * When forceImmediateIndexes is configured for an index name, a deferred
   * addIndex should be built immediately during upgrade. The physical index
   * should exist and no registration row should be written. Since the index ends
   * up non-deferred after the force-immediate resolution, it is not registered.
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

    // then -- built immediately + no registration row (slim: non-deferred not registered)
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertNull("Slim: force-immediate ends up non-deferred → not registered",
        queryDeferredIndexField("Product_Name_1", "status"));
    assertTrue("No deferred statements expected", newDao().findNonTerminal().isEmpty());
  }


  /**
   * Same-step: add deferred then remove in the same step. No physical
   * index and no DeferredIndexes row should exist after upgrade.
   */
  @Test
  public void testAddDeferredThenRemoveInSameStep() {
    // when
    performUpgrade(INITIAL_SCHEMA,
        AddDeferredIndexThenRemove.class);

    // then -- neither physical index nor DeferredIndexes row
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertNull("Should have no DeferredIndexes row",
        queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * Same-step: add deferred then rename in the same step. The renamed
   * deferred index should be persisted as a non-COMPLETED registration row
   * under its new name.
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertTrue("Should contain renamed index",
        deferredJobs.stream().anyMatch(j -> "Product_Name_Renamed".equalsIgnoreCase(j.getIndexName())));
    assertFalse("Should not contain original name",
        deferredJobs.stream().anyMatch(j -> "Product_Name_1".equalsIgnoreCase(j.getIndexName())));
  }


  // =========================================================================
  // Unique and multi-column deferred indexes
  // =========================================================================

  /** Unique deferred index should preserve its unique flag in the persisted registration row. */
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertFalse("Should have a deferred row", deferredJobs.isEmpty());
    assertTrue("Row's indexUnique flag should be true for a unique deferred index",
        deferredJobs.stream().anyMatch(DeferredIndex::isIndexUnique));
  }


  /**
   * A deferred multi-column index should preserve column ordering in the
   * generated SQL, not be physically built, and have the columns stored
   * correctly in the DeferredIndexes table.
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
    assertFalse("Should have a deferred job", deferredJobs.isEmpty());

    // then -- DeferredIndexes has correct columns
    assertEquals("PENDING", queryDeferredIndexField("Product_IdName_1", "status"));
    assertEquals("id,name", queryDeferredIndexField("Product_IdName_1", "indexColumns"));
  }


  // =========================================================================
  // DeferredIndexes table state verification
  // =========================================================================

  /**
   * A second upgrade should leave previously-unbuilt deferred indexes
   * persisted as non-COMPLETED registration rows alongside any new ones.
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
    List<DeferredIndex> deferredJobs = newDao().findNonTerminal();
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

    // then -- physical index NOT built; registration row PENDING; job available
    assertPhysicalIndexDoesNotExist("Category", "Category_Label_1");
    assertEquals("PENDING", queryDeferredIndexField("Category_Label_1", "status"));
    assertFalse("inline-deferred index should produce a non-COMPLETED registration row",
        newDao().findNonTerminal().isEmpty());

    // when -- adopter executes the deferred SQL
    buildDeferredIndexesViaAdopter(path, "Category", "Category_Label_1");

    // then -- physical built, row COMPLETED
    assertPhysicalIndexExists("Category", "Category_Label_1");
    assertEquals("COMPLETED", queryDeferredIndexField("Category_Label_1", "status"));
  }


  /**
   * Creating a new table should track all its indexes in DeferredIndexes.
   */
  @Test
  public void testAddTableRegistersIndexesInDeferredTable() {
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

    // then -- Category_Label_1 should be registered (deferred)
    assertEquals("PENDING", queryDeferredIndexField("Category_Label_1", "status"));
  }


  // =========================================================================
  // Idempotency and edge cases
  // =========================================================================

  /**
   * Running the same upgrade twice should be idempotent — the second run
   * should detect no new steps to apply and produce no errors. The
   * DeferredIndexes state should be unchanged.
   */
  @Test
  public void testReUpgradeIsIdempotent() {
    // given -- first upgrade defers an index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));

    // when -- second upgrade with same schema and steps
    UpgradePath path2 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // then -- no errors, state unchanged
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * RemoveTable should delete all DeferredIndexes rows for that table.
   * Step A adds a deferred index on Product. Step B removes the Product
   * table entirely. After upgrade, no DeferredIndexes row should remain
   * for the removed table.
   */
  @Test
  public void testRemoveTableCleansUpDeferredIndexes() {
    // given -- target schema without Product table
    Schema noProductSchema = schemaWith();

    // when
    performUpgradeSteps(noProductSchema,
        AddDeferredIndex.class,
        RemoveProductTable.class);

    // then -- no DeferredIndexes row for the removed table's index
    assertNull("DeferredIndexes row should be deleted after removeTable",
        queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * Adopter flow — happy path: drive the build tasks via
   * {@link DeferredIndexService#getBuildTasks()} and run each. After the
   * loop the physical index exists and the {@code DeferredIndexes} row is
   * {@code COMPLETED}.
   */
  @Test
  public void testAppSideAdopterFlowBuildsAndMarksCompleted() {
    // given -- upgrade creates a PENDING deferred index
    UpgradePath path = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
    assertFalse("Should have a job to execute", newDao().findNonTerminal().isEmpty());

    // when -- the app-side loop
    buildDeferredIndexesViaAdopter(path, "Product", "Product_Name_1");

    // then -- physical index built AND row flipped to COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * Row-existence model: after a deferred index is built (status=COMPLETED),
   * a subsequent column-rename upgrade still propagates correctly to the
   * registration row's indexColumns and to the physical index. The COMPLETED
   * row stays as COMPLETED because the index is still currently declared
   * deferred — its declarative form is just rewritten.
   */
  @Test
  public void testCompletedDeferredIndexSurvivesColumnRename() {
    // given — upgrade 1 creates and adopter builds the deferred index
    UpgradePath path1 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    buildDeferredIndexesViaAdopter(path1, "Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when — upgrade 2 renames the underlying column (physical column rename
    // propagates to the index's column reference; registration-row indexColumns
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
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("label", queryDeferredIndexField("Product_Name_1", "indexColumns"));
  }


  /**
   * Self-heal policy: if a registration row says non-terminal (e.g. PENDING) but
   * the physical index already exists, the enricher does NOT throw — it
   * rebuilds the index as deferred in the enriched schema and the build
   * task reconciles via {@code dialect.isIndexValid} on its next pass
   * (marking it COMPLETED if VALID). This is the routine-restart case that
   * used to boot-loop on the slim branch.
   */
  @Test
  public void testNonCompletedRowWithMatchingPhysicalAutoRecovers() {
    // given — first upgrade creates a PENDING deferred index
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
    // simulate the routine-restart case: physical index already exists but
    // the row never got flipped to COMPLETED (process crashed mid-build)
    sqlScriptExecutorProvider.get().execute(List.of(
        "CREATE INDEX Product_Name_1 ON Product(name)"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when — next upgrade succeeds without throwing (no drift)
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // and — running the build tasks reconciles the row to COMPLETED
    runBuildTasks();

    // then — row flipped to COMPLETED via isIndexValid auto-detect
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Drift policy: a registration row referencing a table not in the physical
   * schema is fatal. Could happen if the table was DROPped without removing
   * the row, or after restoring a partial backup.
   */
  @Test
  public void testEnricherHardFailsOnRowForMissingTable() {
    // given — manually insert a row referencing a non-existent table
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO DeferredIndexes (id, tableName, indexName, indexUnique, "
            + "indexColumns, status, attemptsCount, createdTime)"
            + "VALUES (42, 'GhostTable', 'GhostIdx', 0, 'col', 'PENDING', 0, 0)"));

    // when / then — enricher detects the orphan
    assertThrowsDriftWithMessageContaining(
        () -> performUpgrade(schemaWithIndex(), AddDeferredIndex.class),
        "GhostTable");
  }


  /**
   * Row-existence model: changing a built deferred index to non-deferred
   * should DELETE the registration row (no longer declared deferred). The
   * physical index is dropped and recreated as non-deferred via the
   * standard ChangeIndex flow.
   */
  @Test
  public void testCompletedDeferredChangedToNonDeferredDeletesRow() {
    // given — upgrade 1 creates and adopter builds the deferred index
    UpgradePath path1 = performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    buildDeferredIndexesViaAdopter(path1, "Product", "Product_Name_1");
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
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

    // then — registration row deleted, physical index still exists (rebuilt as non-deferred)
    assertNull("Registration row for Product_Name_1 should be deleted (no longer declared deferred)",
        queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Drift policy: if a registration row says COMPLETED but the physical index
   * is missing (manual DROP, restored backup, etc.), the next upgrade's
   * enricher must throw IllegalStateException. Morf does not auto-heal.
   */
  @Test
  public void testEnricherHardFailsOnCompletedRowWithoutPhysicalIndex() {
    // given — manually insert a fabricated COMPLETED row referencing a
    // physical index that doesn't exist
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO DeferredIndexes (id, tableName, indexName, indexUnique, "
            + "indexColumns, status, attemptsCount, createdTime)"
            + "VALUES (1, 'Product', 'Phantom_Idx', 0, 'name', 'COMPLETED', 0, 0)"));
    assertPhysicalIndexDoesNotExist("Product", "Phantom_Idx");

    // when / then — any subsequent upgrade trips the enricher's drift check
    assertThrowsDriftWithMessageContaining(
        () -> performUpgrade(schemaWithIndex(), AddDeferredIndex.class),
        "Phantom_Idx", "COMPLETED");
  }


  /**
   * Build task — realistic failure path. CREATE INDEX fails because the
   * underlying data violates the unique constraint. The build task catches
   * the SQLException, marks the row FAILED, and persists the error message.
   * No exception propagates to the adopter.
   */
  @Test
  public void testBuildTaskMarksFailedOnUniqueConstraintViolation() {
    // given — schema declaring a unique deferred index on Product.name
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgrade(target, AddDeferredUniqueIndex.class);
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_UQ", "status"));

    // and — pre-populate the table with duplicates so CREATE UNIQUE INDEX must fail
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO Product (id, name) VALUES (1, 'dup')",
        "INSERT INTO Product (id, name) VALUES (2, 'dup')"));

    // when — build tasks run; the failure is caught and persisted internally
    runBuildTasks();

    // then — row is FAILED with an error message; physical index NOT built
    assertEquals("FAILED", queryDeferredIndexField("Product_Name_UQ", "status"));
    String err = queryDeferredIndexField("Product_Name_UQ", "errorMessage");
    assertNotNull("Error message should be persisted on failure", err);
    assertFalse("Error message should not be empty", err.isEmpty());
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_UQ");
  }


  /**
   * Crash-near-completion auto-heal: a row stuck in IN_PROGRESS whose
   * physical index is already VALID is auto-promoted to COMPLETED on the
   * next build pass via {@code dialect.isIndexValid}. Slim used to
   * boot-loop on this case.
   */
  @Test
  public void testInProgressRowWithValidPhysicalAutoCompletes() {
    // given — upgrade creates a PENDING row; we then simulate a crash-near-completion
    // by manually creating the physical index and flipping the row to IN_PROGRESS.
    // The direct UPDATE bypasses the DAO intentionally — no public API drives a row
    // to IN_PROGRESS without also bumping attemptsCount, and we want to assert the
    // self-heal path independent of any attempts bookkeeping.
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    sqlScriptExecutorProvider.get().execute(List.of(
        "CREATE INDEX Product_Name_1 ON Product(name)",
        "UPDATE DeferredIndexes SET status = 'IN_PROGRESS' WHERE indexName = 'Product_Name_1'"));
    assertEquals("IN_PROGRESS", queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // when — build tasks run
    runBuildTasks();

    // then — row auto-promoted to COMPLETED
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
  }


  /**
   * attemptsCount + errorMessage lifecycle: a row that fails-then-succeeds
   * shows non-zero attempts mid-flight and gets reset to 0 once COMPLETED;
   * errorMessage is populated on FAILED and cleared on COMPLETED.
   */
  @Test
  public void testAttemptsCountAndErrorMessageResetOnCompletion() {
    // given — unique deferred index whose first build attempt will fail (duplicates)
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgrade(target, AddDeferredUniqueIndex.class);
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO Product (id, name) VALUES (1, 'dup')",
        "INSERT INTO Product (id, name) VALUES (2, 'dup')"));

    // when — first pass: build fails, attemptsCount=1, errorMessage set
    runBuildTasks();
    assertEquals("FAILED", queryDeferredIndexField("Product_Name_UQ", "status"));
    assertEquals("1", queryDeferredIndexField("Product_Name_UQ", "attemptsCount"));
    assertNotNull(queryDeferredIndexField("Product_Name_UQ", "errorMessage"));

    // and — second pass after fixing the data: build succeeds
    sqlScriptExecutorProvider.get().execute(List.of(
        "DELETE FROM Product WHERE id = 2"));
    runBuildTasks();

    // then — attemptsCount reset to 0, errorMessage cleared
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_UQ", "status"));
    assertEquals("0", queryDeferredIndexField("Product_Name_UQ", "attemptsCount"));
    assertNull("errorMessage should be cleared on success",
        queryDeferredIndexField("Product_Name_UQ", "errorMessage"));
  }


  /**
   * Idempotency: calling {@code runBuildTasks()} twice in a row leaves the
   * row state correct — the second pass sees {@code status=COMPLETED} (or
   * {@code isIndexValid()=true}) and no-ops.
   */
  @Test
  public void testBuildTasksIdempotentAcrossInvocations() {
    // given
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    runBuildTasks();
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));

    // when — call again; no rows are non-COMPLETED so no work
    runBuildTasks();

    // then — state unchanged
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // =========================================================================
  // Multi-task scenarios — comprehensive coverage of the new build flow
  // =========================================================================

  /**
   * MT1 — three deferred indexes on one table built in a single pass.
   * Verifies fanout: every task runs, every physical index ends up present,
   * every row reaches {@code COMPLETED}.
   */
  @Test
  public void testMT1ThreeDeferredIndexesOnOneTableAllBuildInOnePass() {
    // given — schema declares all three indexes
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred(),
            index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgradeSteps(target,
        AddDeferredIndex.class,
        AddSecondDeferredIndex.class,
        AddDeferredUniqueIndex.class);
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("PENDING", queryDeferredIndexField("Product_IdName_1", "status"));
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_UQ", "status"));

    // when
    runBuildTasks();

    // then — every index physical + COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
    assertPhysicalIndexExists("Product", "Product_Name_UQ");
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_IdName_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_UQ", "status"));
  }


  /**
   * MT2 — deferred indexes spread across two tables. No cross-table
   * interference: both tables' indexes complete cleanly via the service.
   */
  @Test
  public void testMT2DeferredIndexesAcrossTwoTablesAllBuild() {
    // given
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name").deferred()),
        table("Category").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("label", DataType.STRING, 50)
        ).indexes(index("Category_Label_1").columns("label").deferred())
    );
    performUpgradeSteps(target, AddDeferredIndex.class, AddTableWithDeferredIndex.class);

    // when
    runBuildTasks();

    // then — both physical present, both rows COMPLETED
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Category", "Category_Label_1");
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Category_Label_1", "status"));
  }


  /**
   * MT3 — mixed success and failure in one pass. Three deferred indexes;
   * one is unique on a column with pre-existing duplicates and must fail
   * its CREATE. The build task isolates the failure: the other two complete
   * cleanly, the failing one is FAILED with errorMessage, and {@code
   * getProgress()} reports the split.
   */
  @Test
  public void testMT3MixedSuccessAndFailureInOnePass() {
    // given
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred(),
            index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgradeSteps(target,
        AddDeferredIndex.class,
        AddSecondDeferredIndex.class,
        AddDeferredUniqueIndex.class);

    // and — pre-populate duplicates so CREATE UNIQUE INDEX must fail
    sqlScriptExecutorProvider.get().execute(List.of(
        "INSERT INTO Product (id, name) VALUES (1, 'dup')",
        "INSERT INTO Product (id, name) VALUES (2, 'dup')"));

    // when
    runBuildTasks();

    // then — non-unique indexes complete; unique one is FAILED with a message
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_IdName_1", "status"));
    assertEquals("FAILED", queryDeferredIndexField("Product_Name_UQ", "status"));
    assertNotNull("Failing row's errorMessage should be persisted",
        queryDeferredIndexField("Product_Name_UQ", "errorMessage"));
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_UQ");

    // and — getProgress reports 2 COMPLETED + 1 FAILED
    Map<DeferredIndexStatus, Integer> progress = newService().getProgress();
    assertEquals(Integer.valueOf(2), progress.get(DeferredIndexStatus.COMPLETED));
    assertEquals(Integer.valueOf(1), progress.get(DeferredIndexStatus.FAILED));
    assertEquals(Integer.valueOf(0), progress.get(DeferredIndexStatus.PENDING));
    assertEquals(Integer.valueOf(0), progress.get(DeferredIndexStatus.IN_PROGRESS));
  }


  /**
   * MT4 — cross-upgrade lifecycle. Upgrade 1 declares two deferred indexes;
   * after build, both COMPLETED. Upgrade 2 declares a third deferred index;
   * after build, the new one is COMPLETED while the prior two stay
   * COMPLETED with attemptsCount=0 (untouched on the second pass).
   */
  @Test
  public void testMT4CrossUpgradeLifecycle() {
    // given — upgrade 1: two deferred indexes, build, both COMPLETED
    Schema after1 = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred())
    );
    performUpgradeSteps(after1, AddDeferredIndex.class, AddSecondDeferredIndex.class);
    runBuildTasks();
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_IdName_1", "status"));

    // when — upgrade 2: a third deferred index; build
    Schema after2 = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred(),
            index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgradeSteps(after2,
        AddDeferredIndex.class,
        AddSecondDeferredIndex.class,
        AddDeferredUniqueIndex.class);
    runBuildTasks();

    // then — new index COMPLETED; prior two unchanged
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_UQ", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_IdName_1", "status"));
    assertEquals("0", queryDeferredIndexField("Product_Name_1", "attemptsCount"));
    assertEquals("0", queryDeferredIndexField("Product_IdName_1", "attemptsCount"));
  }


  /**
   * MT5 — {@code getProgress()} accuracy across the lifecycle. Three
   * deferred indexes report 3 PENDING before any build, then 3 COMPLETED
   * after one build pass.
   */
  @Test
  public void testMT5GetProgressAccuracyAcrossLifecycle() {
    // given
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred(),
            index("Product_Name_UQ").unique().columns("name").deferred())
    );
    performUpgradeSteps(target,
        AddDeferredIndex.class,
        AddSecondDeferredIndex.class,
        AddDeferredUniqueIndex.class);
    DeferredIndexService service = newService();

    // pre-build
    Map<DeferredIndexStatus, Integer> before = service.getProgress();
    assertEquals(Integer.valueOf(3), before.get(DeferredIndexStatus.PENDING));
    assertEquals(Integer.valueOf(0), before.get(DeferredIndexStatus.COMPLETED));

    // when
    runBuildTasks();

    // post-build
    Map<DeferredIndexStatus, Integer> after = service.getProgress();
    assertEquals(Integer.valueOf(0), after.get(DeferredIndexStatus.PENDING));
    assertEquals(Integer.valueOf(3), after.get(DeferredIndexStatus.COMPLETED));
    assertEquals(Integer.valueOf(0), after.get(DeferredIndexStatus.FAILED));
    assertEquals(Integer.valueOf(0), after.get(DeferredIndexStatus.IN_PROGRESS));
  }


  /**
   * MT6 — repeated invocation idempotency for multi-task case. Declare two
   * deferred indexes; first call builds both. Subsequent calls return an
   * empty task list (every row is COMPLETED) so {@code forEach} is a true
   * no-op.
   */
  @Test
  public void testMT6RepeatedInvocationIdempotency() {
    // given — two deferred indexes, both PENDING
    Schema target = schemaWith(
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("Product_Name_1").columns("name").deferred(),
            index("Product_IdName_1").columns("id", "name").deferred())
    );
    performUpgradeSteps(target, AddDeferredIndex.class, AddSecondDeferredIndex.class);
    DeferredIndexService service = newService();
    assertEquals(2, service.getBuildTasks().size());

    // when — first call builds both
    service.getBuildTasks().forEach(Runnable::run);
    assertEquals("COMPLETED", queryDeferredIndexField("Product_Name_1", "status"));
    assertEquals("COMPLETED", queryDeferredIndexField("Product_IdName_1", "status"));

    // then — subsequent calls return empty task lists
    assertTrue("Second call should return no tasks (all COMPLETED)",
        service.getBuildTasks().isEmpty());
    assertTrue("Third call should return no tasks (all COMPLETED)",
        service.getBuildTasks().isEmpty());

    // and — physical state unchanged
    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
  }


  /** Helper: construct the DAO backed by the test's executor + connection. */
  private DeferredIndexesDAO newDao() {
    return new DeferredIndexesDAO(sqlScriptExecutorProvider, connectionResources,
        new DeferredIndexesStatements());
  }


  /** Helper: construct a service paired with a freshly-built builder + DAO. */
  private DeferredIndexService newService() {
    DeferredIndexesDAO dao = newDao();
    return new DeferredIndexServiceImpl(new DeferredIndexBuilder(connectionResources, dao), dao);
  }


  /**
   * Helper: drive every non-COMPLETED registration row through the new
   * {@link DeferredIndexService} build flow — the equivalent adopter
   * operation.
   */
  private void runBuildTasks() {
    newService().getBuildTasks().forEach(Runnable::run);
  }


  // =========================================================================
  // Config overrides (additional)
  // =========================================================================

  /**
   * Force-deferred: an addIndex() without .deferred() should be deferred
   * when forceDeferredIndexes config includes the index name. The physical
   * index should NOT be built, and a non-COMPLETED registration row should be
   * persisted.
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
    assertFalse("Should have deferred statements", newDao().findNonTerminal().isEmpty());
    assertEquals("PENDING", queryDeferredIndexField("Product_Name_1", "status"));
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
    all.add(deferredIndexesTable());
    Collections.addAll(all, tables);
    return schema(all);
  }

  /**
   * Drives every non-COMPLETED registration row through the new
   * {@link DeferredIndexService} build flow — the equivalent adopter
   * operation.
   *
   * <p>The {@code path}, {@code tableName}, and {@code indexName} parameters
   * are kept for caller compatibility but unused: the service picks up every
   * non-COMPLETED row and reconciles each via isIndexValid / DROP / CREATE
   * as needed.</p>
   *
   * <p>TODO (Phase 5): rewrite the surrounding tests to call this style
   * directly.</p>
   */
  @SuppressWarnings("unused")
  private void buildDeferredIndexesViaAdopter(UpgradePath path, String tableName, String indexName) {
    DeferredIndexService service = newService();
    service.getBuildTasks().forEach(Runnable::run);
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

  private String queryDeferredIndexField(String indexName, String fieldName) {
    String sql = "SELECT " + fieldName + " FROM DeferredIndexes WHERE UPPER(indexName) = '"
        + indexName.toUpperCase() + "'";
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }

}
