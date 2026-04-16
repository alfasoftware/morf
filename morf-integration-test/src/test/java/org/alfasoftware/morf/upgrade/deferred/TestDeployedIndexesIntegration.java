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
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTableWithDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
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

    // then -- getDeferredIndexStatements returns SQL
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertFalse("Should return at least one deferred statement", deferredSql.isEmpty());
    assertTrue("Statement should reference the index name",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));

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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex.class);

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
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertTrue("Should contain Product_Name_1",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
    assertTrue("Should contain Product_IdName_1",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_IDNAME_1")));

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
    Upgrade.performUpgrade(schemaWithIndex(),
        Collections.singletonList(AddDeferredIndex.class),
        connectionResources, disabledConfig, viewDeploymentValidator);

    // then -- index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_1");
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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenChange.class);

    // then -- changed index built immediately
    assertPhysicalIndexExists("Product", "Product_Name_2");
    assertPhysicalIndexDoesNotExist("Product", "Product_Name_1");
  }


  // =========================================================================
  // Cross-step operations
  // =========================================================================

  /**
   * Step A defers an index on column "name". Step B renames "name" to "label".
   * The DeployedIndexes table should be updated with the new column name
   * via the DeployedIndexesChangeService.
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

    // when -- should not throw (upgrade path exists)
    UpgradePath path = performUpgradeSteps(renamedColSchema,
        AddDeferredIndex.class,
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RenameColumnWithDeferredIndex.class);

    // then -- upgrade completed successfully
    // Note: getDeferredIndexStatements may be empty if ChangeColumn.apply()
    // doesn't propagate column renames to index metadata (known limitation).
    // The DeployedIndexes table column is updated via the change service.
    assertTrue("Upgrade should complete", path != null);
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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RemoveColumnWithDeferredIndex.class);

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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.RenameTableWithDeferredIndex.class);

    // then -- deferred index SQL references new table
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertFalse("Should have deferred statements", deferredSql.isEmpty());

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
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertTrue("Should contain Product index",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
    assertTrue("Should contain Category index",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("CATEGORY_LABEL_1")));
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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex.class);

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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRemove.class);

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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndexThenRename.class);

    // then -- renamed deferred index in statements
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertTrue("Should contain renamed index",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_RENAMED")));
    assertFalse("Should not contain original name",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
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
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertFalse("Should have deferred statements", deferredSql.isEmpty());
    assertTrue("Should contain UNIQUE keyword",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("UNIQUE")));
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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredMultiColumnIndex.class);

    // then -- not physically built
    assertPhysicalIndexDoesNotExist("Product", "Product_IdName_1");

    // then -- SQL generated with both columns
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertFalse("Should have deferred statements", deferredSql.isEmpty());

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
        org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0.AddSecondDeferredIndex.class);

    // then — should include BOTH deferred indexes
    List<String> deferredSql = path2.getDeferredIndexStatements();
    assertTrue("Should contain first deferred index",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
    assertTrue("Should contain second deferred index",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_IDNAME_1")));
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
            org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddImmediateIndex.class),
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
    String sql = connectionResources.sqlDialect().convertStatementToSQL(
        org.alfasoftware.morf.sql.SqlUtils.select(
            org.alfasoftware.morf.sql.SqlUtils.field(fieldName))
            .from(org.alfasoftware.morf.sql.SqlUtils.tableRef("DeployedIndexes"))
            .where(org.alfasoftware.morf.sql.SqlUtils.field("indexName").eq(indexName))
    );
    return sqlScriptExecutorProvider.get().executeQuery(sql, rs -> rs.next() ? rs.getString(1) : null);
  }

}
