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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedIndexesTable;
import static org.junit.Assert.assertFalse;
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
      deferredIndexOperationTable(),
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
   * After upgrade with a deferred index, getDeferredIndexStatements()
   * should return SQL for the unbuilt index.
   */
  @Test
  public void testGetDeferredIndexStatementsReturnsSQL() {
    // given
    Schema targetSchema = schemaWithIndex();

    // when
    UpgradePath path = performUpgrade(targetSchema, AddDeferredIndex.class);

    // then
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertFalse("Should return at least one deferred statement", deferredSql.isEmpty());
    assertTrue("Statement should reference the index name",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
  }


  /**
   * An upgrade with no deferred indexes should return empty
   * getDeferredIndexStatements().
   */
  @Test
  public void testNoDeferredIndexesReturnsEmptyStatements() {
    // given -- feature enabled but no deferred indexes in the step
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
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
   * Two deferred indexes in one step should both appear in
   * getDeferredIndexStatements().
   */
  @Test
  public void testMultipleDeferredIndexesInOneStep() {
    // given
    Schema targetSchema = schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
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

    // then
    List<String> deferredSql = path.getDeferredIndexStatements();
    assertTrue("Should contain Product_Name_1",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_NAME_1")));
    assertTrue("Should contain Product_IdName_1",
        deferredSql.stream().anyMatch(s -> s.toUpperCase().contains("PRODUCT_IDNAME_1")));
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

  private Schema schemaWithIndex() {
    return schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
  }

  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
