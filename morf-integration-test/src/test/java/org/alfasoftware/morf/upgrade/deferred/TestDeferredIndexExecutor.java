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
import static org.junit.Assert.assertTrue;

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
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredMultiColumnIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredUniqueIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeferredIndexExecutorImpl}.
 *
 * <p>Verifies that the executor scans the database schema for deferred
 * indexes (declared in table comments but not yet physically built) and
 * creates them.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexExecutor {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );

  private UpgradeConfigAndContext upgradeConfigAndContext;


  /** Create a fresh schema and a default config before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
    upgradeConfigAndContext.setDeferredIndexCreationEnabled(true);
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * After an upgrade step adds a deferred index, the executor should
   * physically build it and the index should exist in the database schema.
   */
  @Test
  public void testExecutorBuildsDeferred() {
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), AddDeferredIndex.class);
    assertDeferredIndexPending("Product", "Product_Name_1");

    createExecutor().execute().join();

    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  /**
   * Executing on a schema with no deferred indexes should complete
   * immediately with no errors.
   */
  @Test
  public void testEmptySchemaReturnsImmediately() {
    createExecutor().execute().join();
    // No exception means success
  }


  /** A deferred unique index should be built with the UNIQUE constraint. */
  @Test
  public void testUniqueIndexCreated() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_UQ").unique().columns("name"))
    );
    performUpgrade(target, AddDeferredUniqueIndex.class);

    createExecutor().execute().join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Product_Name_UQ should be unique",
          sr.getTable("Product").indexes().stream()
              .filter(idx -> "Product_Name_UQ".equalsIgnoreCase(idx.getName()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Index not found"))
              .isUnique());
    }
  }


  /** A deferred multi-column index should preserve column ordering. */
  @Test
  public void testMultiColumnIndexCreated() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_IdName_1").columns("id", "name"))
    );
    performUpgrade(target, AddDeferredMultiColumnIndex.class);

    createExecutor().execute().join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      org.alfasoftware.morf.metadata.Index idx = sr.getTable("Product").indexes().stream()
          .filter(i -> "Product_IdName_1".equalsIgnoreCase(i.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Multi-column index not found"));
      assertEquals("column count", 2, idx.columnNames().size());
      assertTrue("first column should be id", idx.columnNames().get(0).equalsIgnoreCase("id"));
    }
  }


  /** Two deferred indexes added in one step should both be built. */
  @Test
  public void testMultipleDeferredIndexesBuilt() {
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
    performUpgrade(target, AddTwoDeferredIndexes.class);

    createExecutor().execute().join();

    assertPhysicalIndexExists("Product", "Product_Name_1");
    assertPhysicalIndexExists("Product", "Product_IdName_1");
  }


  /**
   * Running the executor a second time after all indexes are built
   * should be a safe no-op.
   */
  @Test
  public void testExecutorIdempotent() {
    performUpgrade(schemaWithIndex("Product_Name_1", "name"), AddDeferredIndex.class);

    createExecutor().execute().join();
    assertPhysicalIndexExists("Product", "Product_Name_1");

    // Second run should complete without error
    createExecutor().execute().join();
    assertPhysicalIndexExists("Product", "Product_Name_1");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void performUpgrade(Schema targetSchema,
                               Class<? extends org.alfasoftware.morf.upgrade.UpgradeStep> step) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(step),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
  }


  private DeferredIndexExecutor createExecutor() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    return new DeferredIndexExecutorImpl(
        connectionResources,
        sqlScriptExecutorProvider,
        config,
        new DeferredIndexExecutorServiceFactory.Default());
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


  /**
   * Verifies that a deferred index exists in the schema as a virtual
   * (not yet physically built) index with {@code isDeferred()=true}.
   */
  private void assertDeferredIndexPending(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Deferred index " + indexName + " should be present with isDeferred()=true",
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName()) && idx.isDeferred()));
    }
  }


  /**
   * Verifies that a physical index exists in the database schema.
   */
  private void assertPhysicalIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
