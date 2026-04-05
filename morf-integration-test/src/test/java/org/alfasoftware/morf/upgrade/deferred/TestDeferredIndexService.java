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
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddTwoDeferredIndexes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for the {@link DeferredIndexService} facade, verifying
 * the full lifecycle through a real database: upgrade step declares deferred
 * indexes via table comments, then the service scans for unbuilt deferred
 * indexes and builds them.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexService {

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


  /** Verify that execute() builds the deferred index and it exists in the schema. */
  @Test
  public void testExecuteBuildsIndexEndToEnd() {
    // given
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);

    // when
    DeferredIndexService service = createService();
    service.execute();
    service.awaitCompletion(60L);

    // then
    assertIndexExists("Product", "Product_Name_1");
  }


  /** Verify that execute() handles multiple deferred indexes in a single run. */
  @Test
  public void testExecuteBuildsMultipleIndexes() {
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
    performUpgrade(targetSchema, AddTwoDeferredIndexes.class);

    // when
    DeferredIndexService service = createService();
    service.execute();
    service.awaitCompletion(60L);

    // then
    assertIndexExists("Product", "Product_Name_1");
    assertIndexExists("Product", "Product_IdName_1");
  }


  /** Verify that execute() with no deferred indexes completes immediately. */
  @Test
  public void testExecuteWithEmptySchema() {
    // when
    DeferredIndexService service = createService();
    service.execute();

    // then
    assertTrue("Should complete immediately on empty schema", service.awaitCompletion(5L));
  }


  /** Verify that awaitCompletion() throws when called before execute(). */
  @Test(expected = IllegalStateException.class)
  public void testAwaitCompletionThrowsWhenNoExecution() {
    // given
    DeferredIndexService service = createService();

    // when -- should throw
    service.awaitCompletion(5L);
  }


  /** Verify that execute() is idempotent -- second run is a safe no-op. */
  @Test
  public void testExecuteIdempotent() {
    // given -- deferred index already built
    performUpgrade(schemaWithIndex(), AddDeferredIndex.class);
    DeferredIndexService service = createService();
    service.execute();
    service.awaitCompletion(60L);
    assertIndexExists("Product", "Product_Name_1");

    // when -- second execute on a fresh service
    DeferredIndexService service2 = createService();
    service2.execute();
    service2.awaitCompletion(60L);

    // then -- still exists, no error
    assertIndexExists("Product", "Product_Name_1");
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private void performUpgrade(Schema targetSchema, Class<? extends UpgradeStep> upgradeStep) {
    Upgrade.performUpgrade(targetSchema, Collections.singletonList(upgradeStep),
        connectionResources, upgradeConfigAndContext, viewDeploymentValidator);
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


  private void assertIndexExists(String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  private DeferredIndexService createService() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    DeferredIndexExecutor executor = new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider, config,
        new DeferredIndexExecutorServiceFactory.Default());
    return new DeferredIndexServiceImpl(executor);
  }
}
