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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedIndexesTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.Upgrade;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.ViewDeploymentValidator;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexStatus;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexTracker;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexTrackerImpl;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesDAOImpl;
import org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0.AddDeferredIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeployedIndexTracker} API.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeployedIndexTracker {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ViewDeploymentValidator viewDeploymentValidator;

  private final UpgradeConfigAndContext config = new UpgradeConfigAndContext();
  { config.setDeferredIndexCreationEnabled(true); }

  private static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(), upgradeAuditTable(),
      deployedIndexesTable(),
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );


  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(INITIAL_SCHEMA, TruncationBehavior.ALWAYS);
  }

  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * markStarted should transition a PENDING deferred index to IN_PROGRESS.
   * Verifies the precondition (PENDING) and postcondition (IN_PROGRESS).
   */
  @Test
  public void testMarkStartedTransitionsToInProgress() {
    // given — upgrade creates a PENDING deferred index
    givenPendingDeferredIndex();
    DeployedIndexTracker tracker = createTracker();

    // then — verify precondition: 1 PENDING
    assertEquals("Precondition: should have 1 PENDING", Integer.valueOf(1),
        tracker.getProgress().get(DeployedIndexStatus.PENDING));

    // when
    tracker.markStarted("Product", "Product_Name_1");

    // then
    assertEquals("Should have 1 IN_PROGRESS", Integer.valueOf(1),
        tracker.getProgress().get(DeployedIndexStatus.IN_PROGRESS));
    assertEquals("Should have 0 PENDING", Integer.valueOf(0),
        tracker.getProgress().get(DeployedIndexStatus.PENDING));
  }


  /**
   * markCompleted should transition an IN_PROGRESS index to COMPLETED.
   * After completion, getPendingIndexes() should return empty (COMPLETED
   * is a terminal state) and progress should show 1 COMPLETED.
   */
  @Test
  public void testMarkCompletedTransitionsToCompleted() {
    // given
    givenPendingDeferredIndex();
    DeployedIndexTracker tracker = createTracker();
    tracker.markStarted("Product", "Product_Name_1");

    // when
    tracker.markCompleted("Product", "Product_Name_1");

    // then
    assertEquals("Should have 1 COMPLETED", Integer.valueOf(1),
        tracker.getProgress().get(DeployedIndexStatus.COMPLETED));
    assertEquals("No pending indexes after completion", 0, tracker.getPendingIndexes().size());
  }


  /**
   * markFailed should transition an IN_PROGRESS index to FAILED with an
   * error message. The failed index should appear in getPendingIndexes()
   * (FAILED is non-terminal) with the error message preserved.
   */
  @Test
  public void testMarkFailedTransitionsToFailed() {
    // given
    givenPendingDeferredIndex();
    DeployedIndexTracker tracker = createTracker();
    tracker.markStarted("Product", "Product_Name_1");

    // when
    tracker.markFailed("Product", "Product_Name_1", "Unique constraint violation");

    // then
    assertEquals("Should have 1 FAILED", Integer.valueOf(1),
        tracker.getProgress().get(DeployedIndexStatus.FAILED));
    java.util.List<org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndex> pending = tracker.getPendingIndexes();
    assertEquals(1, pending.size());
    assertEquals("Unique constraint violation", pending.get(0).getErrorMessage());
    assertEquals(org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexStatus.FAILED, pending.get(0).getStatus());
  }


  /** Creates a PENDING deferred index via an upgrade step. */
  private void givenPendingDeferredIndex() {
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(), deployedIndexesTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
    Upgrade.performUpgrade(target, Collections.singletonList(AddDeferredIndex.class),
        connectionResources, config, viewDeploymentValidator);
  }


  private DeployedIndexTracker createTracker() {
    return new DeployedIndexTrackerImpl(
        new DeployedIndexesDAOImpl(sqlScriptExecutorProvider, connectionResources));
  }
}
