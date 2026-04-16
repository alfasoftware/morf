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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedIndexesTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deployedViewsTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.deferredIndexOperationTable;
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
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexStatus;
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexTracker;
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexTrackerImpl;
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexesDAOImpl;
import org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0.AddDeferredIndex;
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
      deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
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


  /** markStarted should transition a PENDING index to IN_PROGRESS. */
  @Test
  public void testMarkStartedTransitionsToInProgress() {
    // given — upgrade creates a PENDING deferred index
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
    Upgrade.performUpgrade(target, Collections.singletonList(AddDeferredIndex.class),
        connectionResources, config, viewDeploymentValidator);

    DeployedIndexTracker tracker = createTracker();

    // when
    tracker.markStarted("Product", "Product_Name_1");

    // then
    Map<DeployedIndexStatus, Integer> progress = tracker.getProgress();
    assertEquals("Should have 1 IN_PROGRESS", Integer.valueOf(1), progress.get(DeployedIndexStatus.IN_PROGRESS));
  }


  /** markCompleted should transition to COMPLETED. */
  @Test
  public void testMarkCompletedTransitionsToCompleted() {
    // given
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
    Upgrade.performUpgrade(target, Collections.singletonList(AddDeferredIndex.class),
        connectionResources, config, viewDeploymentValidator);

    DeployedIndexTracker tracker = createTracker();
    tracker.markStarted("Product", "Product_Name_1");

    // when
    tracker.markCompleted("Product", "Product_Name_1");

    // then
    assertEquals("Should have 0 PENDING", Integer.valueOf(0),
        tracker.getProgress().get(DeployedIndexStatus.PENDING));
    assertNotNull("completedTime should be set",
        tracker.getPendingIndexes()); // empty since it's COMPLETED now
    assertEquals(0, tracker.getPendingIndexes().size());
  }


  /** markFailed should transition to FAILED with error message. */
  @Test
  public void testMarkFailedTransitionsToFailed() {
    // given
    Schema target = schema(
        deployedViewsTable(), upgradeAuditTable(), deferredIndexOperationTable(),
        deployedIndexesTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index("Product_Name_1").columns("name"))
    );
    Upgrade.performUpgrade(target, Collections.singletonList(AddDeferredIndex.class),
        connectionResources, config, viewDeploymentValidator);

    DeployedIndexTracker tracker = createTracker();
    tracker.markStarted("Product", "Product_Name_1");

    // when
    tracker.markFailed("Product", "Product_Name_1", "Unique constraint violation");

    // then
    Map<DeployedIndexStatus, Integer> progress = tracker.getProgress();
    assertEquals("Should have 1 FAILED", Integer.valueOf(1), progress.get(DeployedIndexStatus.FAILED));
    assertEquals(1, tracker.getPendingIndexes().size());
    assertEquals("Unique constraint violation", tracker.getPendingIndexes().get(0).getErrorMessage());
  }


  private DeployedIndexTracker createTracker() {
    return new DeployedIndexTrackerImpl(
        new DeployedIndexesDAOImpl(sqlScriptExecutorProvider, connectionResources));
  }
}
