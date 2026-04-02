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
import static org.junit.Assert.assertSame;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeferredIndexReadinessCheckImpl}.
 *
 * <p>In the comments-based model, the readiness check is a no-op
 * pass-through because the MetaDataProvider already includes virtual
 * deferred indexes from table comments. These tests verify that
 * {@link DeferredIndexReadinessCheck#augmentSchemaWithPendingIndexes(Schema)}
 * returns the input schema unchanged.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@NotThreadSafe
public class TestDeferredIndexReadinessCheck {

  @Rule
  public MethodRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private ConnectionResources connectionResources;
  @Inject private DatabaseSchemaManager schemaManager;

  private static final Schema TEST_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
  );

  private UpgradeConfigAndContext config;


  /** Drop and recreate the required schema before each test. */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(TEST_SCHEMA, TruncationBehavior.ALWAYS);
    config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
  }


  /** Invalidate the schema manager cache after each test. */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * augmentSchemaWithPendingIndexes should return the same schema instance
   * unchanged when deferred index creation is enabled.
   */
  @Test
  public void testAugmentSchemaReturnsInputUnchanged() {
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(config);

    Schema input = schema(
        table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
            .indexes(index("Apple_V1").columns("pips"))
    );

    Schema result = check.augmentSchemaWithPendingIndexes(input);
    assertSame("Should return the same schema instance", input, result);
  }


  /**
   * augmentSchemaWithPendingIndexes should return the same schema instance
   * when deferred index creation is disabled.
   */
  @Test
  public void testAugmentSchemaReturnsInputWhenDisabled() {
    UpgradeConfigAndContext disabledConfig = new UpgradeConfigAndContext();
    // deferredIndexCreationEnabled defaults to false
    DeferredIndexReadinessCheck check = new DeferredIndexReadinessCheckImpl(disabledConfig);

    Schema input = schema(
        table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
    );

    Schema result = check.augmentSchemaWithPendingIndexes(input);
    assertSame("Should return the same schema instance when disabled", input, result);
  }


  /**
   * The static factory method should produce a working readiness check.
   */
  @Test
  public void testStaticFactoryMethod() {
    DeferredIndexReadinessCheck check = DeferredIndexReadinessCheck.create(config);

    Schema input = schema(
        table("Apple").columns(column("pips", DataType.STRING, 10).nullable())
    );

    Schema result = check.augmentSchemaWithPendingIndexes(input);
    assertSame("Static factory should produce a working check", input, result);
  }
}
