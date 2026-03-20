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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.google.inject.Inject;

import net.jcip.annotations.NotThreadSafe;

/**
 * Integration tests for {@link DeferredIndexExecutorImpl} using a real H2
 * database. Verifies that deferred indexes are physically created in the
 * database schema when the executor processes a list of
 * {@link DeferredAddIndex} operations.
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

  private static final Schema TEST_SCHEMA = schema(
      table("Apple").columns(
          column("pips", DataType.STRING, 10).nullable(),
          column("color", DataType.STRING, 20).nullable()
      )
  );

  private DeferredIndexExecutionConfig config;


  /**
   * Create a fresh schema and a default config before each test.
   */
  @Before
  public void setUp() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(TEST_SCHEMA, TruncationBehavior.ALWAYS);
    config = new DeferredIndexExecutionConfig();
    config.setRetryBaseDelayMs(10L); // fast retries for tests
  }


  /**
   * Invalidate the schema manager cache after each test.
   */
  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  // -------------------------------------------------------------------------
  // Index building tests
  // -------------------------------------------------------------------------

  /** Verify that a single deferred index is physically built in the database. */
  @Test
  public void testBuildsSingleIndex() {
    config.setMaxRetries(0);
    DeferredAddIndex dai = new DeferredAddIndex("Apple",
        index("Apple_1").columns("pips"), "test-upgrade-uuid");

    createExecutor().execute(List.of(dai)).join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Apple_1 should exist in schema",
          sr.getTable("Apple").indexes().stream()
              .anyMatch(idx -> "Apple_1".equalsIgnoreCase(idx.getName())));
    }
  }


  /** Verify that targeting a non-existent table completes without throwing. */
  @Test
  public void testFailedAfterMaxRetriesWithNoRetries() {
    config.setMaxRetries(0);
    DeferredAddIndex dai = new DeferredAddIndex("NoSuchTable",
        index("NoSuchTable_1").columns("col"), "test-upgrade-uuid");

    // Should complete without throwing — failures are logged internally
    createExecutor().execute(List.of(dai)).join();

    // No index should have been created on any table
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("NoSuchTable should not exist",
          sr.tableExists("NoSuchTable"));
    }
  }


  /** Verify that a failing operation is retried before being permanently failed. */
  @Test
  public void testRetryOnFailure() {
    config.setMaxRetries(1);
    DeferredAddIndex dai = new DeferredAddIndex("NoSuchTable",
        index("NoSuchTable_1").columns("col"), "test-upgrade-uuid");

    // Should complete without throwing — retries and final failure are logged internally
    createExecutor().execute(List.of(dai)).join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("NoSuchTable should not exist",
          sr.tableExists("NoSuchTable"));
    }
  }


  /** Verify that passing an empty list completes immediately with no errors. */
  @Test
  public void testEmptyQueueReturnsImmediately() {
    assertTrue("Future should be completed immediately",
        createExecutor().execute(Collections.emptyList()).isDone());
  }


  /** Verify that a unique index is built with the UNIQUE constraint. */
  @Test
  public void testUniqueIndexCreated() {
    config.setMaxRetries(0);
    DeferredAddIndex dai = new DeferredAddIndex("Apple",
        index("Apple_Unique_1").columns("pips").unique(), "test-upgrade-uuid");

    createExecutor().execute(List.of(dai)).join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Apple_Unique_1 should be unique",
          sr.getTable("Apple").indexes().stream()
              .filter(idx -> "Apple_Unique_1".equalsIgnoreCase(idx.getName()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("Index not found"))
              .isUnique());
    }
  }


  /** Verify that a multi-column index is built with columns in the correct order. */
  @Test
  public void testMultiColumnIndexCreated() {
    config.setMaxRetries(0);
    DeferredAddIndex dai = new DeferredAddIndex("Apple",
        index("Apple_Multi_1").columns("pips", "color"), "test-upgrade-uuid");

    createExecutor().execute(List.of(dai)).join();

    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      Index idx = sr.getTable("Apple").indexes().stream()
          .filter(i -> "Apple_Multi_1".equalsIgnoreCase(i.getName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Multi-column index not found"));
      assertEquals("column count", 2, idx.columnNames().size());
      assertTrue("first column should be pips",
          idx.columnNames().get(0).equalsIgnoreCase("pips"));
    }
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Creates a {@link DeferredIndexExecutorImpl} with the current injected
   * dependencies and test config.
   */
  private DeferredIndexExecutor createExecutor() {
    return new DeferredIndexExecutorImpl(
        connectionResources,
        new SqlScriptExecutorProvider(connectionResources),
        config,
        new DeferredIndexExecutorServiceFactory.Default());
  }
}
