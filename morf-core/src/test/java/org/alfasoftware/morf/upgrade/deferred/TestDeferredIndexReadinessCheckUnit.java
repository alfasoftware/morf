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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexReadinessCheckImpl} covering the
 * {@link DeferredIndexReadinessCheck#forceBuildAllPending(Collection)},
 * {@link DeferredIndexReadinessCheck#augmentSchemaWithPendingIndexes(Schema, Collection)},
 * and {@link DeferredIndexReadinessCheck#findMissingDeferredIndexes(Collection)}
 * methods with mocked executor, config, and connection dependencies.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexReadinessCheckUnit {

  private ConnectionResources connectionResources;
  private SchemaResource schemaResource;
  private DeferredIndexExecutor mockExecutor;
  private DeferredIndexExecutionConfig config;


  /** Set up mock connections and default config. */
  @Before
  public void setUp() {
    schemaResource = mock(SchemaResource.class);
    connectionResources = mock(ConnectionResources.class);
    when(connectionResources.openSchemaResource()).thenReturn(schemaResource);
    mockExecutor = mock(DeferredIndexExecutor.class);
    config = new DeferredIndexExecutionConfig();
  }


  /** forceBuildAllPending() should not call executor when replay finds no deferred indexes. */
  @Test
  public void testRunWithEmptyQueue() {
    configureLiveDbWithTable("TestTable",
        table("TestTable").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("TestTable_1").columns("id", "name")
        ));

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    check.forceBuildAllPending(stepsWithIndex());

    verify(mockExecutor, never()).execute(anyList());
  }


  /** forceBuildAllPending() should execute missing indexes and succeed when executor completes. */
  @Test
  public void testRunExecutesPendingOperationsSuccessfully() {
    configureLiveDbWithTableNoIndex();
    when(mockExecutor.execute(anyList())).thenReturn(CompletableFuture.completedFuture(null));

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    check.forceBuildAllPending(stepsWithIndex());

    verify(mockExecutor).execute(anyList());
  }


  /** forceBuildAllPending() should throw IllegalStateException when executor reports failures. */
  @Test
  public void testRunThrowsWhenOperationsFail() {
    configureLiveDbWithTableNoIndex();

    DeferredIndexExecutorImpl realStyleExecutor = mock(DeferredIndexExecutorImpl.class);
    when(realStyleExecutor.execute(anyList())).thenReturn(CompletableFuture.completedFuture(null));
    when(realStyleExecutor.getFailedCount()).thenReturn(1);

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(realStyleExecutor, config, connectionResources);
    try {
      check.forceBuildAllPending(stepsWithIndex());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue("Message should mention failed count", e.getMessage().contains("1"));
    }
  }


  /** augmentSchemaWithPendingIndexes() should add missing deferred index to schema. */
  @Test
  public void testAugmentAddsIndex() {
    configureLiveDbWithTableNoIndex();

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    Schema input = schema(table("TestTable").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("name", DataType.STRING, 100)
    ));

    Schema result = check.augmentSchemaWithPendingIndexes(input, stepsWithIndex());
    assertTrue("Index should be added",
        result.getTable("TestTable").indexes().stream()
            .anyMatch(idx -> "TestTable_1".equalsIgnoreCase(idx.getName())));
  }


  /** augmentSchemaWithPendingIndexes() should skip index when table does not exist in schema. */
  @Test
  public void testAugmentSkipsOpForMissingTable() {
    // Live DB has no table at all
    when(schemaResource.tableExists("TestTable")).thenReturn(false);

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    Schema input = schema(table("OtherTable").columns(
        column("id", DataType.BIG_INTEGER).primaryKey()
    ));

    Schema result = check.augmentSchemaWithPendingIndexes(input, stepsWithIndex());
    assertTrue("OtherTable should still exist", result.tableExists("OtherTable"));
    assertEquals("No indexes should be added to OtherTable", 0, result.getTable("OtherTable").indexes().size());
  }


  /** augmentSchemaWithPendingIndexes() should skip index when it already exists in schema. */
  @Test
  public void testAugmentSkipsExistingIndex() {
    configureLiveDbWithTable("TestTable",
        table("TestTable").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(
            index("TestTable_1").columns("id", "name")
        ));

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    Schema input = schema(table("TestTable").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("name", DataType.STRING, 100)
    ).indexes(
        index("TestTable_1").columns("id", "name")
    ));

    Schema result = check.augmentSchemaWithPendingIndexes(input, stepsWithIndex());
    long indexCount = result.getTable("TestTable").indexes().stream()
        .filter(idx -> "TestTable_1".equalsIgnoreCase(idx.getName()))
        .count();
    assertEquals("Should not duplicate existing index", 1, indexCount);
  }


  /** augmentSchemaWithPendingIndexes() should return unchanged schema when no deferred indexes exist. */
  @Test
  public void testAugmentReturnsUnchangedWhenNoOps() {
    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    Schema result = check.augmentSchemaWithPendingIndexes(input, Collections.emptyList());
    assertSame("Should return input schema unchanged", input, result);
  }


  /** forceBuildAllPending() should not call executor when upgrade steps collection is empty. */
  @Test
  public void testRunSkipsWhenNoSteps() {
    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(mockExecutor, config, connectionResources);
    check.forceBuildAllPending(Collections.emptyList());

    verify(mockExecutor, never()).execute(anyList());
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Returns a collection containing the single {@link AddDeferredIndexStep}.
   */
  private Collection<Class<? extends UpgradeStep>> stepsWithIndex() {
    return List.of(AddDeferredIndexStep.class);
  }


  /**
   * Configures the mock live DB to have the given table with no deferred index built.
   */
  private void configureLiveDbWithTableNoIndex() {
    Table dbTable = table("TestTable").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("name", DataType.STRING, 100));
    when(schemaResource.tableExists("TestTable")).thenReturn(true);
    when(schemaResource.getTable("TestTable")).thenReturn(dbTable);
  }


  /**
   * Configures the mock live DB to have a specific table definition.
   */
  private void configureLiveDbWithTable(String tableName, Table dbTable) {
    when(schemaResource.tableExists(tableName)).thenReturn(true);
    when(schemaResource.getTable(tableName)).thenReturn(dbTable);
  }


  // -------------------------------------------------------------------------
  // Inner UpgradeStep classes for replay
  // -------------------------------------------------------------------------

  /**
   * Test upgrade step that adds a table then a deferred index on it.
   */
  @Sequence(1000)
  @UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
  public static class AddDeferredIndexStep implements UpgradeStep {
    @Override
    public String getJiraId() {
      return "TEST-1";
    }

    @Override
    public String getDescription() {
      return "Add deferred index";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(
          table("TestTable").columns(
              column("id", DataType.BIG_INTEGER).primaryKey(),
              column("name", DataType.STRING, 100)));
      schema.addIndexDeferred("TestTable", index("TestTable_1").columns("id", "name"));
    }
  }
}
