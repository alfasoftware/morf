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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;

/**
 * Shared constants and assertion helpers for deferred index integration tests.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
final class DeferredIndexTestSupport {

  private DeferredIndexTestSupport() {}


  /** Standard initial schema used across deferred index integration tests. */
  static final Schema INITIAL_SCHEMA = schema(
      deployedViewsTable(),
      upgradeAuditTable(),
      table("Product").columns(
          column("id", DataType.BIG_INTEGER).primaryKey(),
          column("name", DataType.STRING, 100)
      )
  );


  /** Builds a target schema with a single index on Product.name. */
  static Schema schemaWithIndex(String indexName, String... columns) {
    return schema(
        deployedViewsTable(), upgradeAuditTable(),
        table("Product").columns(
            column("id", DataType.BIG_INTEGER).primaryKey(),
            column("name", DataType.STRING, 100)
        ).indexes(index(indexName).columns(columns))
    );
  }


  /** Creates a fresh executor with default test settings. */
  static DeferredIndexExecutor createExecutor(ConnectionResources connectionResources,
                                               SqlScriptExecutorProvider sqlScriptExecutorProvider) {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setDeferredIndexMaxRetries(0);
    return new DeferredIndexExecutorImpl(
        connectionResources, sqlScriptExecutorProvider,
        config, new DeferredIndexExecutorServiceFactory.Default());
  }


  /** Creates a fresh service with default test settings. */
  static DeferredIndexService createService(ConnectionResources connectionResources,
                                             SqlScriptExecutorProvider sqlScriptExecutorProvider) {
    return new DeferredIndexServiceImpl(createExecutor(connectionResources, sqlScriptExecutorProvider));
  }


  /** Runs the executor synchronously, blocking until all deferred indexes are built. */
  static void executeDeferred(ConnectionResources connectionResources,
                               SqlScriptExecutorProvider sqlScriptExecutorProvider) {
    createExecutor(connectionResources, sqlScriptExecutorProvider).execute().join();
  }


  /** Asserts that a physical index exists in the database schema. */
  static void assertPhysicalIndexExists(ConnectionResources connectionResources,
                                         String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Physical index " + indexName + " should exist on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }


  /** Asserts that a deferred index exists with isDeferred()=true. */
  static void assertDeferredIndexPending(ConnectionResources connectionResources,
                                          String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertTrue("Deferred index " + indexName + " should be present with isDeferred()=true on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName()) && idx.isDeferred()));
    }
  }


  /** Asserts that an index does not exist at all in the schema. */
  static void assertIndexNotPresent(ConnectionResources connectionResources,
                                     String tableName, String indexName) {
    try (SchemaResource sr = connectionResources.openSchemaResource()) {
      assertFalse("Index " + indexName + " should not be present on " + tableName,
          sr.getTable(tableName).indexes().stream()
              .anyMatch(idx -> indexName.equalsIgnoreCase(idx.getName())));
    }
  }
}
