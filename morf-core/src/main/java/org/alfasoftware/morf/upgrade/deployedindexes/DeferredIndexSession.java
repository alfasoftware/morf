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

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.UpdateStatement;

/**
 * Per-upgrade-session journal for deferred-index tracking. Each mutation
 * method records the change in an in-memory cache and returns the DSL
 * DML statements the visitor should emit alongside its physical DDL to
 * keep the {@code DeployedIndexes} tracking table in sync.
 *
 * <p><b>Slim invariant</b>: only deferred indexes are tracked — callers
 * (the visitor) gate {@link #trackIndex(String, Index)} on the index's
 * effective {@code isDeferred()} after dialect-support normalization.</p>
 *
 * <p><b>Lifecycle</b>: instances are per-upgrade. At session start the
 * enricher calls {@link #prime(DeployedIndex)} for every persisted row so
 * that subsequent {@code removeIndex / updateIndexName / updateColumnName}
 * etc. produce correct DML against rows persisted by earlier upgrades.</p>
 *
 * <p>Separate from {@link DeployedIndexTracker} because the two have
 * fundamentally different shapes: this interface returns DSL statements
 * for batched emission during an upgrade; the tracker executes
 * JDBC directly at application runtime.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeferredIndexSession {

  /**
   * Seeds the in-session cache with a persisted tracking row WITHOUT
   * emitting any DML. Called by the enricher at session start.
   *
   * @param entry the persisted row.
   */
  void prime(DeployedIndex entry);


  /**
   * Records a deferred index and returns the INSERT DML. Under the slim
   * invariant callers only invoke this for effective-deferred indexes.
   *
   * @param tableName the table.
   * @param index the index (must be {@code isDeferred()=true}).
   * @return INSERT statements for the visitor to emit.
   */
  List<InsertStatement> trackIndex(String tableName, Index index);


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return {@code true} if the index is currently tracked as deferred.
   */
  boolean isTrackedDeferred(String tableName, String indexName);


  /**
   * Removes an index from tracking and returns the DELETE.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return DELETE statements, empty if not tracked.
   */
  List<DeleteStatement> removeIndex(String tableName, String indexName);


  /**
   * Removes every tracked index for a table.
   *
   * @param tableName the table.
   * @return DELETE statements, empty if no tracked indexes for the table.
   */
  List<DeleteStatement> removeAllForTable(String tableName);


  /**
   * Removes every tracked index that references the named column.
   *
   * @param tableName the table.
   * @param columnName the column being removed.
   * @return DELETE statements for each affected index.
   */
  List<DeleteStatement> removeIndexesReferencingColumn(String tableName, String columnName);


  /**
   * Re-homes every tracked index from one table name to another.
   *
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE statements.
   */
  List<UpdateStatement> updateTableName(String oldTableName, String newTableName);


  /**
   * Updates column references on every tracked index that mentions the
   * renamed column.
   *
   * @param tableName the table.
   * @param oldColumnName the old column name.
   * @param newColumnName the new column name.
   * @return UPDATE statements, one per affected index.
   */
  List<UpdateStatement> updateColumnName(String tableName, String oldColumnName, String newColumnName);


  /**
   * Renames a tracked index.
   *
   * @param tableName the table.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE statements, empty if not tracked.
   */
  List<UpdateStatement> updateIndexName(String tableName, String oldIndexName, String newIndexName);
}
