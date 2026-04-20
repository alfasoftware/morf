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
 * Tracks deferred-index operations during a single upgrade session and
 * produces the DSL DML statements
 * ({@link InsertStatement}, {@link UpdateStatement}, {@link DeleteStatement})
 * needed to keep the DeployedIndexes table in sync with schema changes.
 *
 * <p><b>Slim invariant</b> (this branch): the service tracks only
 * <em>deferred</em> indexes — non-deferred indexes live only in the physical
 * DB. The visitor gates {@link #trackIndex(String, Index)} calls on
 * {@code isDeferred()}.</p>
 *
 * <p>This service is stateful and scoped to one upgrade run. A fresh
 * instance must be created for each upgrade execution. At the start of
 * each session the enricher
 * {@link DeployedIndexesModelEnricher#enrich(org.alfasoftware.morf.metadata.Schema)}
 * calls {@link #prime(DeployedIndex)} for every persisted deferred row so
 * that subsequent {@link #removeIndex(String, String)} / rename / column
 * operations correctly produce DML against previously-persisted rows.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeployedIndexesService {

  /**
   * Seeds the in-session state with a persisted tracking row WITHOUT
   * emitting any DML. Called by the enricher at the start of an upgrade
   * session for every row already in the DeployedIndexes table — so that
   * subsequent {@link #removeIndex}, {@link #updateIndexName},
   * {@link #updateColumnName}, etc. can correctly identify and emit DML for
   * rows persisted by earlier upgrades.
   *
   * @param entry the persisted row to seed.
   */
  void prime(DeployedIndex entry);


  /**
   * Records a deferred index in the service and returns the INSERT
   * statement that adds it to the DeployedIndexes table. Non-deferred
   * indexes are not tracked in the slim model — the visitor gates calls
   * on {@code index.isDeferred()}.
   *
   * @param tableName the table the index belongs to.
   * @param index the index metadata (must be {@code isDeferred()=true}).
   * @return INSERT statements to be executed by the caller.
   */
  List<InsertStatement> trackIndex(String tableName, Index index);


  /**
   * Returns {@code true} if an index is currently tracked for the given
   * table and index name (case-insensitive).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return true if tracked.
   */
  boolean isTracked(String tableName, String indexName);


  /**
   * Returns {@code true} if the tracked index is deferred.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return true if tracked and deferred.
   */
  boolean isTrackedDeferred(String tableName, String indexName);


  /**
   * Removes an index from tracking and returns DELETE statements.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return DELETE statements, or empty if not tracked.
   */
  List<DeleteStatement> removeIndex(String tableName, String indexName);


  /**
   * Removes all tracked indexes for a table and returns DELETE statements.
   *
   * @param tableName the table name.
   * @return DELETE statements, or empty if no indexes tracked for that table.
   */
  List<DeleteStatement> removeAllForTable(String tableName);


  /**
   * Removes tracked indexes that reference the given column and returns DELETE statements.
   *
   * @param tableName the table name.
   * @param columnName the column name being removed.
   * @return DELETE statements for affected indexes.
   */
  List<DeleteStatement> removeIndexesReferencingColumn(String tableName, String columnName);


  /**
   * Updates the table name for all tracked indexes on the old table.
   *
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE statements.
   */
  List<UpdateStatement> updateTableName(String oldTableName, String newTableName);


  /**
   * Updates column references in tracked indexes when a column is renamed.
   *
   * @param tableName the table name.
   * @param oldColumnName the old column name.
   * @param newColumnName the new column name.
   * @return UPDATE statements for affected indexes.
   */
  List<UpdateStatement> updateColumnName(String tableName, String oldColumnName, String newColumnName);


  /**
   * Updates the index name for a tracked index.
   *
   * @param tableName the table name.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE statements.
   */
  List<UpdateStatement> updateIndexName(String tableName, String oldIndexName, String newIndexName);
}
