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
import org.alfasoftware.morf.sql.Statement;

/**
 * Tracks ALL index operations (deferred and non-deferred) during a single
 * upgrade session and produces the DSL {@link Statement}s needed to keep
 * the DeployedIndexes table in sync with schema changes.
 *
 * <p>This service is stateful and scoped to one upgrade run. A fresh
 * instance must be created for each upgrade execution.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeployedIndexesChangeService {

  /**
   * Records an index in the service and returns the INSERT statement
   * that adds it to the DeployedIndexes table.
   *
   * @param tableName the table the index belongs to.
   * @param index the index metadata.
   * @param upgradeUUID UUID of the upgrade step, or null.
   * @return INSERT statements to be executed by the caller.
   */
  List<Statement> trackIndex(String tableName, Index index, String upgradeUUID);


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
  List<Statement> removeIndex(String tableName, String indexName);


  /**
   * Removes all tracked indexes for a table and returns DELETE statements.
   *
   * @param tableName the table name.
   * @return DELETE statements, or empty if no indexes tracked for that table.
   */
  List<Statement> removeAllForTable(String tableName);


  /**
   * Removes tracked indexes that reference the given column and returns DELETE statements.
   *
   * @param tableName the table name.
   * @param columnName the column name being removed.
   * @return DELETE statements for affected indexes.
   */
  List<Statement> removeIndexesReferencingColumn(String tableName, String columnName);


  /**
   * Updates the table name for all tracked indexes on the old table.
   *
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE statements.
   */
  List<Statement> updateTableName(String oldTableName, String newTableName);


  /**
   * Updates column references in tracked indexes when a column is renamed.
   *
   * @param tableName the table name.
   * @param oldColumnName the old column name.
   * @param newColumnName the new column name.
   * @return UPDATE statements for affected indexes.
   */
  List<Statement> updateColumnName(String tableName, String oldColumnName, String newColumnName);


  /**
   * Updates the index name for a tracked index.
   *
   * @param tableName the table name.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE statements.
   */
  List<Statement> updateIndexName(String tableName, String oldIndexName, String newIndexName);
}
