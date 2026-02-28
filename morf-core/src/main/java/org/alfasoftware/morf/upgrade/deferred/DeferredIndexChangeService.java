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

import java.util.List;

import org.alfasoftware.morf.sql.Statement;

/**
 * Tracks pending deferred ADD INDEX operations within a single upgrade session
 * and produces the DSL {@link Statement}s needed to cancel or rename those
 * operations in the queue when subsequent schema changes affect them.
 *
 * <p>This service is stateful and scoped to one upgrade run. A fresh instance
 * must be created for each upgrade execution.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeferredIndexChangeService {

  /**
   * Records a deferred ADD INDEX operation in the service and returns the
   * INSERT {@link Statement}s that enqueue it in the database
   * ({@code DeferredIndexOperation} row plus one {@code DeferredIndexOperationColumn}
   * row per index column).
   *
   * @param deferredAddIndex the operation to enqueue.
   * @return INSERT statements to be executed by the caller.
   */
  List<Statement> trackPending(DeferredAddIndex deferredAddIndex);


  /**
   * Returns {@code true} if a PENDING deferred ADD INDEX is currently tracked
   * for the given table and index (case-insensitive comparison).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return {@code true} if a pending deferred ADD is tracked.
   */
  boolean hasPendingDeferred(String tableName, String indexName);


  /**
   * Produces DELETE {@link Statement}s to cancel the tracked PENDING operation
   * for the given table/index, and removes it from tracking. Returns an empty
   * list if no such operation is tracked.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return DELETE statements to execute, or an empty list.
   */
  List<Statement> cancelPending(String tableName, String indexName);


  /**
   * Produces DELETE {@link Statement}s to cancel all tracked PENDING operations
   * for the given table, and removes them from tracking. Returns an empty list
   * if no operations are tracked for the table.
   *
   * @param tableName the table name.
   * @return DELETE statements to execute, or an empty list.
   */
  List<Statement> cancelAllPendingForTable(String tableName);


  /**
   * Produces DELETE {@link Statement}s to cancel all tracked PENDING operations
   * for the given table whose column list includes {@code columnName}, and removes
   * them from tracking. Returns an empty list if no matching operations are tracked.
   *
   * @param tableName  the table name.
   * @param columnName the column name.
   * @return DELETE statements to execute, or an empty list.
   */
  List<Statement> cancelPendingReferencingColumn(String tableName, String columnName);


  /**
   * Produces an UPDATE {@link Statement} to rename {@code oldTableName} to
   * {@code newTableName} in tracked PENDING rows, and updates internal tracking.
   * Returns an empty list if no operations are tracked for the old table name.
   *
   * @param oldTableName the current table name.
   * @param newTableName the new table name.
   * @return UPDATE statement to execute, or an empty list.
   */
  List<Statement> updatePendingTableName(String oldTableName, String newTableName);


  /**
   * Produces an UPDATE {@link Statement} to rename {@code oldColumnName} to
   * {@code newColumnName} in tracked PENDING column rows for the given table,
   * for any deferred index that references the column. Returns an empty list if
   * no matching operations are tracked.
   *
   * @param tableName     the table name.
   * @param oldColumnName the current column name.
   * @param newColumnName the new column name.
   * @return UPDATE statement to execute, or an empty list.
   */
  List<Statement> updatePendingColumnName(String tableName, String oldColumnName, String newColumnName);
}
