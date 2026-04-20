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

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.ImplementedBy;

/**
 * Single source of DSL construction for every statement that targets the
 * {@code DeployedIndexes} table — reads, status updates, and the tracking
 * DML used by the visitor. Owns the column-name constants. The
 * {@link DeployedIndexesDAO} executes these statements; the
 * {@link DeployedIndexesService} orchestrates them into
 * visitor-usable lists. Neither builds DSL of its own.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeployedIndexesStatementFactoryImpl.class)
public interface DeployedIndexesStatementFactory {

  /** Table name — the DeployedIndexes tracking table. */
  String TABLE = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;

  /** Column: primary key. */
  String COL_ID = "id";
  /** Column: table the tracked index belongs to. */
  String COL_TABLE_NAME = "tableName";
  /** Column: index name. */
  String COL_INDEX_NAME = "indexName";
  /** Column: whether the index is unique. */
  String COL_INDEX_UNIQUE = "indexUnique";
  /** Column: comma-separated column list the index covers. */
  String COL_INDEX_COLUMNS = "indexColumns";
  /** Column: lifecycle status (PENDING/IN_PROGRESS/COMPLETED/FAILED). */
  String COL_STATUS = "status";
  /** Column: retry count for failed deferred builds. */
  String COL_RETRY_COUNT = "retryCount";
  /** Column: epoch ms when the tracking row was created. */
  String COL_CREATED_TIME = "createdTime";
  /** Column: epoch ms when the app started building this deferred index. */
  String COL_STARTED_TIME = "startedTime";
  /** Column: epoch ms when the app finished building this deferred index. */
  String COL_COMPLETED_TIME = "completedTime";
  /** Column: failure message for FAILED builds. */
  String COL_ERROR_MESSAGE = "errorMessage";


  // -------------------------------------------------------------------------
  // Read queries
  // -------------------------------------------------------------------------

  /**
   * @return SELECT all rows, ordered by id.
   */
  SelectStatement statementToFindAll();


  /**
   * @param tableName filter to this table.
   * @return SELECT rows for {@code tableName}, ordered by id.
   */
  SelectStatement statementToFindByTable(String tableName);


  /**
   * @return SELECT rows whose status is not terminal (PENDING/IN_PROGRESS/FAILED),
   *     ordered by id.
   */
  SelectStatement statementToFindNonTerminalOperations();


  /**
   * @return SELECT status column alone (the DAO aggregates into a status-&gt;count map).
   */
  SelectStatement statementToSelectStatusColumn();


  // -------------------------------------------------------------------------
  // Status update statements (run directly by the DAO)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @param startedTime epoch ms when the app started building this deferred index.
   * @return UPDATE flipping status to IN_PROGRESS and setting {@code startedTime}.
   */
  UpdateStatement statementToMarkStarted(String tableName, String indexName, long startedTime);


  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @param completedTime epoch ms when the app finished building this deferred index.
   * @return UPDATE flipping status to COMPLETED and setting {@code completedTime}.
   */
  UpdateStatement statementToMarkCompleted(String tableName, String indexName, long completedTime);


  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @param errorMessage the failure message.
   * @return UPDATE flipping status to FAILED and setting {@code errorMessage}.
   */
  UpdateStatement statementToMarkFailed(String tableName, String indexName, String errorMessage);


  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @return UPDATE bumping retry count to 1 (simplified — Morf DSL doesn't
   *     support {@code field + 1}; the app manages retry counts).
   */
  UpdateStatement statementToBumpRetryCount(String tableName, String indexName);


  /**
   * @return UPDATE flipping every IN_PROGRESS row back to PENDING.
   */
  UpdateStatement statementToResetInProgress();


  // -------------------------------------------------------------------------
  // Tracking DML (run as part of the upgrade script via the visitor)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the target table.
   * @param index the index metadata (must be deferred under the slim invariant).
   * @return INSERT adding a new tracking row for {@code index} on {@code tableName}
   *     with status PENDING.
   */
  InsertStatement statementToTrackIndex(String tableName, Index index);


  /**
   * @param tableName the table the index belongs to.
   * @param indexName the index name.
   * @return DELETE removing the tracking row for one (table, index).
   */
  DeleteStatement statementToRemoveIndex(String tableName, String indexName);


  /**
   * @param tableName the table to scope the delete to.
   * @return DELETE removing all tracking rows for {@code tableName}.
   */
  DeleteStatement statementToRemoveAllForTable(String tableName);


  /**
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE renaming the {@code tableName} column for every tracking
   *     row that currently has {@code oldTableName}.
   */
  UpdateStatement statementToUpdateTableName(String oldTableName, String newTableName);


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param newColumnsCsv the new column list as a CSV string.
   * @return UPDATE replacing the {@code indexColumns} CSV for one (table,
   *     index).
   */
  UpdateStatement statementToUpdateIndexColumns(String tableName, String indexName, String newColumnsCsv);


  /**
   * @param tableName the table.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE renaming the index in its tracking row.
   */
  UpdateStatement statementToUpdateIndexName(String tableName, String oldIndexName, String newIndexName);
}
