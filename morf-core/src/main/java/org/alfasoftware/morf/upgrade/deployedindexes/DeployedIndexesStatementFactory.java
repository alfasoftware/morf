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

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import java.util.List;
import java.util.UUID;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.Singleton;

/**
 * Single source of DSL construction for every statement that targets the
 * {@code DeployedIndexes} table — reads, status updates, and the tracking
 * DML used by the visitor. Owns the column-name constants. The
 * {@link DeployedIndexesDAO} executes these statements; the
 * {@link DeployedIndexesChangeService} orchestrates them into
 * visitor-usable lists. Neither builds DSL of its own.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeployedIndexesStatementFactory {

  static final String TABLE = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;

  static final String COL_ID = "id";
  static final String COL_TABLE_NAME = "tableName";
  static final String COL_INDEX_NAME = "indexName";
  static final String COL_INDEX_UNIQUE = "indexUnique";
  static final String COL_INDEX_COLUMNS = "indexColumns";
  static final String COL_INDEX_DEFERRED = "indexDeferred";
  static final String COL_STATUS = "status";
  static final String COL_RETRY_COUNT = "retryCount";
  static final String COL_CREATED_TIME = "createdTime";
  static final String COL_STARTED_TIME = "startedTime";
  static final String COL_COMPLETED_TIME = "completedTime";
  static final String COL_ERROR_MESSAGE = "errorMessage";


  // -------------------------------------------------------------------------
  // Read queries
  // -------------------------------------------------------------------------

  /**
   * @return SELECT all rows, ordered by id.
   */
  public SelectStatement statementToFindAll() {
    return selectAllColumns().orderBy(field(COL_ID));
  }


  /**
   * @param tableName filter to this table.
   * @return SELECT rows for {@code tableName}, ordered by id.
   */
  public SelectStatement statementToFindByTable(String tableName) {
    return selectAllColumns()
        .where(field(COL_TABLE_NAME).eq(tableName))
        .orderBy(field(COL_ID));
  }


  /**
   * @return SELECT rows whose status is not terminal (PENDING/IN_PROGRESS/FAILED),
   *     ordered by id.
   */
  public SelectStatement statementToFindNonTerminalOperations() {
    return selectAllColumns()
        .where(or(
            field(COL_STATUS).eq(DeployedIndexStatus.PENDING.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.FAILED.name())))
        .orderBy(field(COL_ID));
  }


  /**
   * @return SELECT status column alone (the DAO aggregates into a status-&gt;count map).
   */
  public SelectStatement statementToSelectStatusColumn() {
    return org.alfasoftware.morf.sql.SqlUtils.select(field(COL_STATUS))
        .from(tableRef(TABLE));
  }


  // -------------------------------------------------------------------------
  // Status update statements (run directly by the DAO)
  // -------------------------------------------------------------------------

  /**
   * @return UPDATE flipping status to IN_PROGRESS and setting {@code startedTime}.
   */
  public UpdateStatement statementToMarkStarted(String tableName, String indexName, long startedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
             literal(startedTime).as(COL_STARTED_TIME))
        .where(field(COL_TABLE_NAME).eq(tableName)
            .and(field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @return UPDATE flipping status to COMPLETED and setting {@code completedTime}.
   */
  public UpdateStatement statementToMarkCompleted(String tableName, String indexName, long completedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.COMPLETED.name()).as(COL_STATUS),
             literal(completedTime).as(COL_COMPLETED_TIME))
        .where(field(COL_TABLE_NAME).eq(tableName)
            .and(field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @return UPDATE flipping status to FAILED and setting {@code errorMessage}.
   */
  public UpdateStatement statementToMarkFailed(String tableName, String indexName, String errorMessage) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.FAILED.name()).as(COL_STATUS),
             literal(errorMessage).as(COL_ERROR_MESSAGE))
        .where(field(COL_TABLE_NAME).eq(tableName)
            .and(field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @return UPDATE bumping retry count to 1 (simplified — Morf DSL doesn't
   *     support {@code field + 1}; the app manages retry counts).
   */
  public UpdateStatement statementToBumpRetryCount(String tableName, String indexName) {
    return update(tableRef(TABLE))
        .set(literal(1).as(COL_RETRY_COUNT))
        .where(field(COL_TABLE_NAME).eq(tableName)
            .and(field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @return UPDATE flipping every IN_PROGRESS row back to PENDING.
   */
  public UpdateStatement statementToResetInProgress() {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.PENDING.name()).as(COL_STATUS))
        .where(field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()));
  }


  // -------------------------------------------------------------------------
  // Tracking DML (run as part of the upgrade script via the visitor)
  // -------------------------------------------------------------------------

  /**
   * @return INSERT adding a new tracking row for {@code index} on {@code tableName}.
   *     Non-deferred indexes go in as COMPLETED; deferred indexes as PENDING.
   */
  public InsertStatement statementToTrackIndex(String tableName, Index index) {
    long operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    long createdTime = System.currentTimeMillis();
    String status = index.isDeferred()
        ? DeployedIndexStatus.PENDING.name()
        : DeployedIndexStatus.COMPLETED.name();

    return insert().into(tableRef(TABLE))
        .values(
            literal(operationId).as(COL_ID),
            literal(tableName).as(COL_TABLE_NAME),
            literal(index.getName()).as(COL_INDEX_NAME),
            literal(index.isUnique()).as(COL_INDEX_UNIQUE),
            literal(String.join(",", index.columnNames())).as(COL_INDEX_COLUMNS),
            literal(index.isDeferred()).as(COL_INDEX_DEFERRED),
            literal(status).as(COL_STATUS),
            literal(0).as(COL_RETRY_COUNT),
            literal(createdTime).as(COL_CREATED_TIME)
        );
  }


  /**
   * @return DELETE removing the tracking row for one (table, index).
   */
  public DeleteStatement statementToRemoveIndex(String tableName, String indexName) {
    return delete(tableRef(TABLE))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  /**
   * @return DELETE removing all tracking rows for {@code tableName}.
   */
  public DeleteStatement statementToRemoveAllForTable(String tableName) {
    return delete(tableRef(TABLE)).where(field(COL_TABLE_NAME).eq(literal(tableName)));
  }


  /**
   * @return UPDATE renaming the {@code tableName} column for every tracking
   *     row that currently has {@code oldTableName}.
   */
  public UpdateStatement statementToUpdateTableName(String oldTableName, String newTableName) {
    return update(tableRef(TABLE))
        .set(literal(newTableName).as(COL_TABLE_NAME))
        .where(field(COL_TABLE_NAME).eq(literal(oldTableName)));
  }


  /**
   * @return UPDATE replacing the {@code indexColumns} CSV for one (table,
   *     index). The caller supplies the new column list as a CSV string.
   */
  public UpdateStatement statementToUpdateIndexColumns(String tableName, String indexName, String newColumnsCsv) {
    return update(tableRef(TABLE))
        .set(literal(newColumnsCsv).as(COL_INDEX_COLUMNS))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  /**
   * @return UPDATE renaming the index in its tracking row.
   */
  public UpdateStatement statementToUpdateIndexName(String tableName, String oldIndexName, String newIndexName) {
    return update(tableRef(TABLE))
        .set(literal(newIndexName).as(COL_INDEX_NAME))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(oldIndexName))));
  }


  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private SelectStatement selectAllColumns() {
    return org.alfasoftware.morf.sql.SqlUtils.select(
        field(COL_ID), field(COL_TABLE_NAME),
        field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
        field(COL_INDEX_DEFERRED), field(COL_STATUS), field(COL_RETRY_COUNT),
        field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
        field(COL_ERROR_MESSAGE))
        .from(tableRef(TABLE));
  }


  /**
   * @return all projection columns as a fixed list — useful for tests asserting
   *     the mapping order.
   */
  public static List<String> allColumns() {
    return List.of(
        COL_ID, COL_TABLE_NAME, COL_INDEX_NAME, COL_INDEX_UNIQUE, COL_INDEX_COLUMNS,
        COL_INDEX_DEFERRED, COL_STATUS, COL_RETRY_COUNT, COL_CREATED_TIME,
        COL_STARTED_TIME, COL_COMPLETED_TIME, COL_ERROR_MESSAGE);
  }
}
