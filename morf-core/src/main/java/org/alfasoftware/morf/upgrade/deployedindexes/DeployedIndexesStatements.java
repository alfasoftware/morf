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
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.or;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Package-private utility holding the DeployedIndexes column names, every
 * DSL statement that targets the table, and the ResultSet → DeployedIndex
 * mapping. Pure static — no instances, no state.
 *
 * <p>Replaces the previous {@code DeployedIndexesStatementFactory} +
 * {@code Impl} pair: since the factory had no dependencies and every
 * caller holds a fresh reference, an interface + DI layer added no value.
 * Keeping it private to the {@code deployedindexes} package prevents
 * adopters from depending on column names or statement shape.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
final class DeployedIndexesStatements {

  /** Table name — the DeployedIndexes tracking table. */
  static final String TABLE = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;

  /** Column: primary key. */
  static final String COL_ID = "id";
  /** Column: table the tracked index belongs to. */
  static final String COL_TABLE_NAME = "tableName";
  /** Column: index name. */
  static final String COL_INDEX_NAME = "indexName";
  /** Column: whether the index is unique. */
  static final String COL_INDEX_UNIQUE = "indexUnique";
  /** Column: comma-separated column list the index covers. */
  static final String COL_INDEX_COLUMNS = "indexColumns";
  /** Column: lifecycle status (PENDING/IN_PROGRESS/COMPLETED/FAILED). */
  static final String COL_STATUS = "status";
  /** Column: retry count for failed deferred builds. */
  static final String COL_RETRY_COUNT = "retryCount";
  /** Column: epoch ms when the tracking row was created. */
  static final String COL_CREATED_TIME = "createdTime";
  /** Column: epoch ms when the app started building this deferred index. */
  static final String COL_STARTED_TIME = "startedTime";
  /** Column: epoch ms when the app finished building this deferred index. */
  static final String COL_COMPLETED_TIME = "completedTime";
  /** Column: failure message for FAILED builds. */
  static final String COL_ERROR_MESSAGE = "errorMessage";


  private DeployedIndexesStatements() {
    // no instances
  }


  // -------------------------------------------------------------------------
  // Read queries
  // -------------------------------------------------------------------------

  /** @return SELECT all rows, ordered by id. */
  static SelectStatement selectAll() {
    return selectAllColumns().orderBy(field(COL_ID));
  }


  /** @return SELECT rows whose status is non-terminal (PENDING/IN_PROGRESS/FAILED). */
  static SelectStatement selectNonTerminal() {
    return selectAllColumns()
        .where(or(
            field(COL_STATUS).eq(DeployedIndexStatus.PENDING.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.FAILED.name())))
        .orderBy(field(COL_ID));
  }


  /** @return SELECT status column alone (caller aggregates into status → count). */
  static SelectStatement selectStatusColumn() {
    return select(field(COL_STATUS)).from(tableRef(TABLE));
  }


  // -------------------------------------------------------------------------
  // Status update statements (executed directly at runtime by the tracker)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param startedTime epoch ms.
   * @return UPDATE flipping status to IN_PROGRESS and setting startedTime.
   */
  static UpdateStatement markStarted(String tableName, String indexName, long startedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
             literal(startedTime).as(COL_STARTED_TIME))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param completedTime epoch ms.
   * @return UPDATE flipping status to COMPLETED and setting completedTime.
   */
  static UpdateStatement markCompleted(String tableName, String indexName, long completedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.COMPLETED.name()).as(COL_STATUS),
             literal(completedTime).as(COL_COMPLETED_TIME))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param errorMessage the failure message.
   * @return UPDATE flipping status to FAILED and setting errorMessage.
   */
  static UpdateStatement markFailed(String tableName, String indexName, String errorMessage) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.FAILED.name()).as(COL_STATUS),
             literal(errorMessage).as(COL_ERROR_MESSAGE))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return UPDATE bumping retry count to 1 (simplified — the DSL doesn't
   *     support field + 1; the adopter manages retry counts).
   */
  static UpdateStatement bumpRetryCount(String tableName, String indexName) {
    return update(tableRef(TABLE))
        .set(literal(1).as(COL_RETRY_COUNT))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /** @return UPDATE flipping every IN_PROGRESS row back to PENDING. */
  static UpdateStatement resetInProgress() {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.PENDING.name()).as(COL_STATUS))
        .where(field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()));
  }


  // -------------------------------------------------------------------------
  // Tracking DML (executed as part of the upgrade script via the visitor)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the table.
   * @param index the index (deferred under the slim invariant).
   * @return INSERT adding a new tracking row with status PENDING.
   */
  static InsertStatement trackIndex(String tableName, Index index) {
    long operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    long createdTime = System.currentTimeMillis();

    return insert().into(tableRef(TABLE))
        .values(
            literal(operationId).as(COL_ID),
            literal(tableName).as(COL_TABLE_NAME),
            literal(index.getName()).as(COL_INDEX_NAME),
            literal(index.isUnique()).as(COL_INDEX_UNIQUE),
            literal(String.join(",", index.columnNames())).as(COL_INDEX_COLUMNS),
            literal(DeployedIndexStatus.PENDING.name()).as(COL_STATUS),
            literal(0).as(COL_RETRY_COUNT),
            literal(createdTime).as(COL_CREATED_TIME)
        );
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return DELETE removing the tracking row.
   */
  static DeleteStatement removeIndex(String tableName, String indexName) {
    return delete(tableRef(TABLE))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  /**
   * @param tableName the table.
   * @return DELETE removing all tracking rows for the table.
   */
  static DeleteStatement removeAllForTable(String tableName) {
    return delete(tableRef(TABLE)).where(field(COL_TABLE_NAME).eq(literal(tableName)));
  }


  /**
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE renaming the tableName column for every tracking row.
   */
  static UpdateStatement updateTableName(String oldTableName, String newTableName) {
    return update(tableRef(TABLE))
        .set(literal(newTableName).as(COL_TABLE_NAME))
        .where(field(COL_TABLE_NAME).eq(literal(oldTableName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param newColumnsCsv the new column list, CSV.
   * @return UPDATE replacing the indexColumns CSV.
   */
  static UpdateStatement updateIndexColumns(String tableName, String indexName, String newColumnsCsv) {
    return update(tableRef(TABLE))
        .set(literal(newColumnsCsv).as(COL_INDEX_COLUMNS))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  /**
   * @param tableName the table.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE renaming the index in its tracking row.
   */
  static UpdateStatement updateIndexName(String tableName, String oldIndexName, String newIndexName) {
    return update(tableRef(TABLE))
        .set(literal(newIndexName).as(COL_INDEX_NAME))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(oldIndexName))));
  }


  // -------------------------------------------------------------------------
  // ResultSet mapping
  // -------------------------------------------------------------------------

  /**
   * Maps a ResultSet positioned on a DeployedIndexes row to a
   * {@link DeployedIndex}. Advances past the resultset's first row if at
   * BOF; callers typically pass in a result that already {@code rs.next()}'d.
   *
   * @param rs the result set.
   * @return the populated DeployedIndex.
   * @throws SQLException if reading fails.
   */
  static DeployedIndex mapRow(ResultSet rs) throws SQLException {
    DeployedIndex entry = new DeployedIndex();
    entry.setId(rs.getLong(COL_ID));
    entry.setTableName(rs.getString(COL_TABLE_NAME));
    entry.setIndexName(rs.getString(COL_INDEX_NAME));
    entry.setIndexUnique(rs.getBoolean(COL_INDEX_UNIQUE));
    entry.setIndexColumns(Arrays.asList(rs.getString(COL_INDEX_COLUMNS).split(",")));
    entry.setStatus(DeployedIndexStatus.valueOf(rs.getString(COL_STATUS)));
    entry.setRetryCount(rs.getInt(COL_RETRY_COUNT));
    entry.setCreatedTime(rs.getLong(COL_CREATED_TIME));

    long startedTime = rs.getLong(COL_STARTED_TIME);
    entry.setStartedTime(rs.wasNull() ? null : startedTime);

    long completedTime = rs.getLong(COL_COMPLETED_TIME);
    entry.setCompletedTime(rs.wasNull() ? null : completedTime);

    entry.setErrorMessage(rs.getString(COL_ERROR_MESSAGE));
    return entry;
  }


  /**
   * Drains the result set into a list of DeployedIndex rows.
   *
   * @param rs the result set.
   * @return all rows mapped.
   * @throws SQLException if reading fails.
   */
  static List<DeployedIndex> mapAll(ResultSet rs) throws SQLException {
    List<DeployedIndex> result = new ArrayList<>();
    while (rs.next()) {
      result.add(mapRow(rs));
    }
    return result;
  }


  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  private static SelectStatement selectAllColumns() {
    return select(
        field(COL_ID), field(COL_TABLE_NAME),
        field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
        field(COL_STATUS), field(COL_RETRY_COUNT),
        field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
        field(COL_ERROR_MESSAGE))
        .from(tableRef(TABLE));
  }
}
