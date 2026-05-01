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

package org.alfasoftware.morf.upgrade.deferredindexes;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.nullLiteral;
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

import com.google.inject.Singleton;

/**
 * Package-private collaborator holding the DeferredIndexes column names,
 * every DSL statement that targets the table, and the ResultSet → DeferredIndex
 * mapping. Stateless but injectable — callers depend on this via constructor
 * injection rather than static method calls, matching the rest of the
 * deferredindexes package's wiring style.
 *
 * <p>Replaces the previous {@code DeferredIndexesStatementFactory} +
 * {@code Impl} pair: with no behavioural variants there's nothing to model
 * behind an interface, so this is a single concrete class. Package-private
 * keeps it out of the adopter-facing API surface.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexesStatements {

  /** Table name — the DeferredIndexes registration table. */
  static final String TABLE = DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME;

  /** Column: primary key. */
  static final String COL_ID = "id";
  /** Column: table the registered index belongs to. */
  static final String COL_TABLE_NAME = "tableName";
  /** Column: index name. */
  static final String COL_INDEX_NAME = "indexName";
  /** Column: whether the index is unique. */
  static final String COL_INDEX_UNIQUE = "indexUnique";
  /** Column: comma-separated column list the index covers. */
  static final String COL_INDEX_COLUMNS = "indexColumns";
  /** Column: lifecycle status (PENDING/IN_PROGRESS/COMPLETED/FAILED). */
  static final String COL_STATUS = "status";
  /** Column: number of CREATE attempts for the deferred build (reset on COMPLETED). */
  static final String COL_ATTEMPTS_COUNT = "attemptsCount";
  /** Column: epoch ms when the registration row was created. */
  static final String COL_CREATED_TIME = "createdTime";
  /** Column: epoch ms when the app started building this deferred index. */
  static final String COL_STARTED_TIME = "startedTime";
  /** Column: epoch ms when the app finished building this deferred index. */
  static final String COL_COMPLETED_TIME = "completedTime";
  /** Column: failure message for FAILED builds. */
  static final String COL_ERROR_MESSAGE = "errorMessage";


  // -------------------------------------------------------------------------
  // Read queries
  // -------------------------------------------------------------------------

  /** @return SELECT all rows, ordered by id. */
  SelectStatement selectAll() {
    return select().from(tableRef(TABLE)).orderBy(field(COL_ID));
  }


  /** @return SELECT rows whose status is non-terminal (PENDING/IN_PROGRESS/FAILED). */
  SelectStatement selectNonTerminal() {
    return select().from(tableRef(TABLE))
        .where(or(
            field(COL_STATUS).eq(DeferredIndexStatus.PENDING.name()),
            field(COL_STATUS).eq(DeferredIndexStatus.IN_PROGRESS.name()),
            field(COL_STATUS).eq(DeferredIndexStatus.FAILED.name())))
        .orderBy(field(COL_ID));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return SELECT the single row for ({@code tableName}, {@code indexName}).
   */
  SelectStatement selectByTableAndIndex(String tableName, String indexName) {
    return select().from(tableRef(TABLE))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /** @return SELECT status column alone (caller aggregates into status → count). */
  SelectStatement selectStatusColumn() {
    return select(field(COL_STATUS)).from(tableRef(TABLE));
  }


  // -------------------------------------------------------------------------
  // Status update statements (executed directly at runtime by the tracker)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param startedTime epoch ms when this attempt began.
   * @param newAttemptsCount the value to write into {@code attemptsCount} —
   *     the build task computes this as {@code currentAttemptsCount + 1} from
   *     the row it just re-fetched.
   * @return UPDATE flipping status to IN_PROGRESS, setting startedTime, and
   *     bumping attemptsCount. Leaves the existing errorMessage in place so
   *     operators can see the prior failure detail until success clears it.
   */
  UpdateStatement markStarted(String tableName, String indexName, long startedTime, int newAttemptsCount) {
    return update(tableRef(TABLE))
        .set(literal(DeferredIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
             literal(startedTime).as(COL_STARTED_TIME),
             literal(newAttemptsCount).as(COL_ATTEMPTS_COUNT))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param completedTime epoch ms.
   * @return UPDATE flipping status to COMPLETED, setting completedTime, and
   *     clearing the recoverable-failure registration columns
   *     ({@code attemptsCount=0}, {@code errorMessage=NULL}).
   */
  UpdateStatement markCompleted(String tableName, String indexName, long completedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeferredIndexStatus.COMPLETED.name()).as(COL_STATUS),
             literal(completedTime).as(COL_COMPLETED_TIME),
             literal(0).as(COL_ATTEMPTS_COUNT),
             nullLiteral().as(COL_ERROR_MESSAGE))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @param errorMessage the failure message (replaces any prior value).
   * @return UPDATE flipping status to FAILED and setting errorMessage. Does
   *     not touch attemptsCount — that was already bumped by the matching
   *     {@link #markStarted}.
   */
  UpdateStatement markFailed(String tableName, String indexName, String errorMessage) {
    return update(tableRef(TABLE))
        .set(literal(DeferredIndexStatus.FAILED.name()).as(COL_STATUS),
             literal(errorMessage).as(COL_ERROR_MESSAGE))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  // -------------------------------------------------------------------------
  // Registration DML (executed as part of the upgrade script via the visitor)
  // -------------------------------------------------------------------------

  /**
   * @param tableName the table.
   * @param index the index — must be effective-deferred.
   * @return INSERT adding a new registration row with status PENDING.
   */
  InsertStatement registerIndex(String tableName, Index index) {
    long operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    long createdTime = System.currentTimeMillis();

    return insert().into(tableRef(TABLE))
        .values(
            literal(operationId).as(COL_ID),
            literal(tableName).as(COL_TABLE_NAME),
            literal(index.getName()).as(COL_INDEX_NAME),
            literal(index.isUnique()).as(COL_INDEX_UNIQUE),
            literal(String.join(",", index.columnNames())).as(COL_INDEX_COLUMNS),
            literal(DeferredIndexStatus.PENDING.name()).as(COL_STATUS),
            literal(0).as(COL_ATTEMPTS_COUNT),
            literal(createdTime).as(COL_CREATED_TIME)
        );
  }


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return DELETE removing the registration row.
   */
  DeleteStatement unregisterIndex(String tableName, String indexName) {
    return delete(tableRef(TABLE))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  /**
   * @param tableName the table.
   * @return DELETE removing all registration rows for the table.
   */
  DeleteStatement unregisterAllFor(String tableName) {
    return delete(tableRef(TABLE)).where(field(COL_TABLE_NAME).eq(literal(tableName)));
  }


  /**
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE renaming the tableName column for every registration row.
   */
  UpdateStatement updateTableName(String oldTableName, String newTableName) {
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
  UpdateStatement updateIndexColumns(String tableName, String indexName, String newColumnsCsv) {
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
   * @return UPDATE renaming the index in its registration row.
   */
  UpdateStatement updateIndexName(String tableName, String oldIndexName, String newIndexName) {
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
   * Maps a ResultSet positioned on a DeferredIndexes row to a
   * {@link DeferredIndex}.
   *
   * @param rs the result set.
   * @return the populated DeferredIndex.
   * @throws SQLException if reading fails.
   */
  DeferredIndex mapRow(ResultSet rs) throws SQLException {
    DeferredIndex entry = new DeferredIndex();
    entry.setId(rs.getLong(COL_ID));
    entry.setTableName(rs.getString(COL_TABLE_NAME));
    entry.setIndexName(rs.getString(COL_INDEX_NAME));
    entry.setIndexUnique(rs.getBoolean(COL_INDEX_UNIQUE));
    entry.setIndexColumns(Arrays.asList(rs.getString(COL_INDEX_COLUMNS).split(",")));
    entry.setStatus(DeferredIndexStatus.valueOf(rs.getString(COL_STATUS)));
    entry.setAttemptsCount(rs.getInt(COL_ATTEMPTS_COUNT));
    entry.setCreatedTime(rs.getLong(COL_CREATED_TIME));

    long startedTime = rs.getLong(COL_STARTED_TIME);
    entry.setStartedTime(rs.wasNull() ? null : startedTime);

    long completedTime = rs.getLong(COL_COMPLETED_TIME);
    entry.setCompletedTime(rs.wasNull() ? null : completedTime);

    entry.setErrorMessage(rs.getString(COL_ERROR_MESSAGE));
    return entry;
  }


  /**
   * Drains the result set into a list of DeferredIndex rows.
   *
   * @param rs the result set.
   * @return all rows mapped.
   * @throws SQLException if reading fails.
   */
  List<DeferredIndex> mapAll(ResultSet rs) throws SQLException {
    List<DeferredIndex> result = new ArrayList<>();
    while (rs.next()) {
      result.add(mapRow(rs));
    }
    return result;
  }


}
