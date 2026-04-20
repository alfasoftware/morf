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

import java.util.UUID;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;

import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeployedIndexesStatementFactory}. Pure DSL
 * construction — no state, no side effects, no DB access.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
public class DeployedIndexesStatementFactoryImpl implements DeployedIndexesStatementFactory {

  /** Default constructor — this factory has no dependencies. */
  public DeployedIndexesStatementFactoryImpl() {
    // no-op
  }


  @Override
  public SelectStatement statementToFindAll() {
    return selectAllColumns().orderBy(field(COL_ID));
  }


  @Override
  public SelectStatement statementToFindByTable(String tableName) {
    return selectAllColumns()
        .where(field(COL_TABLE_NAME).eq(tableName))
        .orderBy(field(COL_ID));
  }


  @Override
  public SelectStatement statementToFindNonTerminalOperations() {
    return selectAllColumns()
        .where(or(
            field(COL_STATUS).eq(DeployedIndexStatus.PENDING.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()),
            field(COL_STATUS).eq(DeployedIndexStatus.FAILED.name())))
        .orderBy(field(COL_ID));
  }


  @Override
  public SelectStatement statementToSelectStatusColumn() {
    return select(field(COL_STATUS)).from(tableRef(TABLE));
  }


  @Override
  public UpdateStatement statementToMarkStarted(String tableName, String indexName, long startedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.IN_PROGRESS.name()).as(COL_STATUS),
             literal(startedTime).as(COL_STARTED_TIME))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  @Override
  public UpdateStatement statementToMarkCompleted(String tableName, String indexName, long completedTime) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.COMPLETED.name()).as(COL_STATUS),
             literal(completedTime).as(COL_COMPLETED_TIME))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  @Override
  public UpdateStatement statementToMarkFailed(String tableName, String indexName, String errorMessage) {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.FAILED.name()).as(COL_STATUS),
             literal(errorMessage).as(COL_ERROR_MESSAGE))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  @Override
  public UpdateStatement statementToBumpRetryCount(String tableName, String indexName) {
    return update(tableRef(TABLE))
        .set(literal(1).as(COL_RETRY_COUNT))
        .where(and(
            field(COL_TABLE_NAME).eq(tableName),
            field(COL_INDEX_NAME).eq(indexName)));
  }


  @Override
  public UpdateStatement statementToResetInProgress() {
    return update(tableRef(TABLE))
        .set(literal(DeployedIndexStatus.PENDING.name()).as(COL_STATUS))
        .where(field(COL_STATUS).eq(DeployedIndexStatus.IN_PROGRESS.name()));
  }


  @Override
  public InsertStatement statementToTrackIndex(String tableName, Index index) {
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


  @Override
  public DeleteStatement statementToRemoveIndex(String tableName, String indexName) {
    return delete(tableRef(TABLE))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  @Override
  public DeleteStatement statementToRemoveAllForTable(String tableName) {
    return delete(tableRef(TABLE)).where(field(COL_TABLE_NAME).eq(literal(tableName)));
  }


  @Override
  public UpdateStatement statementToUpdateTableName(String oldTableName, String newTableName) {
    return update(tableRef(TABLE))
        .set(literal(newTableName).as(COL_TABLE_NAME))
        .where(field(COL_TABLE_NAME).eq(literal(oldTableName)));
  }


  @Override
  public UpdateStatement statementToUpdateIndexColumns(String tableName, String indexName, String newColumnsCsv) {
    return update(tableRef(TABLE))
        .set(literal(newColumnsCsv).as(COL_INDEX_COLUMNS))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(indexName))));
  }


  @Override
  public UpdateStatement statementToUpdateIndexName(String tableName, String oldIndexName, String newIndexName) {
    return update(tableRef(TABLE))
        .set(literal(newIndexName).as(COL_INDEX_NAME))
        .where(and(
            field(COL_TABLE_NAME).eq(literal(tableName)),
            field(COL_INDEX_NAME).eq(literal(oldIndexName))));
  }


  /**
   * @return a pre-configured SELECT of all DeployedIndexes columns from the
   *     table. Used as the base for most read queries.
   */
  private SelectStatement selectAllColumns() {
    return select(
        field(COL_ID), field(COL_TABLE_NAME),
        field(COL_INDEX_NAME), field(COL_INDEX_UNIQUE), field(COL_INDEX_COLUMNS),
        field(COL_STATUS), field(COL_RETRY_COUNT),
        field(COL_CREATED_TIME), field(COL_STARTED_TIME), field(COL_COMPLETED_TIME),
        field(COL_ERROR_MESSAGE))
        .from(tableRef(TABLE));
  }
}
