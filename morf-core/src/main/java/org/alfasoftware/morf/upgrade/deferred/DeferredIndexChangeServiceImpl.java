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

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Default implementation of {@link DeferredIndexChangeService}.
 *
 * <p>Maintains an in-memory map of pending deferred ADD INDEX operations keyed
 * by upper-cased table name then upper-cased index name, and constructs the
 * DSL {@link Statement}s (INSERT/DELETE/UPDATE) needed to manage the deferred
 * operation queue when subsequent schema changes interact with them.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexChangeServiceImpl implements DeferredIndexChangeService {

  /**
   * Pending deferred ADD INDEX operations registered during this upgrade session,
   * keyed by table name (upper-cased) then index name (upper-cased).
   */
  private final Map<String, Map<String, DeferredAddIndex>> pendingDeferredIndexes = new LinkedHashMap<>();


  @Override
  public List<Statement> trackPending(DeferredAddIndex deferredAddIndex) {
    long operationId = Math.abs(UUID.randomUUID().getMostSignificantBits());
    long createdTime = DeferredIndexTimestamps.currentTimestamp();

    List<Statement> statements = new ArrayList<>();

    statements.add(
      insert().into(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .values(
          literal(operationId).as("id"),
          literal(deferredAddIndex.getUpgradeUUID()).as("upgradeUUID"),
          literal(deferredAddIndex.getTableName()).as("tableName"),
          literal(deferredAddIndex.getNewIndex().getName()).as("indexName"),
          literal("ADD").as("operationType"),
          literal(deferredAddIndex.getNewIndex().isUnique()).as("indexUnique"),
          literal("PENDING").as("status"),
          literal(0).as("retryCount"),
          literal(createdTime).as("createdTime")
        )
    );

    int seq = 0;
    for (String columnName : deferredAddIndex.getNewIndex().columnNames()) {
      statements.add(
        insert().into(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME))
          .values(
            literal(Math.abs(UUID.randomUUID().getMostSignificantBits())).as("id"),
            literal(operationId).as("operationId"),
            literal(columnName).as("columnName"),
            literal(seq++).as("columnSequence")
          )
      );
    }

    pendingDeferredIndexes
      .computeIfAbsent(deferredAddIndex.getTableName().toUpperCase(), k -> new LinkedHashMap<>())
      .put(deferredAddIndex.getNewIndex().getName().toUpperCase(), deferredAddIndex);

    return statements;
  }


  @Override
  public boolean hasPendingDeferred(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    return tableMap != null && tableMap.containsKey(indexName.toUpperCase());
  }


  @Override
  public List<Statement> cancelPending(String tableName, String indexName) {
    if (!hasPendingDeferred(tableName, indexName)) {
      return List.of();
    }

    SelectStatement idSubquery = select(field("id"))
      .from(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
      .where(and(
        field("tableName").eq(literal(tableName)),
        field("indexName").eq(literal(indexName)),
        field("status").eq(literal("PENDING"))
      ));

    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap != null) {
      tableMap.remove(indexName.toUpperCase());
      if (tableMap.isEmpty()) {
        pendingDeferredIndexes.remove(tableName.toUpperCase());
      }
    }

    return List.of(
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME))
        .where(field("operationId").in(idSubquery)),
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .where(and(
          field("tableName").eq(literal(tableName)),
          field("indexName").eq(literal(indexName)),
          field("status").eq(literal("PENDING"))
        ))
    );
  }


  @Override
  public List<Statement> cancelAllPendingForTable(String tableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(tableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }

    SelectStatement idSubquery = select(field("id"))
      .from(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
      .where(and(
        field("tableName").eq(literal(tableName)),
        field("status").eq(literal("PENDING"))
      ));

    return List.of(
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME))
        .where(field("operationId").in(idSubquery)),
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .where(and(
          field("tableName").eq(literal(tableName)),
          field("status").eq(literal("PENDING"))
        ))
    );
  }


  @Override
  public List<Statement> cancelPendingReferencingColumn(String tableName, String columnName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<String> toCancel = new ArrayList<>();
    for (DeferredAddIndex dai : tableMap.values()) {
      if (dai.getNewIndex().columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
        toCancel.add(dai.getNewIndex().getName());
      }
    }

    if (toCancel.isEmpty()) {
      return List.of();
    }

    List<Statement> statements = new ArrayList<>();
    for (String indexName : toCancel) {
      statements.addAll(cancelPending(tableName, indexName));
    }
    return statements;
  }


  @Override
  public List<Statement> updatePendingTableName(String oldTableName, String newTableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(oldTableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }

    // Rebuild in-memory entries with the new table name
    Map<String, DeferredAddIndex> updatedMap = new LinkedHashMap<>();
    for (Map.Entry<String, DeferredAddIndex> entry : tableMap.entrySet()) {
      DeferredAddIndex dai = entry.getValue();
      updatedMap.put(entry.getKey(), new DeferredAddIndex(newTableName, dai.getNewIndex(), dai.getUpgradeUUID()));
    }
    pendingDeferredIndexes.put(newTableName.toUpperCase(), updatedMap);

    return List.of(
      update(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .set(literal(newTableName).as("tableName"))
        .where(and(
          field("tableName").eq(literal(oldTableName)),
          field("status").eq(literal("PENDING"))
        ))
    );
  }


  @Override
  public List<Statement> updatePendingColumnName(String tableName, String oldColumnName, String newColumnName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    boolean anyAffected = tableMap.values().stream()
      .anyMatch(dai -> dai.getNewIndex().columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(oldColumnName)));
    if (!anyAffected) {
      return List.of();
    }

    // Rebuild in-memory entries with updated column names
    for (Map.Entry<String, DeferredAddIndex> entry : tableMap.entrySet()) {
      DeferredAddIndex dai = entry.getValue();
      if (dai.getNewIndex().columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(oldColumnName))) {
        List<String> updatedColumns = dai.getNewIndex().columnNames().stream()
            .map(c -> c.equalsIgnoreCase(oldColumnName) ? newColumnName : c)
            .collect(Collectors.toList());
        Index updatedIndex = dai.getNewIndex().isUnique()
            ? index(dai.getNewIndex().getName()).columns(updatedColumns).unique()
            : index(dai.getNewIndex().getName()).columns(updatedColumns);
        entry.setValue(new DeferredAddIndex(dai.getTableName(), updatedIndex, dai.getUpgradeUUID()));
      }
    }

    return List.of(
      update(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME))
        .set(literal(newColumnName).as("columnName"))
        .where(and(
          field("columnName").eq(literal(oldColumnName)),
          field("operationId").in(
            select(field("id"))
              .from(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
              .where(and(
                field("tableName").eq(literal(tableName)),
                field("status").eq(literal("PENDING"))
              ))
          )
        ))
    );
  }


  @Override
  public List<Statement> updatePendingIndexName(String tableName, String oldIndexName, String newIndexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(oldIndexName.toUpperCase())) {
      return List.of();
    }

    DeferredAddIndex existing = tableMap.remove(oldIndexName.toUpperCase());
    tableMap.put(newIndexName.toUpperCase(), existing);

    return List.of(
      update(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .set(literal(newIndexName).as("indexName"))
        .where(and(
          field("tableName").eq(literal(tableName)),
          field("indexName").eq(literal(oldIndexName)),
          field("status").eq(literal("PENDING"))
        ))
    );
  }
}
