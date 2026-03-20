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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tracks pending deferred ADD INDEX operations in memory, resolving cascades
 * (defer then remove = cancelled, defer then rename = renamed, etc.) without
 * producing any SQL statements.
 *
 * <p>This is used both during upgrade execution (in
 * {@link org.alfasoftware.morf.upgrade.AbstractSchemaChangeVisitor}) and
 * during replay-based discovery (in
 * {@link org.alfasoftware.morf.upgrade.SchemaChangeSequence#findSurvivingDeferredIndexes()})
 * to determine which deferred indexes survive cascading schema changes.</p>
 *
 * <p>A single instance is created per usage context and lives for the
 * duration of that context. It is not Guice-managed.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexTracker {

  private static final Log log = LogFactory.getLog(DeferredIndexTracker.class);

  /**
   * Pending deferred ADD INDEX operations, keyed by table name (upper-cased)
   * then index name (upper-cased).
   */
  private final Map<String, Map<String, DeferredAddIndex>> pendingDeferredIndexes = new LinkedHashMap<>();


  /**
   * Records a deferred ADD INDEX operation in the tracker.
   *
   * @param deferredAddIndex the operation to track.
   */
  public void trackPending(DeferredAddIndex deferredAddIndex) {
    if (log.isDebugEnabled()) {
      log.debug("Tracking deferred index: table=" + deferredAddIndex.getTableName()
          + ", index=" + deferredAddIndex.getNewIndex().getName()
          + ", columns=" + deferredAddIndex.getNewIndex().columnNames());
    }

    pendingDeferredIndexes
      .computeIfAbsent(deferredAddIndex.getTableName().toUpperCase(), k -> new LinkedHashMap<>())
      .put(deferredAddIndex.getNewIndex().getName().toUpperCase(), deferredAddIndex);
  }


  /**
   * Returns {@code true} if a pending deferred ADD INDEX is currently tracked
   * for the given table and index (case-insensitive comparison).
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return {@code true} if a pending deferred ADD is tracked.
   */
  public boolean hasPendingDeferred(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    return tableMap != null && tableMap.containsKey(indexName.toUpperCase());
  }


  /**
   * Returns the tracked pending {@link DeferredAddIndex} for the given table
   * and index, if one is tracked.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return the tracked operation, or empty if none is tracked.
   */
  public Optional<DeferredAddIndex> getPendingDeferred(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    return Optional.ofNullable(tableMap != null ? tableMap.get(indexName.toUpperCase()) : null);
  }


  /**
   * Cancels the tracked pending operation for the given table/index and removes
   * it from tracking.
   *
   * @param tableName the table name.
   * @param indexName the index name.
   */
  public void cancelPending(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(indexName.toUpperCase())) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Cancelling deferred index: table=" + tableName + ", index=" + indexName);
    }

    tableMap.remove(indexName.toUpperCase());
    if (tableMap.isEmpty()) {
      pendingDeferredIndexes.remove(tableName.toUpperCase());
    }
  }


  /**
   * Cancels all tracked pending operations for the given table.
   *
   * @param tableName the table name.
   */
  public void cancelAllPendingForTable(String tableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(tableName.toUpperCase());
    if (tableMap != null && !tableMap.isEmpty() && log.isDebugEnabled()) {
      log.debug("Cancelling all deferred indexes for table [" + tableName + "]: " + tableMap.keySet());
    }
  }


  /**
   * Cancels all tracked pending operations for the given table whose column
   * list includes the specified column name.
   *
   * @param tableName  the table name.
   * @param columnName the column name.
   */
  public void cancelPendingReferencingColumn(String tableName, String columnName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return;
    }

    List<String> toCancel = new ArrayList<>();
    for (DeferredAddIndex dai : tableMap.values()) {
      if (dai.getNewIndex().columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
        toCancel.add(dai.getNewIndex().getName());
      }
    }

    for (String idxName : toCancel) {
      cancelPending(tableName, idxName);
    }
  }


  /**
   * Updates the table name for all tracked pending operations on the old table.
   *
   * @param oldTableName the current table name.
   * @param newTableName the new table name.
   */
  public void updatePendingTableName(String oldTableName, String newTableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(oldTableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Renaming table in deferred indexes: [" + oldTableName + "] -> [" + newTableName + "]");
    }

    Map<String, DeferredAddIndex> updatedMap = new LinkedHashMap<>();
    for (Map.Entry<String, DeferredAddIndex> entry : tableMap.entrySet()) {
      DeferredAddIndex dai = entry.getValue();
      updatedMap.put(entry.getKey(), new DeferredAddIndex(newTableName, dai.getNewIndex(), dai.getUpgradeUUID()));
    }
    pendingDeferredIndexes.put(newTableName.toUpperCase(), updatedMap);
  }


  /**
   * Updates a column name in all tracked pending operations for the given table.
   *
   * @param tableName     the table name.
   * @param oldColumnName the current column name.
   * @param newColumnName the new column name.
   */
  public void updatePendingColumnName(String tableName, String oldColumnName, String newColumnName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return;
    }

    boolean anyAffected = tableMap.values().stream()
      .anyMatch(dai -> dai.getNewIndex().columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(oldColumnName)));
    if (!anyAffected) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Renaming column in deferred indexes: table=" + tableName
          + ", [" + oldColumnName + "] -> [" + newColumnName + "]");
    }

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
  }


  /**
   * Updates the index name for a tracked pending operation.
   *
   * @param tableName    the table name.
   * @param oldIndexName the current index name.
   * @param newIndexName the new index name.
   */
  public void updatePendingIndexName(String tableName, String oldIndexName, String newIndexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(oldIndexName.toUpperCase())) {
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Renaming index in deferred indexes: table=" + tableName
          + ", [" + oldIndexName + "] -> [" + newIndexName + "]");
    }

    DeferredAddIndex existing = tableMap.remove(oldIndexName.toUpperCase());
    Index renamedIndex = existing.getNewIndex().isUnique()
        ? index(newIndexName).columns(existing.getNewIndex().columnNames()).unique()
        : index(newIndexName).columns(existing.getNewIndex().columnNames());
    tableMap.put(newIndexName.toUpperCase(), new DeferredAddIndex(existing.getTableName(), renamedIndex, existing.getUpgradeUUID()));
  }


  /**
   * Returns all deferred indexes that survived cascading schema changes.
   * The returned list is a snapshot; subsequent mutations to this tracker
   * do not affect it.
   *
   * @return list of surviving {@link DeferredAddIndex} instances.
   */
  public List<DeferredAddIndex> getSurvivingDeferredIndexes() {
    List<DeferredAddIndex> result = new ArrayList<>();
    for (Map<String, DeferredAddIndex> tableMap : pendingDeferredIndexes.values()) {
      result.addAll(tableMap.values());
    }
    return result;
  }
}
