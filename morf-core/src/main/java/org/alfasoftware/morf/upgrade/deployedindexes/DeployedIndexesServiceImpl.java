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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.UpdateStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeployedIndexesService}. Owns the
 * in-memory session state for tracked indexes and orchestrates statement
 * lists via {@link DeployedIndexesStatementFactory}. Does not build DSL
 * or hold column constants of its own.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeployedIndexesServiceImpl implements DeployedIndexesService {

  private static final Log log = LogFactory.getLog(DeployedIndexesServiceImpl.class);

  private final DeployedIndexesStatementFactory factory;

  /** Tracked indexes: tableName (upper) -&gt; indexName (upper) -&gt; IndexRecord. */
  private final Map<String, Map<String, IndexRecord>> trackedIndexes = new LinkedHashMap<>();


  /**
   * Constructs the service.
   *
   * @param factory statement factory used to build every tracking DML.
   */
  public DeployedIndexesServiceImpl(DeployedIndexesStatementFactory factory) {
    this.factory = factory;
  }


  @Override
  public void prime(DeployedIndex entry) {
    if (log.isDebugEnabled()) {
      log.debug("Priming (persisted row): table=" + entry.getTableName()
          + ", index=" + entry.getIndexName());
    }
    // Reconstruct the Index from the persisted row. Slim invariant: every
    // persisted row is a deferred index, so the rebuilt Index is always
    // marked deferred regardless of the legacy indexDeferred column.
    IndexBuilder builder = index(entry.getIndexName()).columns(entry.getIndexColumns());
    if (entry.isIndexUnique()) {
      builder = builder.unique();
    }
    builder = builder.deferred();
    trackedIndexes
        .computeIfAbsent(entry.getTableName().toUpperCase(), k -> new LinkedHashMap<>())
        .put(entry.getIndexName().toUpperCase(), new IndexRecord(entry.getTableName(), builder));
  }


  @Override
  public List<InsertStatement> trackIndex(String tableName, Index index) {
    if (log.isDebugEnabled()) {
      log.debug("Tracking index: table=" + tableName + ", index=" + index.getName()
          + ", deferred=" + index.isDeferred());
    }
    trackedIndexes
        .computeIfAbsent(tableName.toUpperCase(), k -> new LinkedHashMap<>())
        .put(index.getName().toUpperCase(), new IndexRecord(tableName, index));

    return List.of(factory.statementToTrackIndex(tableName, index));
  }


  @Override
  public boolean isTracked(String tableName, String indexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    return tableMap != null && tableMap.containsKey(indexName.toUpperCase());
  }


  @Override
  public boolean isTrackedDeferred(String tableName, String indexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return false;
    }
    IndexRecord record = tableMap.get(indexName.toUpperCase());
    return record != null && record.index.isDeferred();
  }


  @Override
  public List<DeleteStatement> removeIndex(String tableName, String indexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(indexName.toUpperCase())) {
      return List.of();
    }
    IndexRecord removed = tableMap.remove(indexName.toUpperCase());
    if (tableMap.isEmpty()) {
      trackedIndexes.remove(tableName.toUpperCase());
    }
    return List.of(factory.statementToRemoveIndex(removed.tableName, removed.index.getName()));
  }


  @Override
  public List<DeleteStatement> removeAllForTable(String tableName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.remove(tableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    String storedTableName = tableMap.values().iterator().next().tableName;
    return List.of(factory.statementToRemoveAllForTable(storedTableName));
  }


  @Override
  public List<DeleteStatement> removeIndexesReferencingColumn(String tableName, String columnName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<String> toRemove = tableMap.values().stream()
        .filter(r -> r.index.columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(columnName)))
        .map(r -> r.index.getName())
        .collect(Collectors.toList());

    List<DeleteStatement> statements = new ArrayList<>();
    for (String idxName : toRemove) {
      statements.addAll(removeIndex(tableName, idxName));
    }
    return statements;
  }


  @Override
  public List<UpdateStatement> updateTableName(String oldTableName, String newTableName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.remove(oldTableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    String storedOldTableName = tableMap.values().iterator().next().tableName;

    Map<String, IndexRecord> updatedMap = new LinkedHashMap<>();
    for (Map.Entry<String, IndexRecord> entry : tableMap.entrySet()) {
      updatedMap.put(entry.getKey(), new IndexRecord(newTableName, entry.getValue().index));
    }
    trackedIndexes.put(newTableName.toUpperCase(), updatedMap);

    return List.of(factory.statementToUpdateTableName(storedOldTableName, newTableName));
  }


  @Override
  public List<UpdateStatement> updateColumnName(String tableName, String oldColumnName, String newColumnName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<UpdateStatement> statements = new ArrayList<>();
    for (Map.Entry<String, IndexRecord> entry : tableMap.entrySet()) {
      IndexRecord r = entry.getValue();
      if (r.index.columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(oldColumnName))) {
        List<String> updatedColumns = r.index.columnNames().stream()
            .map(c -> c.equalsIgnoreCase(oldColumnName) ? newColumnName : c)
            .collect(Collectors.toList());

        IndexBuilder builder = index(r.index.getName()).columns(updatedColumns);
        if (r.index.isUnique()) builder = builder.unique();
        if (r.index.isDeferred()) builder = builder.deferred();
        entry.setValue(new IndexRecord(r.tableName, builder));

        statements.add(factory.statementToUpdateIndexColumns(
            r.tableName, r.index.getName(), String.join(",", updatedColumns)));
      }
    }
    return statements;
  }


  @Override
  public List<UpdateStatement> updateIndexName(String tableName, String oldIndexName, String newIndexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(oldIndexName.toUpperCase())) {
      return List.of();
    }

    IndexRecord existing = tableMap.remove(oldIndexName.toUpperCase());

    IndexBuilder builder = index(newIndexName).columns(existing.index.columnNames());
    if (existing.index.isUnique()) builder = builder.unique();
    if (existing.index.isDeferred()) builder = builder.deferred();
    tableMap.put(newIndexName.toUpperCase(), new IndexRecord(existing.tableName, builder));

    return List.of(factory.statementToUpdateIndexName(
        existing.tableName, existing.index.getName(), newIndexName));
  }


  // -------------------------------------------------------------------------
  // Inner record
  // -------------------------------------------------------------------------

  private static final class IndexRecord {
    final String tableName;
    final Index index;

    IndexRecord(String tableName, Index index) {
      this.tableName = tableName;
      this.index = index;
    }
  }
}
