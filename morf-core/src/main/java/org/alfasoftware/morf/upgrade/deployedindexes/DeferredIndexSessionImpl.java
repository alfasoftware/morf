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
 * Default implementation of {@link DeferredIndexSession}. Owns the
 * in-memory per-upgrade cache; defers DSL construction to the injected
 * {@link DeployedIndexesStatements}.
 *
 * <p>Not a Guice singleton — constructed per upgrade run. The
 * {@link DeployedIndexesStatements} dependency is stateless and could be a
 * fresh instance or a Guice-managed singleton.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexSessionImpl implements DeferredIndexSession {

  private static final Log log = LogFactory.getLog(DeferredIndexSessionImpl.class);

  /** Cache: tableName (upper) -&gt; indexName (upper) -&gt; IndexRecord. */
  private final Map<String, Map<String, IndexRecord>> trackedIndexes = new LinkedHashMap<>();

  private final DeployedIndexesStatements statements;


  /**
   * @param statements DSL helper for the DeployedIndexes table.
   */
  public DeferredIndexSessionImpl(DeployedIndexesStatements statements) {
    this.statements = statements;
  }


  @Override
  public void prime(DeployedIndex entry) {
    if (log.isDebugEnabled()) {
      log.debug("Priming (persisted row): table=" + entry.getTableName()
          + ", index=" + entry.getIndexName());
    }
    // Slim invariant: every persisted row is a deferred index.
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
  public List<InsertStatement> trackIndex(String tableName, Index idx) {
    if (log.isDebugEnabled()) {
      log.debug("Tracking index: table=" + tableName + ", index=" + idx.getName()
          + ", deferred=" + idx.isDeferred());
    }
    trackedIndexes
        .computeIfAbsent(tableName.toUpperCase(), k -> new LinkedHashMap<>())
        .put(idx.getName().toUpperCase(), new IndexRecord(tableName, idx));

    return List.of(statements.trackIndex(tableName, idx));
  }


  @Override
  public boolean isTrackedDeferred(String tableName, String indexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    return tableMap != null && tableMap.containsKey(indexName.toUpperCase());
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
    return List.of(statements.removeIndex(removed.tableName, removed.index.getName()));
  }


  @Override
  public List<DeleteStatement> removeAllForTable(String tableName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.remove(tableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    String storedTableName = tableMap.values().iterator().next().tableName;
    return List.of(statements.removeAllForTable(storedTableName));
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

    List<DeleteStatement> deletes = new ArrayList<>();
    for (String idxName : toRemove) {
      deletes.addAll(removeIndex(tableName, idxName));
    }
    return deletes;
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

    return List.of(statements.updateTableName(storedOldTableName, newTableName));
  }


  @Override
  public List<UpdateStatement> updateColumnName(String tableName, String oldColumnName, String newColumnName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<UpdateStatement> updates = new ArrayList<>();
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

        updates.add(statements.updateIndexColumns(
            r.tableName, r.index.getName(), String.join(",", updatedColumns)));
      }
    }
    return updates;
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

    return List.of(statements.updateIndexName(
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
