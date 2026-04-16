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
import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeployedIndexesChangeService}.
 *
 * <p>Tracks ALL index operations during an upgrade session in an in-memory
 * map and produces SQL statements to keep the DeployedIndexes table in sync.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeployedIndexesChangeServiceImpl implements DeployedIndexesChangeService {

  private static final Log log = LogFactory.getLog(DeployedIndexesChangeServiceImpl.class);

  private static final String TABLE = DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME;
  private static final String COL_ID = "id";
  private static final String COL_UPGRADE_UUID = "upgradeUUID";
  private static final String COL_TABLE_NAME = "tableName";
  private static final String COL_INDEX_NAME = "indexName";
  private static final String COL_INDEX_UNIQUE = "indexUnique";
  private static final String COL_INDEX_COLUMNS = "indexColumns";
  private static final String COL_INDEX_DEFERRED = "indexDeferred";
  private static final String COL_STATUS = "status";
  private static final String COL_RETRY_COUNT = "retryCount";
  private static final String COL_CREATED_TIME = "createdTime";

  /** Tracked indexes: tableName (upper) -> indexName (upper) -> IndexRecord. */
  private final Map<String, Map<String, IndexRecord>> trackedIndexes = new LinkedHashMap<>();


  @Override
  public List<Statement> trackIndex(String tableName, Index index, String upgradeUUID) {
    if (log.isDebugEnabled()) {
      log.debug("Tracking index: table=" + tableName + ", index=" + index.getName()
          + ", deferred=" + index.isDeferred());
    }

    IndexRecord record = new IndexRecord(tableName, index, upgradeUUID);
    trackedIndexes
        .computeIfAbsent(tableName.toUpperCase(), k -> new LinkedHashMap<>())
        .put(index.getName().toUpperCase(), record);

    return buildInsertStatements(record);
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
  public List<Statement> removeIndex(String tableName, String indexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(indexName.toUpperCase())) {
      return List.of();
    }
    IndexRecord removed = tableMap.remove(indexName.toUpperCase());
    if (tableMap.isEmpty()) {
      trackedIndexes.remove(tableName.toUpperCase());
    }
    return buildDeleteStatements(
        field(COL_TABLE_NAME).eq(literal(removed.tableName)),
        field(COL_INDEX_NAME).eq(literal(removed.index.getName()))
    );
  }


  @Override
  public List<Statement> removeAllForTable(String tableName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.remove(tableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    String storedTableName = tableMap.values().iterator().next().tableName;
    return buildDeleteStatements(field(COL_TABLE_NAME).eq(literal(storedTableName)));
  }


  @Override
  public List<Statement> removeIndexesReferencingColumn(String tableName, String columnName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<String> toRemove = tableMap.values().stream()
        .filter(r -> r.index.columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(columnName)))
        .map(r -> r.index.getName())
        .collect(Collectors.toList());

    List<Statement> statements = new ArrayList<>();
    for (String idxName : toRemove) {
      statements.addAll(removeIndex(tableName, idxName));
    }
    return statements;
  }


  @Override
  public List<Statement> updateTableName(String oldTableName, String newTableName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.remove(oldTableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }

    String storedOldTableName = tableMap.values().iterator().next().tableName;

    Map<String, IndexRecord> updatedMap = new LinkedHashMap<>();
    for (Map.Entry<String, IndexRecord> entry : tableMap.entrySet()) {
      IndexRecord r = entry.getValue();
      updatedMap.put(entry.getKey(), new IndexRecord(newTableName, r.index, r.upgradeUUID));
    }
    trackedIndexes.put(newTableName.toUpperCase(), updatedMap);

    return List.of(
        update(tableRef(TABLE))
            .set(literal(newTableName).as(COL_TABLE_NAME))
            .where(field(COL_TABLE_NAME).eq(literal(storedOldTableName)))
    );
  }


  @Override
  public List<Statement> updateColumnName(String tableName, String oldColumnName, String newColumnName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    List<Statement> statements = new ArrayList<>();
    for (Map.Entry<String, IndexRecord> entry : tableMap.entrySet()) {
      IndexRecord r = entry.getValue();
      if (r.index.columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(oldColumnName))) {
        List<String> updatedColumns = r.index.columnNames().stream()
            .map(c -> c.equalsIgnoreCase(oldColumnName) ? newColumnName : c)
            .collect(Collectors.toList());

        IndexBuilder builder = index(r.index.getName()).columns(updatedColumns);
        if (r.index.isUnique()) builder = builder.unique();
        if (r.index.isDeferred()) builder = builder.deferred();
        Index updatedIndex = builder;

        entry.setValue(new IndexRecord(r.tableName, updatedIndex, r.upgradeUUID));

        statements.add(
            update(tableRef(TABLE))
                .set(literal(String.join(",", updatedColumns)).as(COL_INDEX_COLUMNS))
                .where(and(
                    field(COL_TABLE_NAME).eq(literal(r.tableName)),
                    field(COL_INDEX_NAME).eq(literal(r.index.getName()))
                ))
        );
      }
    }
    return statements;
  }


  @Override
  public List<Statement> updateIndexName(String tableName, String oldIndexName, String newIndexName) {
    Map<String, IndexRecord> tableMap = trackedIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(oldIndexName.toUpperCase())) {
      return List.of();
    }

    IndexRecord existing = tableMap.remove(oldIndexName.toUpperCase());

    IndexBuilder builder = index(newIndexName).columns(existing.index.columnNames());
    if (existing.index.isUnique()) builder = builder.unique();
    if (existing.index.isDeferred()) builder = builder.deferred();
    Index renamedIndex = builder;

    tableMap.put(newIndexName.toUpperCase(), new IndexRecord(existing.tableName, renamedIndex, existing.upgradeUUID));

    return List.of(
        update(tableRef(TABLE))
            .set(literal(newIndexName).as(COL_INDEX_NAME))
            .where(and(
                field(COL_TABLE_NAME).eq(literal(existing.tableName)),
                field(COL_INDEX_NAME).eq(literal(existing.index.getName()))
            ))
    );
  }


  // -------------------------------------------------------------------------
  // SQL builders
  // -------------------------------------------------------------------------

  private List<Statement> buildInsertStatements(IndexRecord record) {
    long operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    long createdTime = System.currentTimeMillis();
    String status = record.index.isDeferred()
        ? DeployedIndexStatus.PENDING.name()
        : DeployedIndexStatus.COMPLETED.name();

    return List.of(
        insert().into(tableRef(TABLE))
            .values(
                literal(operationId).as(COL_ID),
                literal(record.upgradeUUID).as(COL_UPGRADE_UUID),
                literal(record.tableName).as(COL_TABLE_NAME),
                literal(record.index.getName()).as(COL_INDEX_NAME),
                literal(record.index.isUnique()).as(COL_INDEX_UNIQUE),
                literal(String.join(",", record.index.columnNames())).as(COL_INDEX_COLUMNS),
                literal(record.index.isDeferred()).as(COL_INDEX_DEFERRED),
                literal(status).as(COL_STATUS),
                literal(0).as(COL_RETRY_COUNT),
                literal(createdTime).as(COL_CREATED_TIME)
            )
    );
  }


  private List<Statement> buildDeleteStatements(Criterion... criteria) {
    Criterion where = criteria.length == 1 ? criteria[0] : and(List.of(criteria));
    return List.of(delete(tableRef(TABLE)).where(where));
  }


  // -------------------------------------------------------------------------
  // Inner record
  // -------------------------------------------------------------------------

  private static final class IndexRecord {
    final String tableName;
    final Index index;
    final String upgradeUUID;

    IndexRecord(String tableName, Index index, String upgradeUUID) {
      this.tableName = tableName;
      this.index = index;
      this.upgradeUUID = upgradeUUID;
    }
  }
}
