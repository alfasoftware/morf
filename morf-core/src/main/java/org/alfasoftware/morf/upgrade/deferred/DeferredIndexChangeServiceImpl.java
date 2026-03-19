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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexChangeService}.
 *
 * <p>Maintains an in-memory map of pending deferred ADD INDEX operations keyed
 * by upper-cased table name then upper-cased index name, and constructs the
 * DSL {@link Statement}s (INSERT/DELETE/UPDATE) needed to manage the deferred
 * operation queue when subsequent schema changes interact with them.
 *
 * <p>A single instance is created per upgrade run by
 * {@link org.alfasoftware.morf.upgrade.AbstractSchemaChangeVisitor} and lives
 * for the duration of that run. It is not Guice-managed because the visitor
 * itself is not Guice-managed.
 *
 * <p>The in-memory map mirrors what the generated SQL statements will do once
 * executed, allowing fast lookups (e.g. {@link #hasPendingDeferred}) and
 * column-level tracking (e.g. {@link #cancelPendingReferencingColumn}) without
 * requiring database access. The SQL statements are persisted per-step rather
 * than batched to the end so that crash recovery works correctly: if the
 * upgrade fails mid-way, deferred operations from already-committed steps are
 * safely in the database and will not be lost on restart.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexChangeServiceImpl implements DeferredIndexChangeService {

  private static final Log log = LogFactory.getLog(DeferredIndexChangeServiceImpl.class);

  /**
   * Pending deferred ADD INDEX operations registered during this upgrade session,
   * keyed by table name (upper-cased) then index name (upper-cased).
   */
  private final Map<String, Map<String, DeferredAddIndex>> pendingDeferredIndexes = new LinkedHashMap<>();


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#trackPending(DeferredAddIndex)
   */
  @Override
  public List<Statement> trackPending(DeferredAddIndex deferredAddIndex) {
    if (log.isDebugEnabled()) {
      log.debug("Tracking deferred index: table=" + deferredAddIndex.getTableName()
          + ", index=" + deferredAddIndex.getNewIndex().getName()
          + ", columns=" + deferredAddIndex.getNewIndex().columnNames());
    }

    pendingDeferredIndexes
      .computeIfAbsent(deferredAddIndex.getTableName().toUpperCase(), k -> new LinkedHashMap<>())
      .put(deferredAddIndex.getNewIndex().getName().toUpperCase(), deferredAddIndex);

    return buildInsertStatements(deferredAddIndex);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#hasPendingDeferred(String, String)
   */
  @Override
  public boolean hasPendingDeferred(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    return tableMap != null && tableMap.containsKey(indexName.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#getPendingDeferred(String, String)
   */
  @Override
  public Optional<DeferredAddIndex> getPendingDeferred(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    return Optional.ofNullable(tableMap != null ? tableMap.get(indexName.toUpperCase()) : null);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#cancelPending(String, String)
   */
  @Override
  public List<Statement> cancelPending(String tableName, String indexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(indexName.toUpperCase())) {
      return List.of();
    }
    if (log.isDebugEnabled()) {
      log.debug("Cancelling deferred index: table=" + tableName + ", index=" + indexName);
    }

    DeferredAddIndex dai = tableMap.remove(indexName.toUpperCase());
    if (tableMap.isEmpty()) {
      pendingDeferredIndexes.remove(tableName.toUpperCase());
    }

    return buildDeleteStatements(
      field("tableName").eq(literal(dai.getTableName())),
      field("indexName").eq(literal(dai.getNewIndex().getName()))
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#cancelAllPendingForTable(String)
   */
  @Override
  public List<Statement> cancelAllPendingForTable(String tableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(tableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    if (log.isDebugEnabled()) {
      log.debug("Cancelling all deferred indexes for table [" + tableName + "]: " + tableMap.keySet());
    }

    String storedTableName = tableMap.values().iterator().next().getTableName();
    return buildDeleteStatements(
      field("tableName").eq(literal(storedTableName))
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#cancelPendingReferencingColumn(String, String)
   */
  @Override
  public List<Statement> cancelPendingReferencingColumn(String tableName, String columnName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null) {
      return List.of();
    }

    String storedTableName = tableMap.values().iterator().next().getTableName();

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
      statements.addAll(cancelPending(storedTableName, indexName));
    }
    return statements;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#updatePendingTableName(String, String)
   */
  @Override
  public List<Statement> updatePendingTableName(String oldTableName, String newTableName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.remove(oldTableName.toUpperCase());
    if (tableMap == null || tableMap.isEmpty()) {
      return List.of();
    }
    if (log.isDebugEnabled()) {
      log.debug("Renaming table in deferred indexes: [" + oldTableName + "] -> [" + newTableName + "]");
    }

    String storedOldTableName = tableMap.values().iterator().next().getTableName();

    Map<String, DeferredAddIndex> updatedMap = new LinkedHashMap<>();
    for (Map.Entry<String, DeferredAddIndex> entry : tableMap.entrySet()) {
      DeferredAddIndex dai = entry.getValue();
      updatedMap.put(entry.getKey(), new DeferredAddIndex(newTableName, dai.getNewIndex(), dai.getUpgradeUUID()));
    }
    pendingDeferredIndexes.put(newTableName.toUpperCase(), updatedMap);

    return buildUpdateOperationStatements(
      literal(newTableName).as("tableName"),
      field("tableName").eq(literal(storedOldTableName))
    );
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#updatePendingColumnName(String, String, String)
   */
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
    if (log.isDebugEnabled()) {
      log.debug("Renaming column in deferred indexes: table=" + tableName
          + ", [" + oldColumnName + "] -> [" + newColumnName + "]");
    }

    String storedTableName = tableMap.values().iterator().next().getTableName();

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

    return buildUpdateColumnStatements(storedTableName, oldColumnName, newColumnName);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.deferred.DeferredIndexChangeService#updatePendingIndexName(String, String, String)
   */
  @Override
  public List<Statement> updatePendingIndexName(String tableName, String oldIndexName, String newIndexName) {
    Map<String, DeferredAddIndex> tableMap = pendingDeferredIndexes.get(tableName.toUpperCase());
    if (tableMap == null || !tableMap.containsKey(oldIndexName.toUpperCase())) {
      return List.of();
    }
    if (log.isDebugEnabled()) {
      log.debug("Renaming index in deferred indexes: table=" + tableName
          + ", [" + oldIndexName + "] -> [" + newIndexName + "]");
    }

    DeferredAddIndex existing = tableMap.remove(oldIndexName.toUpperCase());
    String storedTableName = existing.getTableName();
    String storedIndexName = existing.getNewIndex().getName();

    Index renamedIndex = existing.getNewIndex().isUnique()
        ? index(newIndexName).columns(existing.getNewIndex().columnNames()).unique()
        : index(newIndexName).columns(existing.getNewIndex().columnNames());
    tableMap.put(newIndexName.toUpperCase(), new DeferredAddIndex(storedTableName, renamedIndex, existing.getUpgradeUUID()));

    return buildUpdateOperationStatements(
      literal(newIndexName).as("indexName"),
      field("tableName").eq(literal(storedTableName)),
      field("indexName").eq(literal(storedIndexName))
    );
  }


  // -------------------------------------------------------------------------
  // SQL statement builders
  // -------------------------------------------------------------------------

  /**
   * Builds INSERT statements for a deferred operation and its column rows.
   */
  private List<Statement> buildInsertStatements(DeferredAddIndex deferredAddIndex) {
    long operationId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    long createdTime = System.currentTimeMillis();

    List<Statement> statements = new ArrayList<>();

    statements.add(
      insert().into(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .values(
          literal(operationId).as("id"),
          literal(deferredAddIndex.getUpgradeUUID()).as("upgradeUUID"),
          literal(deferredAddIndex.getTableName()).as("tableName"),
          literal(deferredAddIndex.getNewIndex().getName()).as("indexName"),
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
            literal(UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE).as("id"),
            literal(operationId).as("operationId"),
            literal(columnName).as("columnName"),
            literal(seq++).as("columnSequence")
          )
      );
    }

    return statements;
  }


  /**
   * Builds DELETE statements to remove pending operations and their column rows.
   * The criteria identify which operations to delete (e.g. by table name, index name).
   */
  private List<Statement> buildDeleteStatements(Criterion... operationCriteria) {
    Criterion where = pendingWhere(operationCriteria);

    SelectStatement idSubquery = select(field("id"))
      .from(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
      .where(where);

    return List.of(
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME))
        .where(field("operationId").in(idSubquery)),
      delete(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .where(where)
    );
  }


  /**
   * Builds an UPDATE statement against the operation table. The SET clause
   * is the first argument; the remaining arguments form the WHERE clause
   * (combined with a {@code status = 'PENDING'} filter).
   */
  private List<Statement> buildUpdateOperationStatements(org.alfasoftware.morf.sql.element.AliasedField setClause, Criterion... whereCriteria) {
    return List.of(
      update(tableRef(DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME))
        .set(setClause)
        .where(pendingWhere(whereCriteria))
    );
  }


  /**
   * Builds an UPDATE statement to rename a column in the column table, scoped
   * to pending operations for the given table.
   */
  private List<Statement> buildUpdateColumnStatements(String tableName, String oldColumnName, String newColumnName) {
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


  /**
   * Combines the given criteria with a {@code status = 'PENDING'} filter.
   */
  private Criterion pendingWhere(Criterion... criteria) {
    List<Criterion> all = new ArrayList<>(Arrays.asList(criteria));
    all.add(field("status").eq(literal("PENDING")));
    return and(all);
  }
}
