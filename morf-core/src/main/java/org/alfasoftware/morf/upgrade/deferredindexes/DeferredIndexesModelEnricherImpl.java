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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeferredIndexesModelEnricher} for the
 * background-build branch.
 *
 * <p>Responsibilities:</p>
 * <ol>
 *   <li><b>Prime the per-upgrade session</b> with every persisted tracking
 *       row (built and unbuilt) so the visitor's mutation methods correctly
 *       cascade to all currently-declared deferred indexes.</li>
 *   <li><b>Rebuild physical indexes that match a COMPLETED row</b> with the
 *       {@code .deferred()} flag — preserves the declarative property
 *       across the build lifecycle.</li>
 *   <li><b>Virtualize unbuilt-deferred rows</b> (status non-terminal) into
 *       the source schema so {@code SchemaHomology.schemasMatch} treats
 *       them as declared. This covers both the never-built case (no
 *       physical index yet) and the routine-restart case (physical present
 *       but the row says PENDING/IN_PROGRESS/FAILED — the build task will
 *       reconcile via {@link SqlDialect#isIndexValid} on its next pass).</li>
 *   <li><b>Hard-fail only on operator-caused corruption</b>: a COMPLETED
 *       row whose physical index is missing or {@code INVALID} — the
 *       executor cannot auto-recover from these without operator
 *       intervention. Every drift across the schema is collected and
 *       reported in a single {@link IllegalStateException} at the end of
 *       the pass (collect-then-throw, mirroring
 *       {@code SchemaHomology}'s {@code DifferenceWriter} pattern) so the
 *       operator sees every issue in one boot cycle.</li>
 * </ol>
 *
 * <p>The drift policy is intentionally <i>narrow</i> compared to the slim
 * branch's wide hard-fail: routine restarts during long-running builds
 * (Kubernetes pod evict, JVM restart) leave non-COMPLETED rows alongside a
 * physical index. The build task self-heals these via per-task
 * reconciliation. Only the COMPLETED-row anomalies remain as hard failures
 * — those represent state corruption no automated reconciliation can
 * safely fix.</p>
 *
 * <p>{@code SchemaHomology.checkIndex} does not compare {@code isDeferred()}
 * and only logs warnings on missing indexes — so the COMPLETED-row drift
 * checks must live here, not be delegated downstream.</p>
 *
 * <p>Reads persisted rows via {@link DeferredIndexesDAO#findAll()} — a
 * package-private concrete class that also backs {@link DeferredIndexServiceImpl}.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
public class DeferredIndexesModelEnricherImpl implements DeferredIndexesModelEnricher {

  private static final Log log = LogFactory.getLog(DeferredIndexesModelEnricherImpl.class);

  private final DeferredIndexesDAO dao;
  private final ConnectionResources connectionResources;
  private final UpgradeConfigAndContext config;


  /**
   * Constructs the enricher.
   *
   * @param dao persistence layer — provides the {@code findAll()} read at
   *     upgrade start. Package-private, not exposed to adopters.
   * @param connectionResources supplies the JDBC connection used by
   *     {@link SqlDialect#isIndexValid} when checking COMPLETED-row physicals
   *     for the {@code INVALID} drift case.
   * @param config upgrade configuration.
   */
  @Inject
  DeferredIndexesModelEnricherImpl(DeferredIndexesDAO dao,
                                   ConnectionResources connectionResources,
                                   UpgradeConfigAndContext config) {
    this.dao = dao;
    this.connectionResources = connectionResources;
    this.config = config;
  }


  @Override
  public Schema enrich(Schema physicalSchema, DeferredIndexSession session) {
    // Feature disabled, or DeferredIndexes table not yet created -- pass the schema through unchanged.
    if (shouldSkipEnrichment(physicalSchema)) {
      return physicalSchema;
    }

    // Read every tracking row in one go (built and unbuilt). If nothing is tracked, the
    // enricher has nothing to do; the visitor will INSERT new rows during this upgrade run.
    List<DeferredIndex> entries = dao.findAll();
    if (entries.isEmpty()) {
      log.debug("Skipping enrichment — DeferredIndexes table is empty");
      return physicalSchema;
    }

    // Seed the per-upgrade session with every persisted row before the visitor mutates anything,
    // so subsequent remove/rename/column operations cascade correctly to prior-upgrade rows.
    primeSession(entries, session);

    // (table -> (index -> row)) bucketed by upper-cased name for fast per-table lookup
    // while we walk the physical schema. We'll remove() as we consume each table's bucket;
    // anything left over after the walk is an orphan (tracking row references a missing table).
    Map<String, Map<String, DeferredIndex>> entriesByTable = bucketByTable(entries);

    SqlDialect dialect = connectionResources.sqlDialect();
    List<String> drifts = new ArrayList<>();
    // The connection is needed by dialect.isIndexValid() inside reconcileTable -- we hold it
    // open for the whole pass rather than borrow per-call. Closes via try-with-resources.
    try (Connection connection = connectionResources.getDataSource().getConnection()) {
      List<Table> enrichedTables = new ArrayList<>();
      // Track whether any table actually got rewritten -- avoids allocating a new Schema
      // when no tracked indexes were present (common case for an early upgrade run).
      boolean changed = false;
      for (Table physicalTable : physicalSchema.tables()) {
        Map<String, DeferredIndex> rowsForTable =
            entriesByTable.remove(physicalTable.getName().toUpperCase());
        if (rowsForTable == null || rowsForTable.isEmpty()) {
          // No tracking rows for this table -- nothing to virtualize, no drift to check.
          enrichedTables.add(physicalTable);
          continue;
        }
        // reconcileTable rewrites the index list (adds .deferred() flag to COMPLETED-row matches,
        // virtualizes non-COMPLETED rows, appends drift messages for COMPLETED-row anomalies).
        enrichedTables.add(reconcileTable(physicalTable, rowsForTable, dialect, connection, drifts));
        changed = true;
      }
      // Whatever's left in entriesByTable references tables not present in physicalSchema --
      // each of those is an operator-caused drift (table dropped without removing the row).
      collectOrphanedRowDrifts(entriesByTable, drifts);

      // Collect-then-throw: every drift across the schema is reported in a single exception
      // (mirrors SchemaHomology's DifferenceWriter pattern -- operator sees every issue at once).
      if (!drifts.isEmpty()) {
        throw new IllegalStateException(
            "DeferredIndexes drift detected (" + drifts.size() + " issue"
                + (drifts.size() == 1 ? "" : "s") + "):\n  - "
                + String.join("\n  - ", drifts));
      }
      return changed ? SchemaUtils.schema(enrichedTables) : physicalSchema;
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error opening connection for DeferredIndexes enrichment", e);
    }
  }


  /** Side-effect: every persisted row primes the session so visitor mutations
   *  cascade to all currently-declared deferred indexes. */
  private void primeSession(List<DeferredIndex> entries, DeferredIndexSession session) {
    for (DeferredIndex entry : entries) {
      session.prime(entry);
    }
  }


  /** Index entries by upper-cased (tableName, indexName) for fast lookup
   *  while walking the physical schema. */
  private Map<String, Map<String, DeferredIndex>> bucketByTable(List<DeferredIndex> entries) {
    Map<String, Map<String, DeferredIndex>> byTable = new HashMap<>();
    for (DeferredIndex entry : entries) {
      byTable
          .computeIfAbsent(entry.getTableName().toUpperCase(), k -> new HashMap<>())
          .put(entry.getIndexName().toUpperCase(), entry);
    }
    return byTable;
  }


  /**
   * Rebuilds the index list for one physical table by reconciling against
   * its tracking rows. Applies these rules in order:
   * <ul>
   *   <li>physical index matching a COMPLETED row → check
   *       {@link SqlDialect#isIndexValid} — VALID or unknown rebuilds with
   *       {@code .deferred()}; INVALID records a drift</li>
   *   <li>physical index matching a non-COMPLETED row → mark
   *       {@code .deferred()} and let the build task reconcile (the
   *       routine-restart case)</li>
   *   <li>tracking row with no matching physical → virtualize as deferred,
   *       unless COMPLETED in which case records a drift (operator-caused
   *       state corruption — manual recovery required)</li>
   * </ul>
   *
   * <p>Drift findings are appended to {@code drifts} rather than thrown; the
   * caller emits a single {@link IllegalStateException} after every table is
   * walked so the operator sees every issue in one boot cycle. Mirrors
   * {@code SchemaHomology}'s collect-then-report pattern.</p>
   */
  private Table reconcileTable(Table physicalTable,
                               Map<String, DeferredIndex> rowsForTable,
                               SqlDialect dialect,
                               Connection connection,
                               List<String> drifts) {
    Set<String> matchedRowNames = new HashSet<>();
    List<Index> indexes = new ArrayList<>();

    for (Index physical : physicalTable.indexes()) {
      DeferredIndex row = rowsForTable.get(physical.getName().toUpperCase());
      if (row == null) {
        indexes.add(physical);
        continue;
      }
      matchedRowNames.add(row.getIndexName().toUpperCase());
      if (row.getStatus() == DeferredIndexStatus.COMPLETED) {
        Optional<Boolean> valid = dialect.isIndexValid(connection, row.getTableName(), row.getIndexName());
        if (valid.orElse(true)) {
          indexes.add(asDeferred(physical));
        } else {
          drifts.add(
              "row for index '" + row.getIndexName()
                  + "' on table '" + row.getTableName() + "' is COMPLETED but the physical"
                  + " index is INVALID. Drop the invalid physical index manually, mark the row"
                  + " non-COMPLETED (e.g. PENDING) so the next build pass rebuilds it, and restart.");
        }
      } else {
        // Non-COMPLETED row + physical present is the routine-restart case.
        // The build task's next pass will see this via isIndexValid and reconcile
        // (mark COMPLETED if VALID, DROP+CREATE if INVALID).
        indexes.add(asDeferred(physical));
      }
    }

    for (DeferredIndex row : rowsForTable.values()) {
      if (matchedRowNames.contains(row.getIndexName().toUpperCase())) {
        continue;
      }
      if (row.getStatus() == DeferredIndexStatus.COMPLETED) {
        drifts.add(
            "row for index '" + row.getIndexName()
                + "' on table '" + row.getTableName() + "' is COMPLETED but the physical"
                + " index is missing (someone dropped a built index out-of-band). Either"
                + " restore the index from backup or mark the row non-COMPLETED (e.g. PENDING)"
                + " so the next build pass rebuilds it, then restart.");
        continue;
      }
      indexes.add(row.toIndex());
    }

    return table(physicalTable.getName())
        .columns(physicalTable.columns())
        .indexes(indexes);
  }


  /** Records a drift entry for every tracking row that references a table
   *  not in the physical schema. SchemaHomology would normally surface this
   *  later, but per-row messages here are clearer. */
  private void collectOrphanedRowDrifts(Map<String, Map<String, DeferredIndex>> remaining,
                                         List<String> drifts) {
    for (Map<String, DeferredIndex> rows : remaining.values()) {
      for (DeferredIndex row : rows.values()) {
        drifts.add(
            "row for index '" + row.getIndexName()
                + "' references table '" + row.getTableName() + "' which is not in the"
                + " physical schema. Reconcile manually before retrying.");
      }
    }
  }


  /**
   * Rebuilds an existing physical index with the {@code .deferred()} flag
   * applied. Preserves name, columns, and uniqueness.
   */
  private Index asDeferred(Index physical) {
    IndexBuilder builder = index(physical.getName()).columns(physical.columnNames());
    if (physical.isUnique()) {
      builder = builder.unique();
    }
    return builder.deferred();
  }


  /**
   * Early-exit checks: feature disabled or tracking table not yet created.
   * The third case (table exists but is empty) is handled inline in
   * {@code enrich} to avoid a double read.
   */
  private boolean shouldSkipEnrichment(Schema physicalSchema) {
    if (!config.isDeferredIndexCreationEnabled()) {
      log.debug("Skipping enrichment — feature disabled");
      return true;
    }
    if (!physicalSchema.tableExists(DatabaseUpgradeTableContribution.DEFERRED_INDEXES_NAME)) {
      log.debug("Skipping enrichment — DeferredIndexes table does not exist yet");
      return true;
    }
    return false;
  }
}
