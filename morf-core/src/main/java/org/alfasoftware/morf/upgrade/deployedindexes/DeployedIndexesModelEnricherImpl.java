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
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * Default implementation of {@link DeployedIndexesModelEnricher} for the
 * "row-existence = declared deferred" model.
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
 *       them as declared.</li>
 *   <li><b>Hard-fail on drift</b>: a COMPLETED row with no matching physical
 *       index, or a non-COMPLETED row whose physical index already exists,
 *       throws {@link IllegalStateException}. Morf does not auto-heal.</li>
 * </ol>
 *
 * <p>Reads persisted rows via {@link DeployedIndexesDAO#findAll()} — a
 * package-private concrete class that also backs {@link DeployedIndexTrackerImpl}.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
public class DeployedIndexesModelEnricherImpl implements DeployedIndexesModelEnricher {

  private static final Log log = LogFactory.getLog(DeployedIndexesModelEnricherImpl.class);

  private final DeployedIndexesDAO dao;
  private final UpgradeConfigAndContext config;


  /**
   * Constructs the enricher.
   *
   * @param dao persistence layer — provides the {@code findAll()} read at
   *     upgrade start. Package-private, not exposed to adopters.
   * @param config upgrade configuration.
   */
  @Inject
  DeployedIndexesModelEnricherImpl(DeployedIndexesDAO dao, UpgradeConfigAndContext config) {
    this.dao = dao;
    this.config = config;
  }


  @Override
  public Schema enrich(Schema physicalSchema, DeferredIndexSession session) {
    if (shouldSkipEnrichment(physicalSchema)) {
      return physicalSchema;
    }

    List<DeployedIndex> entries = dao.findAll();
    if (entries.isEmpty()) {
      log.debug("Skipping enrichment — DeployedIndexes table is empty");
      return physicalSchema;
    }

    // Prime the session with every persisted row.
    for (DeployedIndex entry : entries) {
      session.prime(entry);
    }

    // Bucket entries by upper-cased table name for fast lookup as we walk
    // the physical schema.
    Map<String, Map<String, DeployedIndex>> entriesByTable = new HashMap<>();
    for (DeployedIndex entry : entries) {
      entriesByTable
          .computeIfAbsent(entry.getTableName().toUpperCase(), k -> new HashMap<>())
          .put(entry.getIndexName().toUpperCase(), entry);
    }

    List<Table> enrichedTables = new ArrayList<>();
    boolean changed = false;

    for (Table physicalTable : physicalSchema.tables()) {
      Map<String, DeployedIndex> rowsForTable =
          entriesByTable.remove(physicalTable.getName().toUpperCase());
      if (rowsForTable == null || rowsForTable.isEmpty()) {
        enrichedTables.add(physicalTable);
        continue;
      }

      // Rebuild the table's index list:
      //   - physical index matching a COMPLETED row → rebuild with .deferred()
      //   - physical index matching a non-COMPLETED row → drift, throw
      //   - tracking row with no matching physical index → virtualize as deferred
      Set<String> matchedRowNames = new HashSet<>();
      List<Index> indexes = new ArrayList<>();

      for (Index physical : physicalTable.indexes()) {
        DeployedIndex row = rowsForTable.get(physical.getName().toUpperCase());
        if (row == null) {
          indexes.add(physical);
          continue;
        }
        matchedRowNames.add(row.getIndexName().toUpperCase());
        if (row.getStatus() == DeployedIndexStatus.COMPLETED) {
          // Rebuild as declared-deferred so isDeferred() is preserved.
          indexes.add(asDeferred(physical));
          changed = true;
        } else {
          // Non-COMPLETED row with a matching physical index — drift.
          throw new IllegalStateException(
              "DeployedIndexes drift: row for index '" + row.getIndexName()
                  + "' on table '" + row.getTableName() + "' has status " + row.getStatus()
                  + " but the physical index already exists. Reconcile manually before retrying.");
        }
      }

      // Virtualize tracking rows whose physical index isn't there.
      for (DeployedIndex row : rowsForTable.values()) {
        if (matchedRowNames.contains(row.getIndexName().toUpperCase())) {
          continue;
        }
        if (row.getStatus() == DeployedIndexStatus.COMPLETED) {
          // COMPLETED row with no matching physical — drift.
          throw new IllegalStateException(
              "DeployedIndexes drift: row for index '" + row.getIndexName()
                  + "' on table '" + row.getTableName() + "' is COMPLETED but the physical"
                  + " index is missing. Reconcile manually before retrying.");
        }
        indexes.add(row.toIndex());
        changed = true;
      }

      enrichedTables.add(table(physicalTable.getName())
          .columns(physicalTable.columns())
          .indexes(indexes));
    }

    // Any rows left in the map reference tables not in the physical schema —
    // a different drift class. SchemaHomology would normally surface this,
    // but at this point we have a row pointing nowhere; surfacing it here
    // gives a clearer message.
    if (!entriesByTable.isEmpty()) {
      List<DeployedIndex> stragglers = new ArrayList<>();
      for (Map<String, DeployedIndex> rows : entriesByTable.values()) {
        stragglers.addAll(rows.values());
      }
      DeployedIndex first = stragglers.get(0);
      throw new IllegalStateException(
          "DeployedIndexes drift: row for index '" + first.getIndexName()
              + "' references table '" + first.getTableName() + "' which is not in the"
              + " physical schema. Reconcile manually before retrying."
              + (stragglers.size() > 1 ? " (" + (stragglers.size() - 1) + " more like this.)" : ""));
    }

    return changed ? SchemaUtils.schema(enrichedTables) : physicalSchema;
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
    if (!physicalSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)) {
      log.debug("Skipping enrichment — DeployedIndexes table does not exist yet");
      return true;
    }
    return false;
  }
}
