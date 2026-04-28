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

import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Default implementation of {@link DeployedIndexesModelEnricher} for the
 * slim invariant (tracking = deferred-only).
 *
 * <p>Responsibilities:</p>
 * <ol>
 *   <li><b>Prime the per-upgrade session</b> with every persisted tracking
 *       row so that remove/rename/column operations on indexes added by
 *       earlier upgrades emit correct DML against the existing rows.</li>
 *   <li><b>Virtualize unbuilt deferred indexes</b> (status not COMPLETED)
 *       into the schema so that {@code SchemaHomology.schemasMatch} treats
 *       them as declared — which they are — and does not treat them as
 *       missing from the physical schema.</li>
 * </ol>
 *
 * <p>Reads persisted rows via {@link DeployedIndexesDAO#findAll()} — a
 * package-private concrete class that also backs {@link DeployedIndexTrackerImpl}.</p>
 *
 * <p>Physical-vs-declared consistency for non-deferred indexes is not this
 * class's concern — {@code SchemaHomology} handles drift detection at
 * upgrade-path-finding time.</p>
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
  public EnrichedModel enrich(Schema physicalSchema, DeferredIndexSession session) {
    if (shouldSkipEnrichment(physicalSchema)) {
      return new EnrichedModel(physicalSchema, DeployedIndexState.empty());
    }

    List<DeployedIndex> entries = dao.findAll();
    if (entries.isEmpty()) {
      log.debug("Skipping enrichment — DeployedIndexes table is empty");
      return new EnrichedModel(physicalSchema, DeployedIndexState.empty());
    }

    // Prime the session with every persisted row — remove/rename/column
    // operations in this session need the in-memory cache populated to emit
    // correct DML against rows persisted by prior upgrades.
    for (DeployedIndex entry : entries) {
      session.prime(entry);
    }

    // Bucket unbuilt entries by upper-cased table name. COMPLETED entries are
    // already in the physical schema, so no virtualization is needed (and no
    // state entry — UNKNOWN is the correct default for the visitor).
    Map<String, List<DeployedIndex>> unbuiltByTable = new HashMap<>();
    for (DeployedIndex entry : entries) {
      if (entry.getStatus() == DeployedIndexStatus.COMPLETED) {
        continue;
      }
      unbuiltByTable
          .computeIfAbsent(entry.getTableName().toUpperCase(), k -> new ArrayList<>())
          .add(entry);
    }

    if (unbuiltByTable.isEmpty()) {
      return new EnrichedModel(physicalSchema, DeployedIndexState.empty());
    }

    Map<IndexKey, IndexPresence> observedPresence = new HashMap<>();
    List<Table> enrichedTables = new ArrayList<>();
    boolean changed = false;

    for (Table physicalTable : physicalSchema.tables()) {
      List<DeployedIndex> unbuilt = unbuiltByTable.remove(physicalTable.getName().toUpperCase());
      if (unbuilt == null || unbuilt.isEmpty()) {
        enrichedTables.add(physicalTable);
        continue;
      }

      List<Index> indexes = new ArrayList<>(physicalTable.indexes());
      for (DeployedIndex entry : unbuilt) {
        indexes.add(entry.toIndex());
        observedPresence.put(new IndexKey(physicalTable.getName(), entry.getIndexName()),
            IndexPresence.ABSENT);
      }
      enrichedTables.add(table(physicalTable.getName())
          .columns(physicalTable.columns())
          .indexes(indexes));
      changed = true;
    }

    // Any unbuilt entries left in the map reference tables not in the physical
    // schema — likely a crashed/partial upgrade or out-of-band DROP TABLE.
    // We don't hard-fail (SchemaHomology will surface real drift later); log
    // and move on.
    if (!unbuiltByTable.isEmpty() && log.isDebugEnabled()) {
      for (List<DeployedIndex> stragglers : unbuiltByTable.values()) {
        for (DeployedIndex e : stragglers) {
          log.debug("Unbuilt deferred row for table not in schema: "
              + e.getTableName() + "." + e.getIndexName());
        }
      }
    }

    Schema schema = changed ? SchemaUtils.schema(enrichedTables) : physicalSchema;
    return new EnrichedModel(schema, new DeployedIndexState(observedPresence));
  }


  /**
   * Early-exit checks that produce an empty state and return the schema
   * unchanged: feature disabled or tracking table not yet created. The
   * third case (table exists but is empty) is handled inline in {@code enrich}
   * to avoid a double read.
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
