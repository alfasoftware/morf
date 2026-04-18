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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils;
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
 * Merges the physical database schema with the {@code DeployedIndexes}
 * tracking table to produce an {@link EnrichedModel}: an enriched schema
 * (indexes carry the correct declarative {@link Index#isDeferred()}, with
 * deferred-but-not-yet-built indexes added as virtual entries) plus a
 * companion {@link DeployedIndexState} recording operational facts
 * (physical presence per index).
 *
 * <p>Keeping the operational state out of the {@link Index} model preserves
 * the declarative nature of the schema types. Questions like "is this
 * index physically there?" go to the {@link DeployedIndexState}, not to
 * the index itself.</p>
 *
 * <p>Consistency validation is performed during enrichment:</p>
 * <ul>
 *   <li>Non-deferred index tracked but missing from DB &rarr; error</li>
 *   <li>Physical index not tracked in DeployedIndexes (after initial population,
 *       excluding {@code _PRF} indexes) &rarr; error</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
public class DeployedIndexesModelEnricher {

  private static final Log log = LogFactory.getLog(DeployedIndexesModelEnricher.class);

  private final DeployedIndexesDAO dao;
  private final UpgradeConfigAndContext config;


  /**
   * Constructs the enricher.
   *
   * @param dao DAO for reading DeployedIndexes.
   * @param config upgrade configuration.
   */
  @Inject
  DeployedIndexesModelEnricher(DeployedIndexesDAO dao, UpgradeConfigAndContext config) {
    this.dao = dao;
    this.config = config;
  }


  /**
   * Convenience factory for the static upgrade path — wires up the DAO
   * from connection resources without exposing it to callers.
   *
   * @param connectionResources database connection resources.
   * @param config upgrade configuration.
   * @return a new enricher.
   */
  public static DeployedIndexesModelEnricher create(
      org.alfasoftware.morf.jdbc.ConnectionResources connectionResources,
      UpgradeConfigAndContext config) {
    DeployedIndexesDAO dao = new DeployedIndexesDAOImpl(
        new org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider(connectionResources),
        connectionResources,
        new DeployedIndexesStatementFactory());
    return new DeployedIndexesModelEnricher(dao, config);
  }


  /**
   * Enriches the physical schema with {@code DeployedIndexes} metadata
   * and produces a companion {@link DeployedIndexState}.
   *
   * <p>If the feature is disabled, the {@code DeployedIndexes} table does
   * not yet exist, or the table is empty, the physical schema is returned
   * unchanged alongside an empty state.</p>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @return the enrichment result: schema + operational state.
   * @throws IllegalStateException if consistency validation fails.
   */
  public EnrichedModel enrich(Schema physicalSchema) {
    if (shouldSkipEnrichment(physicalSchema)) {
      return new EnrichedModel(physicalSchema, DeployedIndexState.empty());
    }

    List<DeployedIndex> entries = dao.findAll();
    if (entries.isEmpty()) {
      log.debug("Skipping enrichment — DeployedIndexes table is empty");
      return new EnrichedModel(physicalSchema, DeployedIndexState.empty());
    }

    // tableName (upper) -> indexName (upper) -> entry. The inner maps are
    // mutated as entries are consumed by the physical-indexes pass; what
    // remains is, by invariant, "tracked but not physically present".
    Map<String, Map<String, DeployedIndex>> trackingRowsByTable = buildTrackingRowsByTable(entries);
    Map<String, IndexPresence> observedPresence = new HashMap<>();

    List<Table> enrichedTables = new ArrayList<>();
    boolean changed = false;

    for (Table physicalTable : physicalSchema.tables()) {
      Optional<Table> enriched = enrichTable(physicalTable,
          trackingRowsByTable.getOrDefault(physicalTable.getName().toUpperCase(), new HashMap<>()),
          observedPresence);
      enrichedTables.add(enriched.orElse(physicalTable));
      changed |= enriched.isPresent();
    }

    validateNoOrphanTrackingRows(trackingRowsByTable);

    Schema schema = changed ? SchemaUtils.schema(enrichedTables) : physicalSchema;
    return new EnrichedModel(schema, new DeployedIndexState(observedPresence));
  }


  /**
   * Early-exit checks that produce an empty state and return the schema
   * unchanged: feature disabled or tracking table not yet created. The
   * third case (table exists but is empty) is handled inline in {@code enrich}
   * to avoid a double {@code dao.findAll()} call.
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


  /**
   * Enriches a single table's indexes. Returns a new {@link Table} if any
   * index changed (rebuilt with deferred flag or a virtual deferred added);
   * {@link Optional#empty()} if no change was needed and the caller should
   * keep the original.
   *
   * @param physicalTable the physical table.
   * @param tableEntries tracking rows for this table, keyed by upper-case
   *     index name; this map is mutated — consumed entries are removed.
   * @param presence output map: operational state is written into this.
   */
  private Optional<Table> enrichTable(Table physicalTable,
                                       Map<String, DeployedIndex> tableEntries,
                                       Map<String, IndexPresence> observedPresence) {
    List<Index> rebuiltIndexes = new ArrayList<>();
    boolean changed = processPhysicalIndexes(physicalTable, tableEntries, rebuiltIndexes, observedPresence);
    changed |= processRemainingTrackingEntries(physicalTable.getName(), tableEntries, rebuiltIndexes, observedPresence);

    if (!changed) {
      return Optional.empty();
    }
    return Optional.of(table(physicalTable.getName())
        .columns(physicalTable.columns())
        .indexes(rebuiltIndexes));
  }


  /**
   * Walks the table's physical indexes. For each index, rebuilds it with
   * the declarative deferred flag from its tracking row (if any) and
   * records PRESENT in the state.
   *
   * <p>Two corner cases:</p>
   * <ul>
   *   <li>{@code _PRF} indexes are performance-testing indexes excluded
   *       from DeployedIndexes by design — they pass through without
   *       tracking validation.</li>
   *   <li>Any other physical index without a matching tracking row is a
   *       hard error: the schema is inconsistent. Recovering silently
   *       would risk losing metadata about whether the index was meant
   *       to be deferred.</li>
   * </ul>
   *
   * <p>Consumed tracking entries are removed from {@code tableEntries}.
   * The leftover entries after this loop are, by construction, "tracked
   * but not physically present" — the virtual-deferred candidates.</p>
   *
   * @return {@code true} if at least one index was rebuilt or added.
   */
  private boolean processPhysicalIndexes(Table physicalTable,
                                          Map<String, DeployedIndex> tableEntries,
                                          List<Index> rebuiltIndexes,
                                          Map<String, IndexPresence> observedPresence) {
    boolean changed = false;
    for (Index physicalIndex : physicalTable.indexes()) {
      if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(physicalIndex.getName())) {
        rebuiltIndexes.add(physicalIndex);
        continue;
      }

      DeployedIndex entry = tableEntries.remove(physicalIndex.getName().toUpperCase());
      if (entry == null) {
        throw new IllegalStateException(
            "Index [" + physicalIndex.getName() + "] on table [" + physicalTable.getName()
            + "] exists in the database but is not tracked in the DeployedIndexes table. "
            + "This indicates a schema inconsistency.");
      }
      rebuiltIndexes.add(rebuildIndex(physicalIndex, entry.isIndexDeferred()));
      observedPresence.put(DeployedIndexState.key(physicalTable.getName(), physicalIndex.getName()),
          IndexPresence.PRESENT);
      changed = true;
    }
    return changed;
  }


  /**
   * Adds a virtual declarative index for every tracking row that wasn't
   * matched by a physical index (i.e. "deferred but not yet built").
   *
   * <p>A non-deferred leftover is a hard error: the schema is
   * inconsistent (an index that was once built is now missing, and it's
   * not safe to silently recreate it — the original intent may have been
   * different).</p>
   *
   * @return {@code true} if at least one virtual index was added.
   */
  private boolean processRemainingTrackingEntries(String tableName,
                                                   Map<String, DeployedIndex> remainingEntries,
                                                   List<Index> rebuiltIndexes,
                                                   Map<String, IndexPresence> observedPresence) {
    boolean changed = false;
    for (DeployedIndex entry : remainingEntries.values()) {
      if (!entry.isIndexDeferred()) {
        throw new IllegalStateException(
            "Non-deferred index [" + entry.getIndexName() + "] on table [" + entry.getTableName()
            + "] is tracked in DeployedIndexes but does not exist in the database. "
            + "This indicates a schema inconsistency.");
      }
      rebuiltIndexes.add(entry.toIndex());
      observedPresence.put(DeployedIndexState.key(tableName, entry.getIndexName()), IndexPresence.ABSENT);
      changed = true;
    }
    // All entries have been consumed (either rebuilt as virtual deferred indexes or thrown
    // above). Clear so validateNoOrphanTrackingRows sees only tables not in the schema.
    remainingEntries.clear();
    return changed;
  }


  /**
   * Hard-fails on any tracking row whose table isn't in the physical schema.
   *
   * <p>An orphan row cannot be produced by correct Morf operation:
   * {@code RemoveTable} emits a matching {@code DELETE FROM DeployedIndexes}
   * alongside the {@code DROP TABLE}, and {@code RenameTable} emits a
   * matching {@code UPDATE} to the tracking row. So an orphan indicates
   * either a visitor bug, a crashed/partial upgrade, a manual DROP TABLE
   * outside Morf, or a restored DB snapshot out of sync with the tracking
   * table — all "something went wrong outside the normal path", which is
   * the same severity class as a non-deferred tracked index missing from
   * the DB (already a hard error).</p>
   */
  private void validateNoOrphanTrackingRows(Map<String, Map<String, DeployedIndex>> trackingRowsByTable) {
    for (Map<String, DeployedIndex> byIndex : trackingRowsByTable.values()) {
      if (!byIndex.isEmpty()) {
        DeployedIndex orphan = byIndex.values().iterator().next();
        throw new IllegalStateException(
            "DeployedIndexes entry for index [" + orphan.getIndexName()
            + "] on table [" + orphan.getTableName()
            + "] references a table not in the schema. "
            + "This indicates a schema inconsistency.");
      }
    }
  }


  /**
   * Rebuilds the given physical index carrying the deferred flag from the
   * tracking table. Uses the public {@code SchemaUtils} builder so the
   * result is a plain declarative {@link Index}.
   */
  private Index rebuildIndex(Index physicalIndex, boolean deferred) {
    SchemaUtils.IndexBuilder builder = index(physicalIndex.getName()).columns(physicalIndex.columnNames());
    if (physicalIndex.isUnique()) {
      builder = builder.unique();
    }
    if (deferred) {
      builder = builder.deferred();
    }
    return builder;
  }


  /**
   * Buckets tracking rows by upper-cased table name → upper-cased index name → row.
   */
  private Map<String, Map<String, DeployedIndex>> buildTrackingRowsByTable(List<DeployedIndex> entries) {
    Map<String, Map<String, DeployedIndex>> map = new HashMap<>();
    for (DeployedIndex entry : entries) {
      map.computeIfAbsent(entry.getTableName().toUpperCase(), k -> new HashMap<>())
          .put(entry.getIndexName().toUpperCase(), entry);
    }
    return map;
  }
}
