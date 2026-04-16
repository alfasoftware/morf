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
 * tracking table to produce:
 * <ul>
 *   <li>a schema where each index carries the correct declarative
 *       {@link Index#isDeferred()} (propagated from the tracking row),
 *       with deferred-but-not-yet-built indexes added as virtual entries;
 *       and</li>
 *   <li>a {@link DeployedIndexState} that records operational facts
 *       (physical presence per index) for the visitor to consult when
 *       deciding DDL strategies.</li>
 * </ul>
 *
 * <p>Keeping the operational state out of the {@link Index} model preserves
 * the declarative nature of the schema types. Questions like "is this
 * index physically there?" go to the {@link DeployedIndexState}, not to
 * the index itself.</p>
 *
 * <p>Consistency validation is performed during enrichment:</p>
 * <ul>
 *   <li>Non-deferred index missing from DB &rarr; error</li>
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
  public DeployedIndexesModelEnricher(DeployedIndexesDAO dao, UpgradeConfigAndContext config) {
    this.dao = dao;
    this.config = config;
  }


  /**
   * Non-Guice constructor for use in the static upgrade path.
   *
   * @param dao DAO for reading DeployedIndexes.
   */
  public DeployedIndexesModelEnricher(DeployedIndexesDAO dao) {
    this(dao, new UpgradeConfigAndContext());
  }


  /**
   * Enriches the physical schema with {@code DeployedIndexes} metadata
   * and produces a companion {@link DeployedIndexState}.
   *
   * <p>If the feature is disabled or the {@code DeployedIndexes} table does
   * not yet exist, the physical schema is returned unchanged alongside an
   * empty state.</p>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @return the enrichment result: schema + operational state.
   * @throws IllegalStateException if consistency validation fails.
   */
  public Result enrich(Schema physicalSchema) {
    if (!config.isDeferredIndexCreationEnabled()) {
      return new Result(physicalSchema, DeployedIndexState.empty());
    }

    if (!physicalSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)) {
      log.debug("DeployedIndexes table does not exist yet — returning physical schema unchanged");
      return new Result(physicalSchema, DeployedIndexState.empty());
    }

    List<DeployedIndex> allEntries = dao.findAll();
    if (allEntries.isEmpty()) {
      log.debug("DeployedIndexes table is empty — returning physical schema unchanged");
      return new Result(physicalSchema, DeployedIndexState.empty());
    }

    // Build a lookup: tableName (upper) -> indexName (upper) -> entry
    Map<String, Map<String, DeployedIndex>> entryMap = buildEntryMap(allEntries);
    Map<String, IndexPresence> presence = new HashMap<>();

    List<Table> enrichedTables = new ArrayList<>();
    boolean changed = false;

    for (Table physicalTable : physicalSchema.tables()) {
      // Skip Morf infrastructure tables
      if (isMorfInfrastructureTable(physicalTable.getName())) {
        enrichedTables.add(physicalTable);
        continue;
      }

      Map<String, DeployedIndex> tableEntries = entryMap.getOrDefault(
          physicalTable.getName().toUpperCase(), new HashMap<>());

      List<Index> rebuiltIndexes = new ArrayList<>();
      boolean tableChanged = false;

      // Physical indexes: rebuild with correct deferred flag from tracking
      for (Index physicalIndex : physicalTable.indexes()) {
        if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(physicalIndex.getName())) {
          rebuiltIndexes.add(physicalIndex);
          continue;
        }

        DeployedIndex entry = tableEntries.remove(physicalIndex.getName().toUpperCase());
        if (entry == null) {
          // Physical index not tracked — schema inconsistency after initial population
          throw new IllegalStateException(
              "Index [" + physicalIndex.getName() + "] on table [" + physicalTable.getName()
              + "] exists in the database but is not tracked in the DeployedIndexes table. "
              + "This indicates a schema inconsistency.");
        }
        rebuiltIndexes.add(rebuildIndex(physicalIndex, entry.isIndexDeferred()));
        presence.put(DeployedIndexState.key(physicalTable.getName(), physicalIndex.getName()), IndexPresence.PRESENT);
        tableChanged = true;
      }

      // Remaining entries: declared in DeployedIndexes but not physically present
      for (DeployedIndex entry : tableEntries.values()) {
        if (!entry.isIndexDeferred()) {
          throw new IllegalStateException(
              "Non-deferred index [" + entry.getIndexName() + "] on table [" + entry.getTableName()
              + "] is tracked in DeployedIndexes but does not exist in the database. "
              + "This indicates a schema inconsistency.");
        }
        // Deferred index not yet built — add as a virtual declarative index
        rebuiltIndexes.add(entry.toIndex());
        presence.put(DeployedIndexState.key(physicalTable.getName(), entry.getIndexName()), IndexPresence.ABSENT);
        tableChanged = true;
      }

      if (tableChanged) {
        enrichedTables.add(table(physicalTable.getName())
            .columns(physicalTable.columns())
            .indexes(rebuiltIndexes));
        changed = true;
      } else {
        enrichedTables.add(physicalTable);
      }
    }

    // Check for entries referencing tables that don't exist in the physical schema
    for (Map.Entry<String, Map<String, DeployedIndex>> tableGroup : entryMap.entrySet()) {
      if (tableGroup.getValue().isEmpty()) {
        continue;
      }
      boolean isMorfTable = false;
      for (Table t : physicalSchema.tables()) {
        if (t.getName().toUpperCase().equals(tableGroup.getKey())) {
          isMorfTable = isMorfInfrastructureTable(t.getName());
          break;
        }
      }
      if (!isMorfTable) {
        for (DeployedIndex orphan : tableGroup.getValue().values()) {
          log.warn("DeployedIndexes entry for index [" + orphan.getIndexName()
              + "] on table [" + orphan.getTableName() + "] references a table not in the schema");
        }
      }
    }

    Schema schema = changed ? SchemaUtils.schema(enrichedTables) : physicalSchema;
    return new Result(schema, new DeployedIndexState(presence));
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


  private Map<String, Map<String, DeployedIndex>> buildEntryMap(List<DeployedIndex> entries) {
    Map<String, Map<String, DeployedIndex>> map = new HashMap<>();
    for (DeployedIndex entry : entries) {
      map.computeIfAbsent(entry.getTableName().toUpperCase(), k -> new HashMap<>())
          .put(entry.getIndexName().toUpperCase(), entry);
    }
    return map;
  }


  private boolean isMorfInfrastructureTable(String tableName) {
    return DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME.equalsIgnoreCase(tableName)
        || DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME.equalsIgnoreCase(tableName)
        || DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME.equalsIgnoreCase(tableName);
  }


  /**
   * Output of {@link #enrich(Schema)}: the enriched schema plus the
   * companion {@link DeployedIndexState} carrying operational facts.
   */
  public static final class Result {
    private final Schema schema;
    private final DeployedIndexState state;

    /**
     * Constructs an enrichment result.
     *
     * @param schema the enriched schema.
     * @param state the companion operational state.
     */
    public Result(Schema schema, DeployedIndexState state) {
      this.schema = schema;
      this.state = state;
    }

    /**
     * @return the enriched schema. Indexes carry the correct
     *         {@link Index#isDeferred()} and deferred-not-yet-built
     *         indexes appear as virtual entries.
     */
    public Schema getSchema() {
      return schema;
    }

    /**
     * @return operational state: physical presence per index.
     */
    public DeployedIndexState getState() {
      return state;
    }
  }
}
