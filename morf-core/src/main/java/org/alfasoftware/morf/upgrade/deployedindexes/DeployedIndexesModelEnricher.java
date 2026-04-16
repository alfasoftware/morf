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

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProviderUtils;
import org.alfasoftware.morf.metadata.ObservedIndex;
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
 * Enriches a physical database schema with metadata from the DeployedIndexes
 * table. After enrichment, every {@link Index} in the schema carries
 * {@link Index#isDeferred()} and {@link Index#isPhysicallyPresent()} properties
 * that the visitor uses for DDL decisions.
 *
 * <p>Consistency validation is performed during enrichment:</p>
 * <ul>
 *   <li>Non-deferred index missing from DB &rarr; error</li>
 *   <li>Physical index not tracked in DeployedIndexes (after initial population,
 *       excluding _PRF indexes) &rarr; error</li>
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
   * Enriches the given physical schema with DeployedIndexes metadata.
   * Returns a new schema where each index carries {@code isDeferred()}
   * and {@code isPhysicallyPresent()} from the DeployedIndexes table.
   *
   * <p>If the DeployedIndexes table does not exist in the schema, the
   * source schema is returned unchanged (pre-initial-population state).</p>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @return the enriched schema.
   * @throws IllegalStateException if consistency validation fails.
   */
  public Schema enrichSchema(Schema physicalSchema) {
    if (!config.isDeferredIndexCreationEnabled()) {
      return physicalSchema;
    }

    if (!physicalSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_INDEXES_NAME)) {
      log.debug("DeployedIndexes table does not exist yet — returning physical schema unchanged");
      return physicalSchema;
    }

    List<DeployedIndexEntry> allEntries = dao.findAll();
    if (allEntries.isEmpty()) {
      log.debug("DeployedIndexes table is empty — returning physical schema unchanged");
      return physicalSchema;
    }

    // Build a lookup: tableName (upper) -> indexName (upper) -> entry
    Map<String, Map<String, DeployedIndexEntry>> entryMap = buildEntryMap(allEntries);

    // Enrich each table's indexes
    List<Table> enrichedTables = new ArrayList<>();
    boolean changed = false;

    for (Table table : physicalSchema.tables()) {
      // Skip Morf infrastructure tables
      if (isMorfInfrastructureTable(table.getName())) {
        enrichedTables.add(table);
        continue;
      }

      Map<String, DeployedIndexEntry> tableEntries = entryMap.getOrDefault(
          table.getName().toUpperCase(), new HashMap<>());

      List<Index> enrichedIndexes = new ArrayList<>();
      boolean tableChanged = false;

      // Process physical indexes
      for (Index physicalIndex : table.indexes()) {
        if (DatabaseMetaDataProviderUtils.shouldIgnoreIndex(physicalIndex.getName())) {
          enrichedIndexes.add(physicalIndex);
          continue;
        }

        DeployedIndexEntry entry = tableEntries.remove(physicalIndex.getName().toUpperCase());
        if (entry != null) {
          // Physical index tracked in DeployedIndexes — enrich
          enrichedIndexes.add(new ObservedIndex(physicalIndex, entry.isIndexDeferred(), true));
          tableChanged = true;
        } else {
          // Physical index NOT in DeployedIndexes — error after initial population
          throw new IllegalStateException(
              "Index [" + physicalIndex.getName() + "] on table [" + table.getName()
              + "] exists in the database but is not tracked in the DeployedIndexes table. "
              + "This indicates a schema inconsistency.");
        }
      }

      // Remaining entries: declared in DeployedIndexes but not physically present
      for (DeployedIndexEntry entry : tableEntries.values()) {
        if (!entry.isIndexDeferred()) {
          // Non-deferred index missing from DB — error
          throw new IllegalStateException(
              "Non-deferred index [" + entry.getIndexName() + "] on table [" + entry.getTableName()
              + "] is tracked in DeployedIndexes but does not exist in the database. "
              + "This indicates a schema inconsistency.");
        }
        // Deferred index not yet built — add as virtual
        Index virtualIndex = entry.toIndex();
        enrichedIndexes.add(new ObservedIndex(virtualIndex, true, false));
        tableChanged = true;
      }

      if (tableChanged) {
        enrichedTables.add(table(table.getName()).columns(table.columns()).indexes(enrichedIndexes));
        changed = true;
      } else {
        enrichedTables.add(table);
      }
    }

    // Check for entries referencing tables that don't exist in the physical schema
    for (Map.Entry<String, Map<String, DeployedIndexEntry>> tableGroup : entryMap.entrySet()) {
      String tableNameUpper = tableGroup.getKey();
      // Skip if this table was already processed (entries consumed above)
      if (tableGroup.getValue().isEmpty()) {
        continue;
      }
      // Check if this is a Morf infrastructure table
      boolean isMorfTable = false;
      for (Table t : physicalSchema.tables()) {
        if (t.getName().toUpperCase().equals(tableNameUpper)) {
          isMorfTable = isMorfInfrastructureTable(t.getName());
          break;
        }
      }
      if (!isMorfTable) {
        for (DeployedIndexEntry orphan : tableGroup.getValue().values()) {
          log.warn("DeployedIndexes entry for index [" + orphan.getIndexName()
              + "] on table [" + orphan.getTableName() + "] references a table not in the schema");
        }
      }
    }

    if (!changed) {
      return physicalSchema;
    }

    return SchemaUtils.schema(enrichedTables);
  }


  private Map<String, Map<String, DeployedIndexEntry>> buildEntryMap(List<DeployedIndexEntry> entries) {
    Map<String, Map<String, DeployedIndexEntry>> map = new HashMap<>();
    for (DeployedIndexEntry entry : entries) {
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
}
