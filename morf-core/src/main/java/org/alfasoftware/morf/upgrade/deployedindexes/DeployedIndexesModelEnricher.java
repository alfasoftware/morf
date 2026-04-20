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

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;

import com.google.inject.ImplementedBy;

/**
 * Merges the physical database schema with the {@code DeployedIndexes}
 * tracking table to produce an {@link EnrichedModel}: an enriched schema
 * (indexes carry the correct declarative
 * {@link org.alfasoftware.morf.metadata.Index#isDeferred()}, with
 * deferred-but-not-yet-built indexes added as virtual entries) plus a
 * companion {@link DeployedIndexState} recording operational facts
 * (physical presence per index).
 *
 * <p>Keeping the operational state out of the
 * {@link org.alfasoftware.morf.metadata.Index} model preserves the
 * declarative nature of the schema types. Questions like "is this index
 * physically there?" go to the {@link DeployedIndexState}, not to the
 * index itself.</p>
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
@ImplementedBy(DeployedIndexesModelEnricherImpl.class)
public interface DeployedIndexesModelEnricher {

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
  EnrichedModel enrich(Schema physicalSchema);


  /**
   * Convenience factory for the static upgrade path — wires up the DAO
   * from connection resources without exposing it to callers.
   *
   * @param connectionResources database connection resources.
   * @param config upgrade configuration.
   * @return a new enricher.
   */
  static DeployedIndexesModelEnricher create(ConnectionResources connectionResources,
                                              UpgradeConfigAndContext config) {
    DeployedIndexesDAO dao = new DeployedIndexesDAOImpl(
        new SqlScriptExecutorProvider(connectionResources),
        connectionResources,
        new DeployedIndexesStatementFactoryImpl());
    return new DeployedIndexesModelEnricherImpl(dao, config);
  }
}
