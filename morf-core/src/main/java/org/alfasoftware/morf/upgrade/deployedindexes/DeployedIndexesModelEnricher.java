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
 * (with deferred-but-not-yet-built indexes added as virtual entries) plus a
 * companion {@link DeployedIndexState} recording operational facts (physical
 * presence per index).
 *
 * <p><b>Slim invariant</b> (this branch): only deferred indexes are tracked
 * in the {@code DeployedIndexes} table. The enricher's job is to
 * (a) <b>prime</b> the per-session {@link DeployedIndexesService} with every
 * persisted row so that in-session remove/rename/column operations against
 * prior-upgrade deferred rows generate correct DML; and (b) <b>virtualize</b>
 * unbuilt deferred indexes (status not COMPLETED) into the schema so
 * {@code SchemaHomology.schemasMatch} treats them as declared.</p>
 *
 * <p>Physical-vs-declared consistency for non-deferred indexes is NOT this
 * class's concern — {@code SchemaHomology} handles drift detection at
 * upgrade-path-finding time.</p>
 *
 * <p>Keeping operational state out of the
 * {@link org.alfasoftware.morf.metadata.Index} model preserves the
 * declarative nature of the schema types. Questions like "is this index
 * physically there?" go to the {@link DeployedIndexState}, not to the
 * index itself.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeployedIndexesModelEnricherImpl.class)
public interface DeployedIndexesModelEnricher {

  /**
   * Enriches the physical schema with {@code DeployedIndexes} metadata and
   * primes the per-session service with persisted tracking rows.
   *
   * <p>If the feature is disabled, the {@code DeployedIndexes} table does
   * not yet exist, or the table is empty, the physical schema is returned
   * unchanged alongside an empty state — and the service is not primed.</p>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @param service the per-session service to prime with persisted rows —
   *     its in-memory map is populated as a side-effect so that the
   *     visitor's remove/rename/column operations emit correct DML against
   *     prior-upgrade tracking rows.
   * @return the enrichment result: schema + operational state.
   */
  EnrichedModel enrich(Schema physicalSchema, DeployedIndexesService service);


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
