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
 * tracking table. Returns an enriched {@link Schema} where built-deferred
 * indexes carry the {@code .deferred()} flag and unbuilt-deferred rows
 * are virtualized as declared indexes.
 *
 * <p><b>Slim invariant</b> (this branch): only deferred indexes are tracked
 * in the {@code DeployedIndexes} table. A row exists in the table iff the
 * index is currently declared {@code .deferred()}. The enricher's job is to
 * (a) <b>prime</b> the per-upgrade {@link DeferredIndexSession} with every
 * persisted row so that in-session mutations (remove/rename/column) cascade
 * correctly to all currently-declared deferred indexes; (b) <b>rebuild</b>
 * COMPLETED-row physical indexes with the {@code .deferred()} flag so
 * {@code Index.isDeferred()} is durable across the build lifecycle; and
 * (c) <b>virtualize</b> non-COMPLETED rows (PENDING/IN_PROGRESS/FAILED)
 * into the schema so {@code SchemaHomology.schemasMatch} treats them as
 * declared.</p>
 *
 * <p>Drift between the tracking table and the physical schema is treated as
 * a fatal error — {@link IllegalStateException} is thrown. Morf does not
 * auto-heal indexes elsewhere, so the enricher follows the same policy.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeployedIndexesModelEnricherImpl.class)
public interface DeployedIndexesModelEnricher {

  /**
   * Enriches the physical schema with {@code DeployedIndexes} metadata and
   * primes the per-upgrade session with persisted tracking rows.
   *
   * <p>If the feature is disabled, the {@code DeployedIndexes} table does
   * not yet exist, or the table is empty, the physical schema is returned
   * unchanged — and the session is not primed.</p>
   *
   * <p>Otherwise:</p>
   * <ul>
   *   <li>Every persisted row primes the session (so the visitor's
   *       remove/rename/column operations emit correct DML against
   *       prior-upgrade tracking rows).</li>
   *   <li>{@code COMPLETED} rows whose physical index exists are rebuilt
   *       in the enriched schema with the {@code .deferred()} flag — so
   *       {@code Index.isDeferred()} is a durable declarative property.</li>
   *   <li>Non-{@code COMPLETED} rows whose index is not physically present
   *       are virtualized into the schema as declared deferred indexes.</li>
   *   <li>Drift — a {@code COMPLETED} row with no matching physical index,
   *       or a non-{@code COMPLETED} row with a matching physical index —
   *       throws {@link IllegalStateException}. Morf does not auto-heal
   *       indexes; the operator must reconcile manually.</li>
   * </ul>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @param session the per-upgrade session to prime with persisted rows.
   * @return the enriched schema.
   * @throws IllegalStateException if the tracking table disagrees with the
   *     physical schema (drift detected).
   */
  Schema enrich(Schema physicalSchema, DeferredIndexSession session);


  /**
   * Convenience factory for the static upgrade path — wires up the
   * {@link DeployedIndexesDAO} from connection resources without exposing
   * it to callers.
   *
   * @param connectionResources database connection resources.
   * @param config upgrade configuration.
   * @return a new enricher.
   */
  static DeployedIndexesModelEnricher create(ConnectionResources connectionResources,
                                              UpgradeConfigAndContext config) {
    DeployedIndexesDAO dao = new DeployedIndexesDAO(
        new SqlScriptExecutorProvider(connectionResources), connectionResources, new DeployedIndexesStatements());
    return new DeployedIndexesModelEnricherImpl(dao, config);
  }
}
