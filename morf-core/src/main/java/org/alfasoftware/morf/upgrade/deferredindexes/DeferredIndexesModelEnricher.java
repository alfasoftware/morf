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

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;

import com.google.inject.ImplementedBy;

/**
 * Merges the physical database schema with the {@code DeferredIndexes}
 * registration table. Returns an enriched {@link Schema} where built-deferred
 * indexes carry the {@code .deferred()} flag and unbuilt-deferred rows
 * are virtualized as declared indexes.
 *
 * <p><b>Background-build invariant</b>: only deferred indexes are registered.
 * A row exists in the table iff the index is currently declared
 * {@code .deferred()}. The enricher's job is to (a) <b>prime</b> the
 * per-upgrade {@link DeferredIndexSession} with every persisted row so that
 * in-session mutations (remove/rename/column) cascade correctly to all
 * currently-declared deferred indexes; (b) <b>rebuild</b> COMPLETED-row
 * physical indexes with the {@code .deferred()} flag so
 * {@code Index.isDeferred()} is durable across the build lifecycle; and
 * (c) <b>virtualize</b> non-COMPLETED rows (PENDING/IN_PROGRESS/FAILED)
 * into the schema so {@code SchemaHomology.schemasMatch} treats them as
 * declared.</p>
 *
 * <p><b>Narrow drift policy</b>: only operator-caused corruption of
 * {@code COMPLETED} rows throws — a missing physical index (manual DROP)
 * or an {@code INVALID} physical (corruption). The routine-restart case
 * (non-COMPLETED row + physical present) does NOT throw — the build task
 * reconciles via {@code dialect.isIndexValid} on its next pass. All drifts
 * across the schema are collected and reported in a single
 * {@link IllegalStateException} at the end of the pass.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexesModelEnricherImpl.class)
public interface DeferredIndexesModelEnricher {

  /**
   * Enriches the physical schema with {@code DeferredIndexes} metadata and
   * primes the per-upgrade session with persisted registration rows.
   *
   * <p>If the feature is disabled, the {@code DeferredIndexes} table does
   * not yet exist, or the table is empty, the physical schema is returned
   * unchanged — and the session is not primed.</p>
   *
   * <p>Otherwise:</p>
   * <ul>
   *   <li>Every persisted row primes the session (so the visitor's
   *       remove/rename/column operations emit correct DML against
   *       prior-upgrade registration rows).</li>
   *   <li>{@code COMPLETED} rows whose physical index is present and VALID
   *       (or unknown — dialects without {@code isIndexValid} support)
   *       are rebuilt in the enriched schema with the {@code .deferred()}
   *       flag — so {@code Index.isDeferred()} is a durable declarative
   *       property.</li>
   *   <li>Non-{@code COMPLETED} rows are <b>always</b> represented as
   *       deferred indexes in the enriched schema, whether their physical
   *       counterpart is present or not — the build task reconciles the
   *       physical state via {@code isIndexValid} on its next pass.</li>
   *   <li>Drift — only operator-caused corruption of {@code COMPLETED}
   *       rows: missing physical (manual DROP) or {@code INVALID} physical
   *       — throws {@link IllegalStateException}. Operator must reconcile
   *       manually.</li>
   * </ul>
   *
   * @param physicalSchema the schema read from JDBC metadata.
   * @param session the per-upgrade session to prime with persisted rows.
   * @return the enriched schema.
   * @throws IllegalStateException if a {@code COMPLETED} row's physical
   *     index is missing or {@code INVALID} (operator-caused drift the
   *     executor cannot auto-recover from).
   */
  Schema enrich(Schema physicalSchema, DeferredIndexSession session);


  /**
   * Convenience factory for the static upgrade path — wires up the
   * {@link DeferredIndexesDAO} from connection resources without exposing
   * it to callers.
   *
   * @param connectionResources database connection resources.
   * @param config upgrade configuration.
   * @return a new enricher.
   */
  static DeferredIndexesModelEnricher create(ConnectionResources connectionResources,
                                              UpgradeConfigAndContext config) {
    DeferredIndexesDAO dao = new DeferredIndexesDAO(
        new SqlScriptExecutorProvider(connectionResources), connectionResources, new DeferredIndexesStatements());
    return new DeferredIndexesModelEnricherImpl(dao, connectionResources, config);
  }
}
