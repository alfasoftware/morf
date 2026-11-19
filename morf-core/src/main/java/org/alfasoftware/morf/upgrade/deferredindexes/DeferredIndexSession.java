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

import java.util.List;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.UpdateStatement;

/**
 * Per-upgrade-session journal for deferred-index registration. Each mutation
 * method records the change in an in-memory cache and returns the DSL
 * DML statements the visitor should emit alongside its physical DDL to
 * keep the {@code DeferredIndexes} registration table in sync.
 *
 * <p><b>Registration invariant</b>: only deferred indexes are registered — callers
 * (the visitor) gate {@link #registerIndex(String, Index)} on the index's
 * effective {@code isDeferred()} after dialect-support normalization.</p>
 *
 * <p><b>Lifecycle</b>: instances are per-upgrade. At session start the
 * enricher calls {@link #prime(DeferredIndex, boolean)} for every persisted row
 * so that subsequent {@code unregisterIndex / updateIndexName / updateColumnName}
 * etc. produce correct DML against rows persisted by earlier upgrades.</p>
 *
 * <p>Separate from {@link DeferredIndexService} because the two have
 * fundamentally different shapes: this interface returns DSL statements
 * for batched emission during an upgrade; the service drives JDBC reads
 * and writes at application runtime.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface DeferredIndexSession {

  /**
   * Seeds the in-session cache with a persisted registration row WITHOUT
   * emitting any DML. Called by the enricher at session start.
   *
   * <p>The enricher supplies {@code physicallyPresent} from its own read of the
   * physical schema. It must not be inferred from {@link DeferredIndex#getStatus()}:
   * a build that creates the index and then dies before writing {@code COMPLETED}
   * leaves a non-terminal row over an index that genuinely exists, and a
   * {@code CREATE INDEX CONCURRENTLY} that fails on PostgreSQL leaves one behind
   * too. Status records how far the build got; only the schema says what is
   * actually there.</p>
   *
   * @param entry the persisted row.
   * @param physicallyPresent whether an index of this name exists on the table in
   *     the physical schema, valid or otherwise.
   */
  void prime(DeferredIndex entry, boolean physicallyPresent);


  /**
   * Returns an independent session holding the same state as this one.
   *
   * <p>An upgrade is walked more than once — the {@code InlineTableUpgrader} and the
   * graph-based visitor each produce a script from the same steps, and only one of
   * those scripts is executed. Sessions are mutable: visiting {@code removeIndex}
   * evicts the index, visiting {@code addIndex} registers one. A walk that observed
   * an earlier walk's mutations would draw different conclusions about which indexes
   * are physically present, and emit different DDL. Each walk therefore takes its own
   * copy of the primed session.</p>
   *
   * @return a copy that can be mutated without affecting this session.
   */
  DeferredIndexSession copy();


  /**
   * Records a deferred index and returns the INSERT DML. Callers only
   * invoke this for effective-deferred indexes.
   *
   * @param tableName the table.
   * @param index the index (must be {@code isDeferred()=true}).
   * @return INSERT statements for the visitor to emit.
   */
  List<InsertStatement> registerIndex(String tableName, Index index);


  /**
   * Records a deferred index whose physical form <em>already exists</em> and
   * returns the INSERT DML. Used when the visitor satisfies a declared-deferred
   * index by renaming a shape-matching ignored {@code _PRF} index instead of
   * creating a new one: the index is physically present the moment the upgrade
   * script runs, so it must never enter the build queue.
   *
   * <p>The row is written as {@code COMPLETED}, and the index counts as present
   * for {@link #willBePhysicallyPresent} from this point in the script onwards.</p>
   *
   * @param tableName the table.
   * @param index the index (must be {@code isDeferred()=true}).
   * @return INSERT statements for the visitor to emit.
   */
  List<InsertStatement> registerCompletedIndex(String tableName, Index index);


  /**
   * @param tableName the table.
   * @param indexName the index.
   * @return {@code true} if the index is currently registered as deferred
   *     (any status — built or unbuilt).
   */
  boolean isRegistered(String tableName, String indexName);


  /**
   * Projects forward: will an index of this name exist in the database by the time
   * the generated upgrade script reaches the current emission point? The visitor
   * uses this to decide whether to emit physical DDL — a DROP or RENAME against an
   * index that isn't there would fail the script.
   *
   * <p>Answers for the three cases:</p>
   * <ul>
   *   <li><b>Not registered</b> — {@code true}. Either an ordinary non-deferred
   *   index, or not an index at all; both are the caller's business, not this
   *   session's, and the visitor's existing DDL is correct.</li>
   *   <li><b>Registered by this upgrade</b> — {@code true} only when the emitted DDL
   *   has already materialised it (the PRF-rename case, via
   *   {@link #registerCompletedIndex}). A freshly declared deferred index is absent
   *   until the adopter builds it.</li>
   *   <li><b>Primed from a persisted row</b> — whatever the enricher observed in the
   *   physical schema. Note this is deliberately independent of the row's status;
   *   see {@link #prime(DeferredIndex, boolean)}.</li>
   * </ul>
   *
   * <p>Callers must read this <em>before</em> the mutation methods below, which
   * evict or rewrite the record it consults.</p>
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return {@code true} if the index will exist at this point in the script.
   */
  boolean willBePhysicallyPresent(String tableName, String indexName);


  /**
   * Removes an index from registration and returns the DELETE.
   *
   * @param tableName the table.
   * @param indexName the index.
   * @return DELETE statements, empty if not registered.
   */
  List<DeleteStatement> unregisterIndex(String tableName, String indexName);


  /**
   * Removes every registered index for a table.
   *
   * @param tableName the table.
   * @return DELETE statements, empty if no registered indexes for the table.
   */
  List<DeleteStatement> unregisterAllFor(String tableName);


  /**
   * Removes every registered index that references the named column.
   *
   * @param tableName the table.
   * @param columnName the column being removed.
   * @return DELETE statements for each affected index.
   */
  List<DeleteStatement> unregisterByColumn(String tableName, String columnName);


  /**
   * Re-homes every registered index from one table name to another.
   *
   * @param oldTableName the old table name.
   * @param newTableName the new table name.
   * @return UPDATE statements.
   */
  List<UpdateStatement> updateTableName(String oldTableName, String newTableName);


  /**
   * Updates column references on every registered index that mentions the
   * renamed column.
   *
   * @param tableName the table.
   * @param oldColumnName the old column name.
   * @param newColumnName the new column name.
   * @return UPDATE statements, one per affected index.
   */
  List<UpdateStatement> updateColumnName(String tableName, String oldColumnName, String newColumnName);


  /**
   * Renames a registered index.
   *
   * @param tableName the table.
   * @param oldIndexName the old index name.
   * @param newIndexName the new index name.
   * @return UPDATE statements, empty if not registered.
   */
  List<UpdateStatement> updateIndexName(String tableName, String oldIndexName, String newIndexName);


  /**
   * Convenience factory for the static upgrade path. Wires up the package-private
   * {@link DeferredIndexesStatements} helper without exposing it to callers.
   *
   * @return a new per-upgrade session.
   */
  static DeferredIndexSession create() {
    return new DeferredIndexSessionImpl(new DeferredIndexesStatements());
  }
}
