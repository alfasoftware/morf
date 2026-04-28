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

import java.util.Optional;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;

/**
 * Policy class encapsulating the dialect-aware "should we track this index
 * in DeployedIndexes?" decision and the matching "should we emit physical
 * CREATE INDEX immediately?" decision.
 *
 * <p>Replaces three formerly-scattered concerns in
 * {@code AbstractSchemaChangeVisitor}: the {@code effectiveIndex}
 * normalization helper, plus copy-pasted {@code if (effective.isDeferred())}
 * gates in {@code visit(AddIndex)}, {@code visit(AddTable)}, and
 * {@code visit(ChangeIndex)}, plus the {@code shouldEmitPhysicalIndexDdl}
 * helper.</p>
 *
 * <p>Stateless, dialect-bound. Visitor instances construct one in their
 * constructor from the same {@code SqlDialect} they already hold.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public final class DeferredIndexTrackingPolicy {

  private final SqlDialect sqlDialect;


  /**
   * @param sqlDialect dialect used to ask {@link SqlDialect#supportsDeferredIndexCreation()}.
   */
  public DeferredIndexTrackingPolicy(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
  }


  /**
   * Decides whether the index should produce a tracking row, returning
   * the form the row should describe.
   *
   * <p>An index is tracked iff it is declared {@code .deferred()} AND the
   * dialect supports deferred creation. On dialects that don't support
   * deferred creation, declared-deferred indexes are normalized to
   * immediate (built at upgrade time, no tracking row).</p>
   *
   * @param declared the index as declared in the schema change.
   * @return the form to track if it should be tracked, otherwise empty.
   */
  public Optional<Index> toTrackedIndex(Index declared) {
    if (!declared.isDeferred()) {
      return Optional.empty();
    }
    if (!sqlDialect.supportsDeferredIndexCreation()) {
      return Optional.empty();
    }
    return Optional.of(declared);
  }


  /**
   * Decides whether the visitor must emit a physical CREATE INDEX statement
   * (or a rename equivalent) at upgrade time.
   *
   * <p>Returns true iff the index is non-deferred OR the dialect doesn't
   * support deferred creation. In both cases, the index has to be built
   * immediately during the upgrade rather than queued for the adopter.</p>
   *
   * @param declared the index as declared in the schema change.
   * @return true if physical DDL is required at upgrade time.
   */
  public boolean requiresImmediateBuild(Index declared) {
    return !declared.isDeferred() || !sqlDialect.supportsDeferredIndexCreation();
  }


  /**
   * Returns the index in the form the visitor should physically emit DDL
   * for. On unsupported-dialect normalization, drops the {@code .deferred()}
   * flag so dialect handlers don't go down a deferred-DDL path that doesn't
   * exist.
   *
   * @param declared the index as declared.
   * @return the dialect-normalized form.
   */
  public Index effectiveIndex(Index declared) {
    if (!declared.isDeferred() || sqlDialect.supportsDeferredIndexCreation()) {
      return declared;
    }
    SchemaUtils.IndexBuilder builder = SchemaUtils.index(declared.getName())
        .columns(declared.columnNames());
    if (declared.isUnique()) {
      builder = builder.unique();
    }
    return builder;
  }
}
