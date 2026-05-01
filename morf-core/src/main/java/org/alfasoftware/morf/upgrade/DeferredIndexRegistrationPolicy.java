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

package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;

/**
 * Policy class encapsulating the dialect-aware "should we track this index
 * in DeferredIndexes?" decision and the matching "should we emit physical
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
final class DeferredIndexRegistrationPolicy {

  private final SqlDialect sqlDialect;


  /**
   * @param sqlDialect dialect used to ask {@link SqlDialect#supportsDeferredIndexCreation()}.
   */
  DeferredIndexRegistrationPolicy(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
  }


  /**
   * Decides whether the index should produce a registration row.
   *
   * <p>An index is registered iff it is declared {@code .deferred()} AND the
   * dialect supports deferred creation. On dialects that don't support
   * deferred creation, declared-deferred indexes are normalized to
   * immediate (built at upgrade time, no registration row).</p>
   *
   * <p>Idempotent under {@link #normalize} — calling on either the raw
   * or the normalized form produces the same answer.</p>
   *
   * @param declared the index (raw or normalized).
   * @return true if a registration row should be created for this index.
   */
  boolean shouldRegister(Index declared) {
    return declared.isDeferred() && sqlDialect.supportsDeferredIndexCreation();
  }


  /**
   * Decides whether the visitor must emit a physical CREATE INDEX statement
   * (or a rename equivalent) at upgrade time.
   *
   * <p>Returns true iff the index is non-deferred OR the dialect doesn't
   * support deferred creation. In both cases, the index has to be built
   * immediately during the upgrade rather than queued for the adopter.</p>
   *
   * <p>Idempotent under {@link #normalize} — calling on either the raw
   * or the normalized form produces the same answer.</p>
   *
   * @param declared the index (raw or normalized).
   * @return true if physical DDL is required at upgrade time.
   */
  boolean requiresImmediateBuild(Index declared) {
    return !declared.isDeferred() || !sqlDialect.supportsDeferredIndexCreation();
  }


  /**
   * Normalizes an index for DDL emission. If the index is declared
   * {@code .deferred()} but the current dialect does not support deferred
   * index creation (e.g. MySQL, SQL Server), strips the {@code .deferred()}
   * flag so downstream dialect handlers treat it as a regular immediate
   * index. All other indexes pass through unchanged.
   *
   * <p>Preserves name, columns, and uniqueness.</p>
   *
   * <p>Idempotent: {@code normalize(normalize(x))} equals {@code normalize(x)}.</p>
   *
   * @param declared the index as declared by the upgrade step.
   * @return the index in the form the visitor should emit DDL for.
   */
  Index normalize(Index declared) {
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
