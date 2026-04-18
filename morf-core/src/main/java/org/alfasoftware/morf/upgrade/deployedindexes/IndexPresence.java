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

/**
 * Operational presence of an index as observed by the enricher.
 *
 * <p>{@link #UNKNOWN} deserves special attention: hitting it is the
 * <em>normal</em> result for indexes that first appear during the current
 * upgrade session (the enricher runs once against the source schema +
 * tracking table and does not see subsequent in-memory mutations). It is
 * NOT an error signal in that common case — callers decide how to interpret
 * it in their context. UNKNOWN should, however, never appear for an index
 * that was live in the source schema or had a tracking row when the
 * enricher ran; that would indicate the enricher silently skipped work
 * (a bug).</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public enum IndexPresence {

  /** Enricher observed a matching physical index. */
  PRESENT,

  /** Enricher observed a tracking row with no matching physical index
   *  (e.g. a deferred index that hasn't been built yet). */
  ABSENT,

  /**
   * Enricher has no record of this index. The <em>normal</em> result for
   * indexes that first appear during the current upgrade session —
   * e.g. an in-session {@code AddIndex} queues a CREATE INDEX but the
   * enricher ran before that step. Callers decide the meaning:
   * <ul>
   *   <li>{@code AbstractSchemaChangeVisitor.willBePhysicallyPresentAtThisEmission}
   *       treats UNKNOWN as "present" (the CREATE is already queued in-session).</li>
   *   <li>{@code Upgrade.collectDeferredIndexJobs} treats UNKNOWN as "needs
   *       building" when emitting deferred index statements.</li>
   * </ul>
   * <p>UNKNOWN should NOT appear for an index that was live in the source
   * schema or had a tracking row when the enricher ran; that would indicate
   * a logic bug in the enricher.</p>
   */
  UNKNOWN
}
