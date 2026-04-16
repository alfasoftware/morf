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
 * <ul>
 *   <li>{@link #PRESENT} — the enricher saw a matching physical index.</li>
 *   <li>{@link #ABSENT} — the enricher saw a tracking row with no matching
 *       physical index (e.g. a virtual deferred index not yet built).</li>
 *   <li>{@link #UNKNOWN} — the enricher didn't see this index at all. Callers
 *       decide whether to treat unknown as "present" (e.g. an in-session
 *       addition whose CREATE INDEX is already queued) or "not built yet"
 *       (e.g. a new deferred index needing its CREATE INDEX emitted).</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public enum IndexPresence {

  /** The enricher saw a matching physical index. */
  PRESENT,

  /** The enricher saw a tracking row with no matching physical index. */
  ABSENT,

  /** The enricher didn't see this index at all. */
  UNKNOWN
}
