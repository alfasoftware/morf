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

import org.alfasoftware.morf.metadata.Schema;

/**
 * Upgrade-time read-only context passed to
 * {@link UpgradeStep#execute(SchemaEditor, DataEditor, UpgradeContext)}.
 *
 * <p>Provides information a step can inspect (but not mutate) while it runs
 * — things that don't belong on {@link SchemaEditor} (which is a pure
 * command interface) and aren't part of {@link DataEditor} (which is for
 * DML). Regular upgrade steps don't need this; infrastructure steps like
 * {@code CreateDeployedIndexes} do.</p>
 *
 * <p>This interface is expected to grow: future context needs (dialect,
 * {@link UpgradeConfigAndContext}, step metadata, etc.) should be added
 * here rather than retrofitted onto {@link SchemaEditor}.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public interface UpgradeContext {

  /**
   * Returns the database schema as it was at the start of the upgrade.
   *
   * <p><b>Invariant:</b> the returned schema reflects the source-of-upgrade-start
   * state. It is unaffected by any in-session {@link SchemaEditor#addTable},
   * {@link SchemaEditor#addColumn}, etc. calls the step may have already
   * made on the editor.</p>
   *
   * @return the source schema.
   */
  Schema getSourceSchema();
}
