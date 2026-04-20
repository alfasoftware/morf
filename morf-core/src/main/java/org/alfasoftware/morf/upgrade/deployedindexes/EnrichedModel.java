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

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;

/**
 * Output of {@link DeployedIndexesModelEnricher#enrich(Schema)}: the
 * enriched schema paired with the companion {@link DeployedIndexState}.
 *
 * <p>The schema carries the correct declarative {@link Index#isDeferred()}
 * on each index (propagated from the tracking row), and deferred-but-not-
 * yet-built indexes appear as virtual entries. The state records
 * operational facts (physical presence) for the visitor and the deferred-
 * SQL scan to consult.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public final class EnrichedModel {

  private final Schema schema;
  private final DeployedIndexState state;


  /**
   * @param schema the enriched schema.
   * @param state the companion operational state.
   */
  public EnrichedModel(Schema schema, DeployedIndexState state) {
    this.schema = schema;
    this.state = state;
  }


  /**
   * @return the enriched schema.
   */
  public Schema getSchema() {
    return schema;
  }


  /**
   * @return operational state: physical presence per index.
   */
  public DeployedIndexState getState() {
    return state;
  }
}
