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

package org.alfasoftware.morf.upgrade.deferred;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default implementation of {@link DeferredIndexReadinessCheck}.
 *
 * <p>In the comments-based model, deferred indexes are declared in table
 * comments and the MetaDataProvider includes them as virtual indexes.
 * The {@link #augmentSchemaWithPendingIndexes(Schema)} method is therefore
 * a no-op pass-through: the schema already contains the deferred indexes.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@Singleton
class DeferredIndexReadinessCheckImpl implements DeferredIndexReadinessCheck {

  private static final Log log = LogFactory.getLog(DeferredIndexReadinessCheckImpl.class);

  private final UpgradeConfigAndContext config;


  /**
   * Constructs a readiness check with injected dependencies.
   *
   * @param config upgrade configuration.
   */
  @Inject
  DeferredIndexReadinessCheckImpl(UpgradeConfigAndContext config) {
    this.config = config;
  }


  /**
   * Returns the source schema unchanged. In the comments-based model the
   * MetaDataProvider already includes virtual deferred indexes from table
   * comments, so no augmentation is needed.
   *
   * @see DeferredIndexReadinessCheck#augmentSchemaWithPendingIndexes(Schema)
   */
  @Override
  public Schema augmentSchemaWithPendingIndexes(Schema sourceSchema) {
    if (!config.isDeferredIndexCreationEnabled()) {
      return sourceSchema;
    }

    log.debug("Comments-based model: schema already includes deferred indexes from table comments");
    return sourceSchema;
  }
}
