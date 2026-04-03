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

import com.google.inject.ImplementedBy;

/**
 * Startup hook that reconciles deferred index state before the upgrade
 * framework begins schema diffing.
 *
 * <p>In the comments-based model, deferred indexes are declared in table
 * comments and the MetaDataProvider already includes them as virtual
 * indexes in the schema. This check is invoked during application startup
 * by the upgrade framework
 * ({@link org.alfasoftware.morf.upgrade.Upgrade#findPath findPath}):</p>
 *
 * <ul>
 *   <li>{@link #augmentSchemaWithPendingIndexes(Schema)} is called after
 *       the source schema is read. In the comments-based model the
 *       MetaDataProvider already includes virtual deferred indexes, so
 *       this method is a no-op pass-through.</li>
 * </ul>
 *
 * <p>On a normal restart with no upgrade, pending deferred indexes are
 * left for {@link DeferredIndexService#execute()} to build.</p>
 *
 * @see DeferredIndexService
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexReadinessCheckImpl.class)
public interface DeferredIndexReadinessCheck {

  /**
   * Augments the given source schema with virtual indexes from deferred
   * index operations that have not yet been built.
   *
   * <p>In the comments-based model, the MetaDataProvider already includes
   * virtual deferred indexes from table comments, so this method returns
   * the source schema unchanged.</p>
   *
   * @param sourceSchema the current database schema before upgrade.
   * @return the augmented schema with deferred indexes included.
   */
  Schema augmentSchemaWithPendingIndexes(Schema sourceSchema);


  /**
   * Creates a readiness check instance for use in the static upgrade path
   * where Guice is not available.
   *
   * @param config upgrade configuration.
   * @return a new readiness check instance.
   */
  static DeferredIndexReadinessCheck create(
      org.alfasoftware.morf.upgrade.UpgradeConfigAndContext config) {
    return new DeferredIndexReadinessCheckImpl(config);
  }
}
