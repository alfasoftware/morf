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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertSame;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradeConfigAndContext;
import org.junit.Test;

/**
 * Unit tests for {@link DeferredIndexReadinessCheckImpl} in the
 * comments-based model. The readiness check is a no-op pass-through
 * because the MetaDataProvider already includes virtual deferred indexes
 * from table comments.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexReadinessCheckUnit {

  /** augmentSchemaWithPendingIndexes should return schema unchanged when disabled. */
  @Test
  public void testAugmentReturnsUnchangedWhenDisabled() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(false);

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(config);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    assertSame("Should return input schema unchanged", input, check.augmentSchemaWithPendingIndexes(input));
  }


  /** augmentSchemaWithPendingIndexes should return schema unchanged when enabled (no-op in comments model). */
  @Test
  public void testAugmentReturnsUnchangedWhenEnabled() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);

    DeferredIndexReadinessCheckImpl check = new DeferredIndexReadinessCheckImpl(config);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    assertSame("Should return input schema unchanged (comments-based model)", input, check.augmentSchemaWithPendingIndexes(input));
  }


  /** Static factory create(config) should return a working instance. */
  @Test
  public void testStaticFactoryCreate() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);

    DeferredIndexReadinessCheck check = DeferredIndexReadinessCheck.create(config);
    Schema input = schema(table("Foo").columns(column("id", DataType.BIG_INTEGER).primaryKey()));

    assertSame("Static factory should produce a working no-op check",
        input, check.augmentSchemaWithPendingIndexes(input));
  }


}
