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

package org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v2_0_0;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Renames table "Product" to "Item". Used to test cross-step
 * table rename affecting a deferred index from a previous step.
 */
@Sequence(90017)
@UUID("d1f00002-0002-0002-0002-000000000017")
public class RenameTableWithDeferredIndex implements UpgradeStep {

  @Override
  public String getJiraId() { return "TEST-17"; }

  @Override
  public String getDescription() { return "Rename table Product to Item"; }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.renameTable("Product", "Item");
  }
}
