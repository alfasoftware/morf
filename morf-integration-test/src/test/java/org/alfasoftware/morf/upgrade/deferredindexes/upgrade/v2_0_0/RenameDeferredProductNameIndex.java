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

package org.alfasoftware.morf.upgrade.deferredindexes.upgrade.v2_0_0;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Renames Product_Name_1 to Product_Name_Renamed -- used in cross-upgrade
 * scenarios to exercise RenameIndex against a deferred index whose physical
 * was materialised by a PRF rename.
 */
@Sequence(90032)
@UUID("d1f00002-0002-0002-0002-000000000032")
public class RenameDeferredProductNameIndex implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "DEFERRED-032";
  }


  @Override
  public String getDescription() {
    return "";
  }


  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.renameIndex("Product", "Product_Name_1", "Product_Name_Renamed");
  }
}
