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

package org.alfasoftware.morf.upgrade.deferred.upgrade.v2_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Changes the deferred index created by AddDeferredIndex to a
 * non-deferred multi-column index. Tests the "change the plan" path:
 * if the original deferred index hasn't been built, the comment is
 * updated and no wasted physical build occurs.
 */
@Sequence(90013)
@UUID("d1f00002-0002-0002-0002-000000000013")
public class ChangeDeferredIndex implements UpgradeStep {

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.changeIndex("Product",
        index("Product_Name_1").columns("name"),
        index("Product_Name_1").columns("id", "name"));
  }

  @Override public String getJiraId() { return "DEFERRED-000"; }
  @Override public String getDescription() { return ""; }
}
