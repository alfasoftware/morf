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

package org.alfasoftware.morf.upgrade.deferred.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

/**
 * Adds a deferred multi-column index on Product(id, name).
 */
@Sequence(90006)
@UUID("d1f00001-0001-0001-0001-000000000006")
public class AddDeferredMultiColumnIndex extends AbstractDeferredIndexTestStep {

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addIndex("Product", index("Product_IdName_1").columns("id", "name").deferred());
  }
}
