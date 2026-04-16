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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

/**
 * Defers an index that includes "description", renames "description" to
 * "summary", then removes the deferred index and the renamed column.
 * The removeIndex must auto-cancel the deferred operation via
 * {@code hasPendingDeferred}, even though an intermediate column rename
 * occurred. The in-memory tracking must reflect the rename so that
 * a hypothetical {@code cancelPendingReferencingColumn} call would also
 * succeed — that path is verified by unit tests.
 */
@Sequence(90011)
@UUID("d1f00001-0001-0001-0001-000000000011")
public class AddDeferredIndexThenRenameColumnThenRemove extends AbstractDeferredIndexTestStep {

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addIndex("Product", index("Product_Desc_1").columns("description").deferred());
    schema.changeColumn("Product",
        column("description", DataType.STRING, 200),
        column("summary", DataType.STRING, 200));
    schema.removeIndex("Product", index("Product_Desc_1").columns("description"));
    schema.removeColumn("Product", column("summary", DataType.STRING, 200));
  }
}
