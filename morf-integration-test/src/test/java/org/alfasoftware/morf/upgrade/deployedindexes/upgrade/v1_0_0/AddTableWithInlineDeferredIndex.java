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

package org.alfasoftware.morf.upgrade.deployedindexes.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

/**
 * Creates a new table with a deferred index declared inline on the table
 * (rather than via a separate addIndex call). Exercises the actually-defer
 * path: the visitor should filter the deferred index out of the CREATE TABLE
 * statement and queue it for the adopter via the deferred pipeline.
 */
@Sequence(90008)
@UUID("d1f00001-0001-0001-0001-000000000008")
public class AddTableWithInlineDeferredIndex extends AbstractDeferredIndexTestStep {

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addTable(table("Category").columns(
        column("id", DataType.BIG_INTEGER).primaryKey(),
        column("label", DataType.STRING, 50)
    ).indexes(index("Category_Label_1").columns("label").deferred()));
  }
}
