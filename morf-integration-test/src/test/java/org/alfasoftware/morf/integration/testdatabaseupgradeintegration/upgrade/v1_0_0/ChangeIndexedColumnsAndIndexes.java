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

package org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("1c5ad80d-bd36-48b6-b5d6-5c06eed66802")
public class ChangeIndexedColumnsAndIndexes extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // each change works on the indexes as left by the changes before it
    schema.changeColumn("IndexedColumnTable", column("stringCol", DataType.STRING, 10), column("stringCol", DataType.STRING, 20));
    schema.changeIndex("IndexedColumnTable", index("IndexedColumnTable_1").unique().columns("stringCol"), index("IndexedColumnTable_1").unique().columns("stringCol", "code"));
    // renamed, widened and made nullable at once
    schema.changeColumn("IndexedColumnTable", column("stringCol", DataType.STRING, 20), column("renamedStringCol", DataType.STRING, 25).nullable());
    schema.addIndex("IndexedColumnTable", index("IndexedColumnTable_4").columns("bigIntegerCol", "renamedStringCol"));
    schema.changeColumn("IndexedColumnTable", column("bigIntegerCol", DataType.BIG_INTEGER), column("bigIntegerCol", DataType.BIG_INTEGER).nullable());
    // only the case of the name changes
    schema.changeColumn("IndexedColumnTable", column("decimalNineTwoCol", DataType.DECIMAL, 9, 2).nullable(), column("DecimalNineTwoCol", DataType.DECIMAL, 9, 2).nullable());
  }
}
