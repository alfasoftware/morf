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

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("a98e10e9-9288-4b6f-8707-73c998d82941")
public class ChangePrecisionOfIndexedColumns extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.changeColumn("IndexedColumnTable", column("decimalNineTwoCol", DataType.DECIMAL, 9, 2).nullable(), column("decimalNineTwoCol", DataType.DECIMAL, 12, 4).nullable());
    schema.changeColumn("IndexedColumnTable", column("decimalTenZeroCol", DataType.DECIMAL, 10), column("decimalTenZeroCol", DataType.BIG_INTEGER));
  }
}
