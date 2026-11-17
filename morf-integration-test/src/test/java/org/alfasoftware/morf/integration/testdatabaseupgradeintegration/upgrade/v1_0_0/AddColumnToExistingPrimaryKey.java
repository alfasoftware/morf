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
@UUID("7e2afe25-5090-4dca-9f8b-321f7634f3b1")
public class AddColumnToExistingPrimaryKey extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // extends the existing primary key through changeColumn rather than changePrimaryKeyColumns
    schema.changeColumn("BasicTable", column("decimalTenZeroCol", DataType.DECIMAL, 10), column("decimalTenZeroCol", DataType.DECIMAL, 10).primaryKey());
  }
}
