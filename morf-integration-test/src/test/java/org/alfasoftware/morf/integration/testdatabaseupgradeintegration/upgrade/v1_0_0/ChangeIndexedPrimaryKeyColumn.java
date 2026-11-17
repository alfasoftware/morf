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

import com.google.common.collect.ImmutableList;

@Sequence(1)
@UUID("47deed33-dfc5-4847-8523-522a972b5758")
public class ChangeIndexedPrimaryKeyColumn extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // take the only primary key column out of the key, then put it back
    schema.changeColumn("IndexedColumnTable", column("code", DataType.STRING, 10).primaryKey(), column("code", DataType.STRING, 10));
    schema.changeColumn("IndexedColumnTable", column("code", DataType.STRING, 10), column("code", DataType.STRING, 10).primaryKey());

    schema.changeColumn("IndexedColumnTable", column("code", DataType.STRING, 10).primaryKey(), column("renamedCode", DataType.STRING, 20).primaryKey());
    // reduced below the original width, which the data accommodates
    schema.changeColumn("IndexedColumnTable", column("renamedCode", DataType.STRING, 20).primaryKey(), column("renamedCode", DataType.STRING, 5).primaryKey());

    // change the type of an indexed column while it is in a composite primary key, then take it out of the key
    schema.changePrimaryKeyColumns("IndexedColumnTable", ImmutableList.of("renamedCode"), ImmutableList.of("renamedCode", "decimalTenZeroCol"));
    schema.changeColumn("IndexedColumnTable", column("decimalTenZeroCol", DataType.DECIMAL, 10).primaryKey(), column("decimalTenZeroCol", DataType.BIG_INTEGER).primaryKey());
    schema.changeColumn("IndexedColumnTable", column("decimalTenZeroCol", DataType.BIG_INTEGER).primaryKey(), column("decimalTenZeroCol", DataType.BIG_INTEGER));
  }
}
