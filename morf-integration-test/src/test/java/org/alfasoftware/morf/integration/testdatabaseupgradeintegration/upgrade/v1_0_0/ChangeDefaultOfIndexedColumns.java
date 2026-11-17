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
@UUID("4a3999f5-f6dc-4529-b237-b2bb4ab94d10")
public class ChangeDefaultOfIndexedColumns extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    // in a unique index, in composite indexes and in the primary key
    schema.changeColumn("IndexedColumnTable", column("stringCol", DataType.STRING, 10), column("stringCol", DataType.STRING, 10).defaultValue("X"));
    schema.changeColumn("IndexedColumnTable", column("decimalTenZeroCol", DataType.DECIMAL, 10), column("decimalTenZeroCol", DataType.DECIMAL, 10).defaultValue("5"));
    schema.changeColumn("IndexedColumnTable", column("bigIntegerCol", DataType.BIG_INTEGER), column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("7"));
    schema.changeColumn("IndexedColumnTable", column("code", DataType.STRING, 10).primaryKey(), column("code", DataType.STRING, 10).primaryKey().defaultValue("Z"));
    schema.changeColumn("IndexedColumnTable", column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("7"), column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("8"));
    // made not nullable with a default at once, which the data accommodates
    schema.changeColumn("IndexedColumnTable", column("nullableStringCol", DataType.STRING, 10).nullable(), column("nullableStringCol", DataType.STRING, 10).defaultValue("Y"));

    // default values are not part of the schema read from the database, so they must not be left behind
    schema.changeColumn("IndexedColumnTable", column("stringCol", DataType.STRING, 10).defaultValue("X"), column("stringCol", DataType.STRING, 10));
    schema.changeColumn("IndexedColumnTable", column("decimalTenZeroCol", DataType.DECIMAL, 10).defaultValue("5"), column("decimalTenZeroCol", DataType.DECIMAL, 10));
    schema.changeColumn("IndexedColumnTable", column("bigIntegerCol", DataType.BIG_INTEGER).defaultValue("8"), column("bigIntegerCol", DataType.BIG_INTEGER));
    schema.changeColumn("IndexedColumnTable", column("code", DataType.STRING, 10).primaryKey().defaultValue("Z"), column("code", DataType.STRING, 10).primaryKey());
    schema.changeColumn("IndexedColumnTable", column("nullableStringCol", DataType.STRING, 10).defaultValue("Y"), column("nullableStringCol", DataType.STRING, 10).nullable());
  }
}
