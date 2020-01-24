/* Copyright 2017 Alfa Financial Software
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
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("2354042d-024d-4e56-a6ce-57c8ccc01af9")
public class CreateTableAsSelect extends AbstractTestUpgradeStep {

  public static Table tableToAdd() {
    return table("TableAsSelect").columns(
      column("stringCol", DataType.STRING, 25).primaryKey(),
      column("stringColNullable", DataType.STRING, 30).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 15),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    SelectStatement selectDataFromBasicTable =
        select(
          cast(field("stringCol")).asString(25),
          cast(field("stringCol")).asString(30),
          cast(field("decimalTenZeroCol")).asType(DataType.DECIMAL, 15),
          field("nullableBigIntegerCol")
        ).from(tableRef("BasicTable"));

    schema.addTableFrom(tableToAdd(), selectDataFromBasicTable);
  }
}