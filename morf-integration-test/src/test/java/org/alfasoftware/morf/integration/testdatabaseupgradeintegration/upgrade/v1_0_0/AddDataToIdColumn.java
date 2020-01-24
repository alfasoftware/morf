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

import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("1ade56c0-b1d7-11e2-9e96-080020011111")
public class AddDataToIdColumn extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    data.executeStatement(
        insert()
                .into(tableRef("IdTable"))
                .values(literal("blaa1").as("someValue"))
        );
    data.executeStatement(
        insert()
                .into(tableRef("IdTable"))
                .values(literal("blaa2").as("someValue"))
        );
    data.executeStatement(
        insert()
                .into(tableRef("IdTable"))
                .values(literal("blaa3").as("someValue"))
        );
  }
}