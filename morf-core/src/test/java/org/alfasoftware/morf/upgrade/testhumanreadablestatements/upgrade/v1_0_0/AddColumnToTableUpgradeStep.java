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

package org.alfasoftware.morf.upgrade.testhumanreadablestatements.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Sample upgrade step to add columns to tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@Sequence(1)
public class AddColumnToTableUpgradeStep implements UpgradeStep {

  /**
   * @see UpgradeStep#execute(SchemaEditor, DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addColumn("table_one", column("column_one", DataType.STRING, 10).nullable(), new FieldLiteral("A"));
    schema.addColumn("table_two", column("column_two", DataType.STRING, 10), new FieldLiteral("A"));
    schema.addColumn("table_three", column("column_three", DataType.DECIMAL, 9, 5).nullable(), new FieldLiteral(10d));
    schema.addColumn("table_four", column("column_four", DataType.DECIMAL, 9, 5), new FieldLiteral(10d));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
   */
  @Override
  public String getDescription() {
    return "Adds new columns to tables";
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
   */
  @Override
  public String getJiraId() {
    return "SAMPLE-1";
  }
}