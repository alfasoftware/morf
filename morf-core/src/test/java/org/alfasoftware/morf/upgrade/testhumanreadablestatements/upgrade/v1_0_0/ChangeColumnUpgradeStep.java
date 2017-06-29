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
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * Sample upgrade step to add columns to tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@Sequence(2)
public class ChangeColumnUpgradeStep implements UpgradeStep {

  /**
   * @see UpgradeStep#execute(SchemaEditor, DataEditor)
   */
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.changeColumn("table_one", column("column_one", DataType.STRING, 10).nullable(), column("column_one", DataType.STRING, 10));
    schema.changeColumn("table_two", column("column_two", DataType.STRING, 10), column("column_two", DataType.STRING, 10).nullable());
    schema.changeColumn("table_three", column("column_three", DataType.DECIMAL, 9, 5).nullable(), column("column_three", DataType.DECIMAL, 9, 5));
    schema.changeColumn("table_four", column("column_four", DataType.DECIMAL, 9, 5), column("column_four", DataType.DECIMAL, 9, 5).nullable());
    schema.changeColumn("table_five", column("column_five", DataType.DECIMAL, 9), column("column_five", DataType.DECIMAL, 9, 5));
    schema.changeColumn("table_six", column("column_six", DataType.DECIMAL, 9, 5), column("column_six", DataType.DECIMAL, 9));
    schema.changeColumn("table_seven", column("column_seven", DataType.DECIMAL, 9), column("column_seven", DataType.STRING, 9));
    schema.changeColumn("table_eight", column("column_eight", DataType.STRING, 9), column("column_eight", DataType.DECIMAL, 9));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
   */
  @Override
  public String getDescription() {
    return "Change some columns on some tables";
  }

  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
   */
  @Override
  public String getJiraId() {
    return "SAMPLE-2";
  }
}