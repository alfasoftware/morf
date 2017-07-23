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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Criterion.eq;

import org.alfasoftware.morf.metadata.Table;

/**
 * Utility to remove rows from the AutoNumber table.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class AutoNumberRemovalHelper {


  /**
   * Removes the row, referring to a {@linkplain Table}, from the AutoNumber table.
   *
   * @param dataEditor Executor of statements
   * @param table The table for which autonumbering will be rmeoved
   */
  public static void removeAutonumber(DataEditor dataEditor, Table table) {
    dataEditor.executeStatement(delete(tableRef("AutoNumber")).where(eq(field("name"), literal(table.getName()))));
  }
}
