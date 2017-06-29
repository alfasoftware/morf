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

package org.alfasoftware.morf.upgrade.adapt;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Tests the filtered schema can remove tables in a case-insensitive manner.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestFilteredSchema {

  /**
   * Tests that the table list is filtered using a case-insensitive comparator.
   */
  @Test
  public void testCaseInsensitiveRemoval() {
    Table lowerCaseTable = table("lower").columns(column("col", DataType.STRING, 10).nullable());
    Table mixedCaseTable = table("Mixed").columns(column("col", DataType.STRING, 10).nullable());
    Table upperCaseTable = table("UPPER").columns(column("col", DataType.STRING, 10).nullable());
    Schema testSchema = schema(lowerCaseTable, mixedCaseTable, upperCaseTable);

    TableSetSchema schema = new FilteredSchema(testSchema, "LOWER", "MIXED", "upper");
    assertFalse("Lowercase table exists when removed by uppercase name", schema.tableExists("lower"));
    assertFalse("Mixed case table exists when removed by uppercase name", schema.tableExists("Mixed"));
    assertFalse("Uppercase table exists when removed by lowercase name", schema.tableExists("UPPER"));
  }

}
