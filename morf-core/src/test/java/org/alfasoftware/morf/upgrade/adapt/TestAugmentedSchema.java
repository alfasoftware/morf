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

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link AugmentedSchema}
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class TestAugmentedSchema {

  /**
   * Tests that the table list is updated with new tables.
   */
  @Test
  public void testAdditionOfNewTables() {
    Table lowerCaseTable = table("lower").columns(column("col", DataType.STRING, 10).nullable());
    Table mixedCaseTable = table("Mixed").columns(column("col", DataType.STRING, 10).nullable());
    Table upperCaseTable = table("UPPER").columns(column("col", DataType.STRING, 10).nullable());

    Sequence lowerCaseSequence= sequence("lower", 1,false);
    Sequence mixedCaseSequence = sequence("Mixed", 5, false);
    Sequence upperCaseSequence = sequence("UPPER", 10, false);

    Schema testSchema = schema(schema(lowerCaseTable, mixedCaseTable, upperCaseTable),
      schema(),
      schema(lowerCaseSequence,mixedCaseSequence,upperCaseSequence));

    Table newTable = table("newTable").columns(column("col", DataType.STRING, 10).nullable());

    TableSetSchema schema = new AugmentedSchema(testSchema, newTable);
    assertTrue("New table doesn't exist in the updated schema", schema.tableExists(newTable.getName()));
    assertTrue("Lowercase doesn't exist in the updated schema", schema.tableExists("lower"));
    assertTrue("Mixed case doesn't exist in the updated schema", schema.tableExists("Mixed"));
    assertTrue("Uppercase doesn't exist in the updated schema", schema.tableExists("UPPER"));

    assertTrue("Lowercase sequence doesn't exist in the updated schema", schema.sequenceExists("lower"));
    assertTrue("Mixed case sequence doesn't exist in the updated schema", schema.sequenceExists("Mixed"));
    assertTrue("Uppercase sequence doesn't exist in the updated schema", schema.sequenceExists("UPPER"));
  }


  /**
   * Tests that the table list is updated with new tables.
   */
  @Test
  public void testAdditionOfNewSequences() {
    Table lowerCaseTable = table("lower").columns(column("col", DataType.STRING, 10).nullable());
    Table mixedCaseTable = table("Mixed").columns(column("col", DataType.STRING, 10).nullable());
    Table upperCaseTable = table("UPPER").columns(column("col", DataType.STRING, 10).nullable());

    Sequence lowerCaseSequence= sequence("lower", 1,false);
    Sequence mixedCaseSequence = sequence("Mixed", 5, false);
    Sequence upperCaseSequence = sequence("UPPER", 10, false);

    Schema testSchema = schema(schema(lowerCaseTable, mixedCaseTable, upperCaseTable),
      schema(),
      schema(lowerCaseSequence,mixedCaseSequence,upperCaseSequence));

    Sequence newSequence = sequence("newSequence", 100, true);

    TableSetSchema schema = new AugmentedSchema(testSchema, newSequence);
    assertTrue("Lowercase doesn't exist in the updated schema", schema.tableExists("lower"));
    assertTrue("Mixed case doesn't exist in the updated schema", schema.tableExists("Mixed"));
    assertTrue("Uppercase doesn't exist in the updated schema", schema.tableExists("UPPER"));

    assertTrue("Lowercase sequence doesn't exist in the updated schema", schema.sequenceExists("lower"));
    assertTrue("Mixed case sequence doesn't exist in the updated schema", schema.sequenceExists("Mixed"));
    assertTrue("Uppercase sequence doesn't exist in the updated schema", schema.sequenceExists("UPPER"));
    assertTrue("New sequence doesn't exist in the updated schema", schema.sequenceExists("newSequence"));
  }

}
