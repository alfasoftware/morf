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

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.metadata.Sequence;
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

    Sequence lowerCaseSequence= sequence("lower", 1,false);
    Sequence mixedCaseSequence = sequence("Mixed", 5, false);
    Sequence upperCaseSequence = sequence("UPPER", 10, false);

    Schema testSchema = schema(schema(lowerCaseTable, mixedCaseTable, upperCaseTable),
      schema(),
      schema(lowerCaseSequence,mixedCaseSequence,upperCaseSequence));

    TableSetSchema schema = new FilteredSchema(testSchema, "LOWER", "MIXED", "upper");
    assertFalse("Lowercase table exists when removed by uppercase name", schema.tableExists("lower"));
    assertFalse("Mixed case table exists when removed by uppercase name", schema.tableExists("Mixed"));
    assertFalse("Uppercase table exists when removed by lowercase name", schema.tableExists("UPPER"));

    assertTrue("Lowercase sequence should still exist", schema.sequenceExists("lower"));
    assertTrue("Mixed case sequence should still exist", schema.sequenceExists("Mixed"));
    assertTrue("Uppercase sequence should still exist", schema.sequenceExists("UPPER"));
  }


  /**
   * Tests that the table and sequence lists are filtered using a case-insensitive comparator.
   */
  @Test
  public void testCaseInsensitiveRemovalForSequences() {
    Table lowerCaseTable = table("lower").columns(column("col", DataType.STRING, 10).nullable());
    Table mixedCaseTable = table("Mixed").columns(column("col", DataType.STRING, 10).nullable());
    Table upperCaseTable = table("UPPER").columns(column("col", DataType.STRING, 10).nullable());

    Sequence lowerCaseSequence= sequence("lower", 1,false);
    Sequence mixedCaseSequence = sequence("Mixed", 5, false);
    Sequence upperCaseSequence = sequence("UPPER", 10, false);

    Schema testSchema = schema(schema(lowerCaseTable, mixedCaseTable, upperCaseTable),
      schema(),
      schema(lowerCaseSequence,mixedCaseSequence,upperCaseSequence));

    TableSetSchema schema = new FilteredSchema(testSchema, ImmutableList.of("LOWER", "MIXED", "upper"),
      "LOWER", "MIXED", "upper");
    assertFalse("Lowercase table exists when removed by uppercase name", schema.tableExists("lower"));
    assertFalse("Mixed case table exists when removed by uppercase name", schema.tableExists("Mixed"));
    assertFalse("Uppercase table exists when removed by lowercase name", schema.tableExists("UPPER"));

    assertFalse("Lowercase sequence exists when removed by uppercase name", schema.sequenceExists("lower"));
    assertFalse("Mixed case sequence exists when removed by uppercase name", schema.sequenceExists("Mixed"));
    assertFalse("Uppercase sequence exists when removed by lowercase name", schema.sequenceExists("UPPER"));
  }

}
