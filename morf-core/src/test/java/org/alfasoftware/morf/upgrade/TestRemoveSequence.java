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

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Before;
import org.junit.Test;

import static org.alfasoftware.morf.metadata.SchemaUtils.*;
import static org.junit.Assert.*;

/**
 * Test class for {@link RemoveSequence}.
 *
 * @author Copyright (c) Alfa Financial Software 2024.
 */
public class TestRemoveSequence {

  /** Test data */
  private Table alphaTable;

  /** Test data */
  private Table bravoTable;

  /** Test data */
  private Sequence appleSequence;

  /** Test data */
  private Sequence mangoSequence;

  /** Test data */
  private RemoveSequence removeSequence;

  /**
   * Setup the test case
   */
  @Before
  public void setUp() throws Exception {

    alphaTable = table("Alpha").columns(column("col", DataType.STRING, 10).nullable());

    bravoTable = table("Bravo").columns(column("col", DataType.STRING, 10).nullable());

    appleSequence = sequence("Apple");

    mangoSequence = sequence("Mango").startsWith(2);

    removeSequence = new RemoveSequence(appleSequence);

  }


  /**
   * Test that a sequence can be removed from a schema and that existing sequences and tables are unaffected
   */
  @Test
  public void testRemovingSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(appleSequence, mangoSequence));

    Schema updatedSchema = removeSequence.apply(testSchema);

    assertEquals("Post upgrade table count", 2, updatedSchema.tables().size());
    assertTrue(updatedSchema.tableExists("Alpha"));
    assertTrue(updatedSchema.tableExists("Bravo"));

    assertEquals("Post upgrade sequence count", 1, updatedSchema.sequences().size());
    assertFalse(updatedSchema.sequenceExists("Apple"));
    assertTrue(updatedSchema.sequenceExists("Mango"));

    Sequence resultSequence = updatedSchema.getSequence("Mango");

    assertNotNull(resultSequence);
  }


  /**
   * Test that the removal of a sequence can be reversed and pre-existing tables and sequences are unaffected.
   */
  @Test
  public void testReverseRemovingSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(mangoSequence));

    assertEquals("Pre upgrade sequence count", 1, testSchema.sequences().size());
    assertFalse(testSchema.sequenceExists("Apple"));
    assertTrue(testSchema.sequenceExists("Mango"));

    Schema updatedSchema = removeSequence.reverse(testSchema);

    assertEquals("Post upgrade table count", 2, updatedSchema.tables().size());
    assertTrue(updatedSchema.tableExists("Alpha"));
    assertTrue(updatedSchema.tableExists("Bravo"));

    assertEquals("Post upgrade sequence count", 2, updatedSchema.sequences().size());
    assertTrue(updatedSchema.sequenceExists("Apple"));
    assertTrue(updatedSchema.sequenceExists("Mango"));
  }


  /**
   * Tests that attempting to removing a sequence if the sequence is not present.
   */
  @Test
  public void testRemovingNonExistantSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(appleSequence));

    try {
      Sequence sequence = sequence("Orange").startsWith(5).temporary();
      removeSequence = new RemoveSequence(sequence);
      removeSequence.apply(testSchema);
      fail("Should fail since sequence is not there");
    } catch (Exception e) {
      // Expected
    }
  }


  /**
   * Tests that attempting to reversing a remove sequence fails if the sequence is currently present on the schema.
   */
  @Test
  public void testReversingForExistantSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(appleSequence));

    try {
      removeSequence.reverse(testSchema);
      fail("Should fail since sequence is presently on schema");
    } catch (Exception e) {
      // Expected
    }
  }

}
