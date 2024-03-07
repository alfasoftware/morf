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
import static org.junit.Assert.assertNotNull;

/**
 * Test class for {@link AddSequence}.
 *
 * @author Copyright (c) Alfa Financial Software 2024.
 */
public class TestAddSequence {

  /** Test data */
  private Table alphaTable;

  /** Test data */
  private Table bravoTable;

  /** Test data */
  private Sequence appleSequence;

  /** Test data */
  private Sequence mangoSequence;

  /** Test data */
  private AddSequence addSequence;

  /**
   * Setup the test case
   */
  @Before
  public void setUp() throws Exception {

    alphaTable = table("Alpha").columns(column("col", DataType.STRING, 10).nullable());

    bravoTable = table("Bravo").columns(column("col", DataType.STRING, 10).nullable());

    appleSequence = sequence("Apple");

    mangoSequence = sequence("Mango").startsWith(2).temporary();

    addSequence = new AddSequence(appleSequence);

  }


  /**
   * Test that a sequence can be added to a schema and that existing sequences and tables are unaffected
   */
  @Test
  public void testAddingSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(mangoSequence));

    Schema updatedSchema = addSequence.apply(testSchema);

    assertEquals("Post upgrade table count", 2, updatedSchema.tables().size());
    assertTrue(updatedSchema.tableExists("Alpha"));
    assertTrue(updatedSchema.tableExists("Bravo"));

    assertEquals("Post upgrade sequence count", 2, updatedSchema.sequences().size());
    assertTrue(updatedSchema.sequenceExists("Apple"));
    assertTrue(updatedSchema.sequenceExists("Mango"));

    Sequence resultSequence = updatedSchema.getSequence("Apple");

    assertNotNull(resultSequence);
  }


  /**
   * Test that the addition of a sequence can be reversed and pre-existing tables and sequences are unaffected.
   */
  @Test
  public void testReverseAddingOfSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(mangoSequence, appleSequence));

    assertEquals("Pre upgrade sequence count", 2, testSchema.sequences().size());
    assertTrue(testSchema.sequenceExists("Apple"));
    assertTrue(testSchema.sequenceExists("Mango"));

    Schema updatedSchema = addSequence.reverse(testSchema);

    assertEquals("Post upgrade table count", 2, updatedSchema.tables().size());
    assertTrue(updatedSchema.tableExists("Alpha"));
    assertTrue(updatedSchema.tableExists("Bravo"));

    assertEquals("Post upgrade sequence count", 1, updatedSchema.sequences().size());
    assertFalse(updatedSchema.sequenceExists("Apple"));
    assertTrue(updatedSchema.sequenceExists("Mango"));
  }


  /**
   * Test that attempting to add a new sequence fails if a sequence of the same name is already present
   */
  @Test
  public void testAddingExistantSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(mangoSequence, appleSequence));

    try {
      Sequence sequence = sequence("Apple");
      addSequence = new AddSequence(sequence);
      addSequence.apply(testSchema);
      fail("Should fail since sequence is already present");
    } catch (Exception e) {
      // Expected
    }
  }


  /**
   * Test that attempting to reverse an add sequence fails if the sequence does not currently exist
   */
  @Test
  public void testRemovingNonExistantSequence() {
    Schema testSchema = schema(schema(alphaTable, bravoTable),
      schema(),
      schema(mangoSequence));

    try {
      addSequence.reverse(testSchema);
      fail("Should fail since sequence is not present");
    } catch (Exception e) {
      // Expected
    }
  }

}
