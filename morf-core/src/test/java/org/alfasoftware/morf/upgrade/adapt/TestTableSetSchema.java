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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.metadata.Sequence;
import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;

/**
 * Test the functionality provided by {@link TableSetSchema}
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */

public class TestTableSetSchema {

  /**
   * The schema to use for the tests
   */
  private TableSetSchema schema;

  /**
   * Creates two table entries for testing.
   */
  @Before
  public void setUp() {
    Set<Table> tables = new HashSet<Table>();
    tables.add(table("Apple").columns(column("firmness", DataType.STRING, 10).nullable()));
    tables.add(table("Mango").columns(column("smell", DataType.STRING, 10).nullable()));

    Set<Sequence> sequences = new HashSet<Sequence>();
    sequences.add(sequence("First", 1, false));
    sequences.add(sequence("Second", 5, false));

    schema = new TableSetSchema(tables, sequences);
  }


  /**
   * Test that the Tableset schema can report whether a table exists by name
   */
  @Test
  public void testTableExists() {
    assertTrue(schema.tableExists("Apple"));
    assertFalse(schema.tableExists("Peach"));
  }


  /**
   * Tables should not be treated as case sensitive
   */
  @Test
  public void testTableExistsNotCaseSensitive() {
    assertTrue(schema.tableExists("APPLE"));
    assertTrue(schema.tableExists("apple"));
  }


  /**
   * Tests that the TableSet return the correct {@linkplain Table} reference.
   */
  @Test
  public void testGetTable() {
    // Given...
    final String tableName = "Apple";

    // When...
    Table appleTable = schema.getTable(tableName);

    // Then...
    assertNotNull(appleTable);
    assertEquals(tableName, appleTable.getName());
  }


  /**
   * Tables should not be treated as case sensitive
   */
  @Test
  public void testGetTableCaseInsensitive() {
    // Given...
    final String tableName = "Apple";

    // When...
    Table uppercaseTable = schema.getTable(tableName.toUpperCase());
    Table lowercaseTable = schema.getTable(tableName.toLowerCase());

    // Then...
    assertNotNull(uppercaseTable);
    assertNotNull(lowercaseTable);
  }


  /**
   * {@link TableSetSchema#getTable(String)} should throw an exception if the
   * table doesn't doesn't exist in the schema set
   *
   * <p>N.B.: Peach should always throw an exception.</p>
   */
  @Test(expected = RuntimeException.class)
  public void testGetTableWhenTableDoesntExist() {
    // Given..
    final String wrongTable = "Peach";

    // When...
    schema.getTable(wrongTable);

    // Then...
    // RuntimeException should've been raised
  }


  /**
   * Test that the SequenceSet schema can report whether a sequence exists by name
   */
  @Test
  public void testSequenceExists() {
    assertTrue(schema.sequenceExists("First"));
    assertFalse(schema.tableExists("Third"));
  }


  /**
   * Sequences should not be treated as case sensitive
   */
  @Test
  public void testSequenceExistsNotCaseSensitive() {
    assertTrue(schema.sequenceExists("FIRST"));
    assertTrue(schema.sequenceExists("first"));
  }


  /**
   * Tests that the SequenceSet return the correct {@linkplain Sequence} reference.
   */
  @Test
  public void testGetSequence() {
    // Given...
    final String sequenceName = "First";

    // When...
    Sequence firstSequence = schema.getSequence(sequenceName);

    // Then...
    assertNotNull(firstSequence);
    assertEquals(sequenceName, firstSequence.getName());
  }


  /**
   * Sequences should not be treated as case sensitive
   */
  @Test
  public void testGetSequenceCaseInsensitive() {
    // Given...
    final String sequenceName = "First";

    // When...
    Sequence uppercaseSequence = schema.getSequence(sequenceName.toUpperCase());
    Sequence lowercaseSequence = schema.getSequence(sequenceName.toLowerCase());

    // Then...
    assertNotNull(uppercaseSequence);
    assertNotNull(lowercaseSequence);
  }


  /**
   * {@link TableSetSchema#getSequence(String)} should throw an exception if the
   * sequence doesn't exist in the schema set
   *
   * <p>N.B.: Peach should always throw an exception.</p>
   */
  @Test(expected = RuntimeException.class)
  public void testGetSequenceWhenSequenceDoesntExist() {
    // Given..
    final String wrongSequence = "Third";

    // When...
    schema.getSequence(wrongSequence);

    // Then...
    // RuntimeException should've been raised
  }
}
