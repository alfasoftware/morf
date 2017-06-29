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

package org.alfasoftware.morf.sql.element;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

/**
 * Tests for field literals
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestFieldReference {


  /**
   * Verify that deep copy works as expected for string field reference.
   */
  @Test
  public void testDeepCopyWithString() {
    FieldReference fr = new FieldReference("TEST1");
    fr.as("testName");
    FieldReference frCopy = (FieldReference)fr.deepCopy();

    assertEquals("Field reference name matches", fr.getName(), frCopy.getName());
    assertEquals("Field reference alias type matches", fr.getAlias(), frCopy.getAlias());
    assertEquals("Field reference direction matches", fr.getDirection(), frCopy.getDirection());
  }


  /**
   * Verify that deep copy works as expected for Boolean field reference.
   */
  @Test
  public void testDeepCopyWithTable() {
    FieldReference fr = new FieldReference(new TableReference("Agreement"), "Test1");
    fr.as("testName");
    FieldReference frCopy = (FieldReference)fr.deepCopy();

    assertEquals("Field reference name matches", fr.getName(), frCopy.getName());
    assertEquals("Field reference alias type matches", fr.getAlias(), frCopy.getAlias());
    assertEquals("Field reference direction matches", fr.getDirection(), frCopy.getDirection());
    assertEquals("Field reference table matches", fr.getTable().getName(), frCopy.getTable().getName());
  }

  /**
   * Confirms that the implied name returns the alias if it is set,
   * otherwise the name of the source field
   */
  @Test
  public void testImpliedName() {
    FieldReference fr = new FieldReference(new TableReference("Agreement"), "Test1");
    assertEquals("Field reference implied name defaults to source field name", fr.getImpliedName(), "Test1");
    fr.as("testName");
    assertEquals("Field reference implied name  uses alias if available", fr.getImpliedName(), "testName");
  }
  
  
  /**
   * Test the equals method. 
   */
  @Test
  public void testEquals() {
    final String fieldName = "date";
    final String anotherFieldName = "amount";
    final TableReference table = new TableReference("Schedule");
    final TableReference anotherTable = new TableReference("Asset");
    
    FieldReference fOriginal = new FieldReference(table, fieldName, Direction.ASCENDING, NullValueHandling.FIRST);
    FieldReference fSame = new FieldReference(table, fieldName, Direction.ASCENDING, NullValueHandling.FIRST);
    
    FieldReference fDiffTable = new FieldReference(anotherTable, fieldName, Direction.ASCENDING, NullValueHandling.FIRST);
    FieldReference fDiffField = new FieldReference(table, anotherFieldName, Direction.ASCENDING, NullValueHandling.FIRST);
    FieldReference fDiffDirection = new FieldReference(table, fieldName, Direction.DESCENDING, NullValueHandling.FIRST);
    FieldReference fDiffNullHandling = new FieldReference(table, fieldName, Direction.ASCENDING, NullValueHandling.LAST);
    
    assertEquals("Fields should be the same", fOriginal, fSame);
    assertEquals("Fields should be the same", fSame, fOriginal);
    assertNotEquals("Tables differ", fOriginal,  fDiffTable);
    assertNotEquals("Fields differ", fOriginal,  fDiffField);
    assertNotEquals("Directions differ", fOriginal,  fDiffDirection);
    assertNotEquals("Null handling differs", fOriginal,  fDiffNullHandling);
  }
}
