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

package org.alfasoftware.morf.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Tests for Delete Statements
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestSqlDeleteElementGeneration {

  /**
   * Tests a simple delete without any criterion
   */
  @Test
  public void testSimpleDelete() {
    DeleteStatement stmt = new DeleteStatement(new TableReference("Agreement"));

    // Check the positives
    assertNotNull("Has a table selected", stmt.getTable());
    assertEquals("Has the agreement table", "Agreement", stmt.getTable().getName());

    // Check for side effects
    assertNull("Should be no where criteria", stmt.getWhereCriterion());
  }


  /**
   * Tests delete statement with where clause
   */
  @Test
  public void testDeleteWithWhereCriterion() {

    DeleteStatement stmt = new DeleteStatement(new TableReference("Agreement")).where(Criterion.eq(new FieldReference("productCode"), "CONT"));

    // Check the positives
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called Agreement", "Agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());
    assertNotNull("Field name should be set", stmt.getWhereCriterion().getField());
    assertEquals("Field in where should be productCode", "productCode", ((FieldReference) stmt.getWhereCriterion().getField()).getName());
    assertTrue("Class of object in where clause should be string", stmt.getWhereCriterion().getValue() instanceof String);
    assertEquals("String value in where clause should be CONT", "CONT", stmt.getWhereCriterion().getValue());
  }


  /**
   * Test that deep copy works as expected.
   */
  @Test
  public void testDeepCopy(){
    DeleteStatement stmt = new DeleteStatement(new TableReference("Agreement")).where(Criterion.eq(new FieldReference("productCode"), "CONT"));

    DeleteStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of table", stmt.getTable() != stmtCopy.getTable());
    assertEquals("Table should match", stmt.getTable().getName(), stmtCopy.getTable().getName());
    assertTrue("Should be different instance of where criterion", stmt.getWhereCriterion() != stmtCopy.getWhereCriterion());
    assertEquals("Where criteria operator should match", stmt.getWhereCriterion().getOperator(), stmtCopy.getWhereCriterion().getOperator());
    assertEquals("Where criteria field should match", ((FieldReference) stmt.getWhereCriterion().getField()).getName(), ((FieldReference) stmtCopy.getWhereCriterion().getField()).getName());
    assertEquals("Where criteria value should match", stmt.getWhereCriterion().getValue(), stmtCopy.getWhereCriterion().getValue());
  }
}
