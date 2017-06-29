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
import static org.junit.Assert.fail;

import org.junit.Test;

import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Tests for Insert Statements
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSqlInsertElementGeneration {

  /**
   * Tests a simple insert without any source
   */
  @Test
  public void testSimpleInsert() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement"));

    // Check the positives
    assertNotNull("Has a table selected", stmt.getTable());
    assertEquals("Has the agreement table", "Agreement", stmt.getTable().getName());

    // Check for side effects
    assertNull("Should be no select statement", stmt.getSelectStatement());
    assertNull("Should be no from table", stmt.getFromTable());
    assertNotNull("Should be a field list", stmt.getFields());
    assertEquals("Field list should be empty", 0, stmt.getFields().size());
  }


  /**
   * Tests a simple insert with a source table
   */
  @Test
  public void testSimpleInsertWithSourceTable() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement")).from(new TableReference("Agreement2"));

    // Check the positives
    assertNotNull("Has a table selected", stmt.getTable());
    assertEquals("Has the agreement table", "Agreement", stmt.getTable().getName());
    assertNotNull("Has a from table selected", stmt.getFromTable());
    assertEquals("Has the agreement2 table", "Agreement2", stmt.getFromTable().getName());

    // Check for side effects
    assertNull("Should be no select statement", stmt.getSelectStatement());
    assertNotNull("Should be a field list", stmt.getFields());
    assertEquals("Field list should be empty", 0, stmt.getFields().size());
  }


  /**
   * Tests a simple insert with a field list
   */
  @Test
  public void testSimpleInsertWithFields() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement")).fields(new FieldReference("A"), new FieldReference("B"), new FieldReference("C"));

    // Check the positives
    assertNotNull("Has a table selected", stmt.getTable());
    assertEquals("Has the agreement table", "Agreement", stmt.getTable().getName());
    assertNotNull("Has a field list", stmt.getFields());
    assertEquals("Has three fields", 3, stmt.getFields().size());
    assertEquals("First field is A", "A", ((FieldReference) stmt.getFields().get(0)).getName());
    assertEquals("Second field is B", "B", ((FieldReference) stmt.getFields().get(1)).getName());
    assertEquals("Third field is C", "C", ((FieldReference) stmt.getFields().get(2)).getName());

    // Check for side effects
    assertNull("Should be no select statement", stmt.getSelectStatement());
    assertNull("Should be no from table", stmt.getFromTable());
  }


  /**
   * Tests that you can't add a select statement and a table
   */
  @Test
  public void testAddBothSourceTableAndSelect() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("agreement"));

    try {
      stmt.from(new TableReference("agreement1"));
      stmt.from(new SelectStatement().from(new TableReference("agreement1")));

      fail("Should not be able to specify a source table and then a select statement");
    } catch(UnsupportedOperationException e) {
      // OK
    }

    stmt = new InsertStatement().into(new TableReference("agreement"));

    try {
      stmt.from(new SelectStatement().from(new TableReference("agreement1")));
      stmt.from(new TableReference("agreement1"));

      fail("Should not be able to specify a select statement and then a source table");
    } catch(UnsupportedOperationException e) {
      // OK
    }
  }


  /**
   * Tests that you can't add a from table and a field list
   */
  @Test
  public void testAddBothSourceTableAndFieldList() {
    InsertStatement stmt = new InsertStatement().into(new TableReference("agreement"));

    try {
      stmt.from(new TableReference("agreement1"));
      stmt.fields(new FieldReference("test"));

      fail("Should not be able to specify a source table and then a field list");
    } catch(UnsupportedOperationException e) {
      // OK
    }

    stmt = new InsertStatement().into(new TableReference("agreement"));

    try {
      stmt.fields(new FieldReference("test"));
      stmt.from(new TableReference("agreement1"));

      fail("Should not be able to specify a field list and then a source table");
    } catch(UnsupportedOperationException e) {
      // OK
    }
  }

  /**
   * Test that deep copy works for insert with fields.
   */
  @Test
  public void testDeepCopyForInsertWithFields(){
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement")).fields(new FieldReference("A"), new FieldReference("B"), new FieldReference("C"));
    InsertStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of the table", stmt.getTable() != stmtCopy.getTable());
    assertEquals("Table name should match", stmt.getTable().getName(), stmtCopy.getTable().getName());
    for(int i = 0; i< stmt.getFields().size(); i++ ){
      assertTrue("Should be different instance of field", stmt.getFields().get(i) != stmtCopy.getFields().get(i));
      assertEquals("Field should match", ((FieldReference) stmt.getFields().get(i)).getName(), ((FieldReference) stmtCopy.getFields().get(i)).getName());
    }
  }


  /**
   * Test that deep copy works for insert with source table.
   */
  @Test
  public void testDeepCopyForInsertWithSourceTable(){
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement")).from(new TableReference("Agreement2"));
    InsertStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of source table", stmt.getFromTable() != stmtCopy.getFromTable());
    assertEquals("Source table name should match", stmt.getFromTable().getName(), stmtCopy.getFromTable().getName());
  }


  /**
   * Test that deep copy works for insert with select.
   */
  @Test
  public void testDeepCopyForInsertWithSelect(){
    InsertStatement stmt = new InsertStatement().into(new TableReference("Agreement")).from(new SelectStatement().from(new TableReference("agreement1")));
    InsertStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of select statement", stmt.getSelectStatement() != stmtCopy.getSelectStatement());
    assertTrue("Should be different instance of table in select statement table", stmt.getSelectStatement().getTable() != stmtCopy.getSelectStatement().getTable());
    assertEquals("Table name in select statement should match", stmt.getSelectStatement().getTable().getName(), stmtCopy.getSelectStatement().getTable().getName());
  }

}
