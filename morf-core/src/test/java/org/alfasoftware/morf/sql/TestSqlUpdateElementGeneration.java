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
import org.alfasoftware.morf.sql.element.FieldFromSelect;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 * Tests for Update Statements
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSqlUpdateElementGeneration {

  /**
   * Tests a simple update without any source
   */
  @Test
  public void testSimpleUpdate() {
    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).set(new FieldLiteral("A1001001").as("agreementNumber"));

    // Check the positives
    assertNotNull("Has a table selected", stmt.getTable());
    assertEquals("Has the agreement table", "Agreement", stmt.getTable().getName());

    // Check for side effects
    assertNull("Should be no where criteria", stmt.getWhereCriterion());
    assertNotNull("Should be a field list", stmt.getFields());
    assertEquals("Field list should have 1", 1, stmt.getFields().size());
  }

  /**
   * Tests that the {@link UseParallelDml} hint can be added to the update statement.
   */
  @Test
  public void testUpdateWithUseParallelDmlHintWithoutDegreeOfParallelism() {
    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).useParallelDml()
            .set(new FieldLiteral("A1001001").as("agreementNumber"));

    assertTrue("Use parallel dml hint", stmt.getHints().contains(new UseParallelDml()));
  }

  /**
   * Tests that the {@link UseParallelDml} hint can be added to the update statement.
   */
  @Test
  public void testUpdateWithUseParallelDmlHintWithDegreeOfParallelism() {
    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).useParallelDml(4)
            .set(new FieldLiteral("A1001001").as("agreementNumber"));

    assertTrue("Use parallel dml hint with degreeOfParallelism", stmt.getHints().contains(new UseParallelDml(4)));
  }

  /**
   * Tests update statement with where clause
   */
  @Test
  public void testUpdateWithWhereCriterion() {

    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).set(new FieldLiteral("A1001001").as("agreementNumber")).where(Criterion.eq(new FieldReference("productCode"), "CONT"));

    // Check the positives
    assertEquals("Should be 1 fields", 1, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called Agreement", "Agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());
    assertNotNull("Field name should be set", stmt.getWhereCriterion().getField());
    assertEquals("Field in where should be productCode", "productCode", ((FieldReference) stmt.getWhereCriterion().getField()).getName());
    assertTrue("Class of object in where clause should be string", stmt.getWhereCriterion().getValue() instanceof String);
    assertEquals("String value in where clause should be CONT", "CONT", stmt.getWhereCriterion().getValue());
  }


  /**
   * Tests updating multiple fields including field from select in an update statement
   */
  @Test
  public void testUpdateWithMultipleFields() {

    SelectStatement fieldOneSelect = new SelectStatement(new FieldReference("assetCost")).from(new TableReference("Schedule"))
        .where(Criterion.eq(new FieldReference(new TableReference("Schedule"), "agreementNumber"), "A001003657"));

    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).set(
      new FieldFromSelect(fieldOneSelect).as("agreementAssetCost"), new FieldLiteral("blank").as("agreementDescription"))
        .where(Criterion.eq(new FieldReference("productCode"), "CONT"));

    // Check the positives
    assertEquals("Should be 2 fields", 2, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called Agreement", "Agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());
    assertNotNull("Field name should be set", stmt.getWhereCriterion().getField());
    assertEquals("Field in where should be productCode", "productCode", ((FieldReference) stmt.getWhereCriterion().getField()).getName());
    assertTrue("Class of object in where clause should be string", stmt.getWhereCriterion().getValue() instanceof String);
    assertEquals("String value in where clause should be CONT", "CONT", stmt.getWhereCriterion().getValue());

    // Test Field From Select properties
    FieldFromSelect ffs = (FieldFromSelect) stmt.getFields().get(0);
    assertEquals("Field count in field from select should be 1", 1, ffs.getSelectStatement().getFields().size());
    assertEquals("Field name in field from select should be assetCost", "assetCost", ((FieldReference) ffs.getSelectStatement().getFields().get(0)).getName());
  }


  /**
   * Test that deep copy works as expected.
   */
  @Test
  public void testDeepCopy(){
    SelectStatement fieldOneSelect = new SelectStatement(new FieldReference("assetCost")).from(new TableReference("Schedule"))
        .where(Criterion.eq(new FieldReference(new TableReference("Schedule"), "agreementNumber"), "A001003657"));

    UpdateStatement stmt = new UpdateStatement(new TableReference("Agreement")).set(
      new FieldFromSelect(fieldOneSelect).as("agreementAssetCost"), new FieldLiteral("blank").as("agreementDescription")).where(
      Criterion.eq(new FieldReference("productCode"), "CONT"));

    UpdateStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of field from select", stmt.getFields().get(0) != stmtCopy.getFields().get(0));
    assertEquals("Field should match", ((FieldFromSelect) stmt.getFields().get(0)).getAlias(), ((FieldFromSelect) stmtCopy.getFields().get(0)).getAlias());
    assertTrue("Should be different instance of field literal", stmt.getFields().get(1) != stmtCopy.getFields().get(1));
    assertEquals("Field should match", ((FieldLiteral) stmt.getFields().get(1)).getAlias(), ((FieldLiteral) stmtCopy.getFields().get(1)).getAlias());
    assertTrue("Should be different instance of table", stmt.getTable() != stmtCopy.getTable());
    assertEquals("Table should match", stmt.getTable().getName(), stmtCopy.getTable().getName());
    assertTrue("Should be different instance of where criterion", stmt.getWhereCriterion() != stmtCopy.getWhereCriterion());
    assertEquals("Where criteria operator should match", stmt.getWhereCriterion().getOperator(), stmtCopy.getWhereCriterion().getOperator());
    assertEquals("Where criteria field should match", ((FieldReference) stmt.getWhereCriterion().getField()).getName(), ((FieldReference) stmtCopy.getWhereCriterion().getField()).getName());
    assertEquals("Where criteria value should match", stmt.getWhereCriterion().getValue(), stmtCopy.getWhereCriterion().getValue());

  }

}
