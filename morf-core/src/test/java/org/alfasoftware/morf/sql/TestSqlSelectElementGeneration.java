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

import java.util.List;

import org.junit.Test;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.Operator;
import org.alfasoftware.morf.sql.element.TableReference;
import com.google.common.collect.ImmutableSet;

/**
 * Test that the SQL Select statement behaves as expected.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class TestSqlSelectElementGeneration {

  /**
   * Tests a simple select over a table
   */
  @Test
  public void testSimpleSelect() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Test a simple select with list of fields
   */
  @Test
  public void testSimpleSelectOfFields() {
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber"),
                                               new FieldReference("startDate"),
                                               new FieldReference("endDate").as("agreementEndDate"))
                                              .from(new TableReference("agreement"));

    // Check the positives
    assertEquals("Should be three fields", 3, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());

    FieldReference firstField = (FieldReference) stmt.getFields().get(0);
    FieldReference secondField = (FieldReference) stmt.getFields().get(1);
    FieldReference thirdField = (FieldReference) stmt.getFields().get(2);

    assertNotNull("First field must not be null", firstField);
    assertNotNull("Second field must not be null", secondField);
    assertNotNull("Third field must not be null", thirdField);

    assertEquals("First field should be agreement number", "agreementNumber", firstField.getName());
    assertEquals("Second field should be start date", "startDate", secondField.getName());
    assertEquals("Third field should be end date", "endDate", thirdField.getName());

    assertEquals("No alias on first field", "", firstField.getAlias());
    assertEquals("No alias on second field", "", secondField.getAlias());
    assertEquals("Alias should be set on third field", "agreementEndDate", thirdField.getAlias());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Test a simple select with list of fields
   */
  @Test
  public void testSimpleSelectOfFieldsAndLiterals() {
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber"),
                                               new FieldReference("startDate"),
                                               new FieldReference("endDate").as("agreementEndDate"),
                                               new FieldLiteral("LITERAL_VALUE").as("lastColumn"))
                                              .from(new TableReference("agreement"));

    // Check the positives
    assertEquals("Should be three fields", 4, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());

    FieldReference firstField = (FieldReference) stmt.getFields().get(0);
    FieldReference secondField = (FieldReference) stmt.getFields().get(1);
    FieldReference thirdField = (FieldReference) stmt.getFields().get(2);
    AliasedField fourthField = stmt.getFields().get(3);

    assertNotNull("First field must not be null", firstField);
    assertNotNull("Second field must not be null", secondField);
    assertNotNull("Third field must not be null", thirdField);
    assertNotNull("Third field must not be null", fourthField);

    assertEquals("First field should be agreement number", "agreementNumber", firstField.getName());
    assertEquals("Second field should be start date", "startDate", secondField.getName());
    assertEquals("Third field should be end date", "endDate", thirdField.getName());

    assertTrue("Fourth field should be a literal", fourthField instanceof FieldLiteral);

    FieldLiteral fourthFieldLiteral = (FieldLiteral) fourthField;

    assertEquals("Literal value should be set", "LITERAL_VALUE", fourthFieldLiteral.getValue());

    assertEquals("No alias on first field", "", firstField.getAlias());
    assertEquals("No alias on second field", "", secondField.getAlias());
    assertEquals("Alias should be set on third field", "agreementEndDate", thirdField.getAlias());
    assertEquals("Alias should be set on fourth field", "lastColumn", fourthField.getAlias());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with where clause
   */
  @Test
  public void testSimpleSelectWithWhere() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement")).where(Criterion.eq(new FieldReference("agreementNumber"), "A0001"));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());
    assertNotNull("Field name should be set", stmt.getWhereCriterion().getField());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) stmt.getWhereCriterion().getField()).getName());
    assertTrue("Class of object in where clause should be string", stmt.getWhereCriterion().getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", stmt.getWhereCriterion().getValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with an "or" criterion
   */
  @Test
  public void testSimpleSelectWithOrInWhere() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                    .where(Criterion.or(
                             Criterion.eq(new FieldReference("agreementNumber"), "A0001"),
                             Criterion.greaterThan(new FieldReference("startDate"), new Integer(20080101))
                           ));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());

    assertEquals("Top level criterion should be an or operation", Operator.OR, stmt.getWhereCriterion().getOperator());
    assertEquals("And operation should have two criteria", 2, stmt.getWhereCriterion().getCriteria().size());

    Criterion firstCriterion = stmt.getWhereCriterion().getCriteria().get(0);
    Criterion secondCriterion = stmt.getWhereCriterion().getCriteria().get(1);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("First operator should be equals", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be string", firstCriterion.getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", firstCriterion.getValue());

    assertNotNull("Field name should be set", secondCriterion.getField());
    assertEquals("Second operator should be greater than", Operator.GT, secondCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "startDate", ((FieldReference) secondCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", secondCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20080101", 20080101, ((Integer) secondCriterion.getValue()).intValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with an "and" criterion
   */
  @Test
  public void testSimpleSelectWithAndInWhere() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                    .where(Criterion.and(
                             Criterion.eq(new FieldReference("agreementNumber"), "A0001"),
                             Criterion.greaterThan(new FieldReference("startDate"), new Integer(20080101))
                           ));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());

    assertEquals("Top level criterion should be an and operation", Operator.AND, stmt.getWhereCriterion().getOperator());
    assertEquals("And operation should have two criteria", 2, stmt.getWhereCriterion().getCriteria().size());

    Criterion firstCriterion = stmt.getWhereCriterion().getCriteria().get(0);
    Criterion secondCriterion = stmt.getWhereCriterion().getCriteria().get(1);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("First operator should be equals", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be string", firstCriterion.getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", firstCriterion.getValue());

    assertNotNull("Field name should be set", secondCriterion.getField());
    assertEquals("Second operator should be greater than", Operator.GT, secondCriterion.getOperator());
    assertEquals("Field should be startDate", "startDate", ((FieldReference) secondCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", secondCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20080101", 20080101, ((Integer) secondCriterion.getValue()).intValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with a "not" operation
   */
  @Test
  public void testSimpleSelectWithNotInWhere() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                    .where(Criterion.not(
                             Criterion.eq(new FieldReference("agreementNumber"), "A0001")
                           ));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());

    assertEquals("Top level criterion should be a not operation", Operator.NOT, stmt.getWhereCriterion().getOperator());
    assertEquals("And operation should have two criteria", 1, stmt.getWhereCriterion().getCriteria().size());

    Criterion firstCriterion = stmt.getWhereCriterion().getCriteria().get(0);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("First operator should be equals", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be string", firstCriterion.getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", firstCriterion.getValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with multiple where clauses
   */
  @Test
  public void testSimpleSelectWithMultipleWheres() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                    .where(Criterion.and(
                             Criterion.eq(new FieldReference("agreementNumber"), "A0001"),
                             Criterion.greaterThan(new FieldReference("startDate"), new Integer(20080101)),
                             Criterion.lessThan(new FieldReference("endDate"), new Integer(20090101))
                           ));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());

    assertEquals("Top level criterion should be a not operation", Operator.AND, stmt.getWhereCriterion().getOperator());
    assertEquals("And operation should have three criteria", 3, stmt.getWhereCriterion().getCriteria().size());

    Criterion firstCriterion = stmt.getWhereCriterion().getCriteria().get(0);
    Criterion secondCriterion = stmt.getWhereCriterion().getCriteria().get(1);
    Criterion thirdCriterion = stmt.getWhereCriterion().getCriteria().get(2);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("First operator should be equals", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be string", firstCriterion.getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", firstCriterion.getValue());

    assertNotNull("Field name should be set", secondCriterion.getField());
    assertEquals("Second operator should be greater than", Operator.GT, secondCriterion.getOperator());
    assertEquals("Field should be startDate", "startDate", ((FieldReference) secondCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", secondCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20080101", 20080101, ((Integer) secondCriterion.getValue()).intValue());

    assertNotNull("Field name should be set", thirdCriterion.getField());
    assertEquals("Third operator should be less than", Operator.LT, thirdCriterion.getOperator());
    assertEquals("Field should be endDate", "endDate", ((FieldReference) thirdCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", thirdCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20090101", 20090101, ((Integer) thirdCriterion.getValue()).intValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with nested where statements
   */
  @Test
  public void testSimpleSelectWithNestedWheres() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                    .where(Criterion.and(
                             Criterion.eq(new FieldReference("agreementNumber"), "A0001"),
                             Criterion.or(
                               Criterion.greaterThan(new FieldReference("startDate"), new Integer(20080101)),
                               Criterion.lessThan(new FieldReference("endDate"), new Integer(20090101))
                             )
                           ));

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertNotNull("Should have a where clause", stmt.getWhereCriterion());

    assertEquals("Top level criterion should be a not operation", Operator.AND, stmt.getWhereCriterion().getOperator());
    assertEquals("And operation should have three criteria", 2, stmt.getWhereCriterion().getCriteria().size());

    Criterion firstCriterion = stmt.getWhereCriterion().getCriteria().get(0);
    Criterion secondCriterion = stmt.getWhereCriterion().getCriteria().get(1);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("First operator should be equals", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be string", firstCriterion.getValue() instanceof String);
    assertEquals("String value in where clause should be A0001", "A0001", firstCriterion.getValue());

    assertNull("Field should not be set on second criterion", secondCriterion.getField());
    assertEquals("Second operator should be or", Operator.OR, secondCriterion.getOperator());
    assertNull("Object should not be set", secondCriterion.getValue());

    firstCriterion = secondCriterion.getCriteria().get(0);
    secondCriterion = secondCriterion.getCriteria().get(1);

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("Second operator should be greater than", Operator.GT, firstCriterion.getOperator());
    assertEquals("Field should be startDate", "startDate", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", firstCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20080101", 20080101, ((Integer) firstCriterion.getValue()).intValue());

    assertNotNull("Field name should be set", secondCriterion.getField());
    assertEquals("Third operator should be less than", Operator.LT, secondCriterion.getOperator());
    assertEquals("Field should be endDate", "endDate", ((FieldReference) secondCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be integer", secondCriterion.getValue() instanceof Integer);
    assertEquals("Integer value in where clause should be 20090101", 20090101, ((Integer) secondCriterion.getValue()).intValue());

    // Check that no side-effects have occurred
    assertEquals("Should have no join clause", 0, stmt.getJoins().size());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with a join clause
   */
  @Test
  public void testSimpleSelectJoin() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                                                .innerJoin(new TableReference("schedule"),
                                                   Criterion.eq(new FieldReference(new TableReference("agreement"), "agreementNumber"),
                                                                new FieldReference(new TableReference("schedule"), "schAgreementNumber"))
                                                );

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertEquals("Should be a single joined table", 1, stmt.getJoins().size());

    Join firstJoin = stmt.getJoins().get(0);
    Criterion criterion = firstJoin.getCriterion();

    assertNotNull("Should have a criterion", criterion);
    assertEquals("Should be an equals operator on join", Operator.EQ, criterion.getOperator());
    assertEquals("Should be an inner join", JoinType.INNER_JOIN, firstJoin.getType());

    assertNotNull("Should have a table name on join", firstJoin.getTable());
    assertEquals("Join table should be schedule", "schedule", firstJoin.getTable().getName());

    assertNotNull("Field name should be set", criterion.getField());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) criterion.getField()).getName());
    assertTrue("Class of object in where clause should be field", criterion.getValue() instanceof FieldReference);
    assertEquals("Field name in where clause should be agreement number", "schAgreementNumber", ((FieldReference) criterion.getValue()).getName());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with a left join clause to a select statement
   */
  @Test
  public void testSimpleSelectLeftOuterJoinToSelect() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                                                .leftOuterJoin(new SelectStatement(),
                                                   Criterion.eq(new FieldReference(new TableReference("agreement"), "agreementNumber"),
                                                                new FieldReference(new TableReference("schedule"), "schAgreementNumber"))
                                                );

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertEquals("Should be a single joined statement", 1, stmt.getJoins().size());

    Join firstJoin = stmt.getJoins().get(0);
    Criterion criterion = firstJoin.getCriterion();

    assertNotNull("Should have a criterion", criterion);
    assertEquals("Should be an equals operator on join", Operator.EQ, criterion.getOperator());
    assertEquals("Should be a left join", JoinType.LEFT_OUTER_JOIN, firstJoin.getType());

    assertNull("Should have no table on join", firstJoin.getTable());
    assertNotNull("Should have select on join", firstJoin.getSubSelect());

    assertNotNull("Field name should be set", criterion.getField());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) criterion.getField()).getName());
    assertTrue("Class of object in where clause should be field", criterion.getValue() instanceof FieldReference);
    assertEquals("Field name in where clause should be agreement number", "schAgreementNumber", ((FieldReference) criterion.getValue()).getName());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with multiple joined tables
   */
  @Test
  public void testSimpleSelectMultipleJoin() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                                                .innerJoin(new TableReference("schedule"),
                                                   Criterion.eq(new FieldReference(new TableReference("agreement"), "agreementNumber"),
                                                                new FieldReference(new TableReference("schedule"), "schAgreementNumber"))
                                                ).leftOuterJoin(new TableReference("receivable"),
                                                   Criterion.and(
                                                     Criterion.eq(new FieldReference(new TableReference("schedule"), "schAgreementNumber"),
                                                                  new FieldReference(new TableReference("receivable"), "agreementNumber")),
                                                     Criterion.eq(new FieldReference(new TableReference("schedule"), "scheduleNumber"),
                                                                  new FieldReference(new TableReference("receivable"), "scheduleNumber"))
                                                   )
                                                );

    // Check the positives
    assertEquals("Should be no fields", 0, stmt.getFields().size());
    assertNotNull("Should be a single table", stmt.getTable());
    assertEquals("Table should be called agreement", "agreement", stmt.getTable().getName());
    assertEquals("Should be two joined tables", 2, stmt.getJoins().size());

    Join firstJoin = stmt.getJoins().get(0);
    Join secondJoin = stmt.getJoins().get(1);
    Criterion firstCriterion = firstJoin.getCriterion();

    assertNotNull("Should have a criterion", firstCriterion);
    assertEquals("Should be an equals operator on join", Operator.EQ, firstCriterion.getOperator());
    assertEquals("Should be an inner join", JoinType.INNER_JOIN, firstJoin.getType());

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("Field should be agreementNumber", "agreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be field", firstCriterion.getValue() instanceof FieldReference);
    assertEquals("Field name in where clause should be agreement number", "schAgreementNumber", ((FieldReference) firstCriterion.getValue()).getName());

    Criterion wrapperCriterion = secondJoin.getCriterion();

    assertNotNull("Should have a criterion", wrapperCriterion );
    assertEquals("Should be an equals operator on join", Operator.AND, wrapperCriterion.getOperator());
    assertEquals("Should be an outer join", JoinType.LEFT_OUTER_JOIN, secondJoin.getType());

    assertEquals("Second join should have two criteria within the and criterion", 2, wrapperCriterion.getCriteria().size());

    firstCriterion = wrapperCriterion.getCriteria().get(0);
    Criterion secondCriterion = wrapperCriterion.getCriteria().get(1);

    assertNotNull("Should have a criterion", firstCriterion);
    assertEquals("Should be an equals operator on join", Operator.EQ, firstCriterion.getOperator());

    assertNotNull("Field name should be set", firstCriterion.getField());
    assertEquals("Field should be schedule agreement number", "schAgreementNumber", ((FieldReference) firstCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be field", firstCriterion.getValue() instanceof FieldReference);
    assertEquals("Field name in where clause should be agreement number", "agreementNumber", ((FieldReference) firstCriterion.getValue()).getName());

    assertNotNull("Table should be specified on equality", ((FieldReference) firstCriterion.getField()).getTable());
    assertEquals("Table name of first criterion should be schedule", "schedule", ((FieldReference) firstCriterion.getField()).getTable().getName());

    FieldReference rhs = (FieldReference) firstCriterion.getValue();

    assertNotNull("Table should be specified for RHS of equality", rhs.getTable());
    assertEquals("Table name of RHS should be receivable", "receivable", rhs.getTable().getName());

    assertNotNull("Should have a criterion", secondCriterion);
    assertEquals("Should be an equals operator on join", Operator.EQ, secondCriterion.getOperator());

    assertNotNull("Field name should be set", secondCriterion.getField());
    assertEquals("Field should be schedule number", "scheduleNumber", ((FieldReference) secondCriterion.getField()).getName());
    assertTrue("Class of object in where clause should be field", secondCriterion.getValue() instanceof FieldReference);
    assertEquals("Field name in where clause should be schedule number", "scheduleNumber", ((FieldReference) secondCriterion.getValue()).getName());

    assertNotNull("Table should be specified on equality", ((FieldReference) secondCriterion.getField()).getTable());
    assertEquals("Table name of second criterion should be schedule", "schedule", ((FieldReference) secondCriterion.getField()).getTable().getName());

    rhs = (FieldReference) secondCriterion.getValue();

    assertNotNull("Table should be specified for RHS of equality", rhs.getTable());
    assertEquals("Table name of RHS should be receivable", "receivable", rhs.getTable().getName());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());
  }


  /**
   * Tests a simple select with a having clause
   */
  @Test
  public void testSimpleSelectWithHavingClause() {
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber"))
                                              .from(new TableReference("schedule"))
                                              .groupBy(new FieldReference("agreementNumber"))
                                              .having(Criterion.eq(new FieldReference("blah"), "X"));

    assertEquals("Should have one group by clause", 1, stmt.getGroupBys().size());
    assertNotNull("Should have one having clause", stmt.getHaving());

    FieldReference groupByField = (FieldReference) stmt.getGroupBys().get(0);

    assertNotNull("Group by field should be set", groupByField);
    assertEquals("Group by field should be agreement number", "agreementNumber", groupByField.getName());

    Criterion havingCriterion = stmt.getHaving();

    assertNotNull("Having criterion should not be null", havingCriterion);
    assertNotNull("Having field should not be null", havingCriterion.getField());
    assertEquals("Having field should be blah", "blah", ((FieldReference) havingCriterion.getField()).getName());
    assertEquals("Having value should be X", "X", havingCriterion.getValue());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no order by clause", 0, stmt.getOrderBys().size());

  }


  /**
   * Tests a simple select with an "order by" clause
   */
  @Test
  public void testSimpleSelectWithOrderByClause() {
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber"))
                                              .from(new TableReference("schedule"))
                                              .orderBy(new FieldReference("agreementNumber"));

    assertEquals("Should have one order by clause", 1, stmt.getOrderBys().size());

    FieldReference orderByField = (FieldReference) stmt.getOrderBys().get(0);

    assertNotNull("Order by field should be set", orderByField);
    assertEquals("Order by field should be agreement number", "agreementNumber", orderByField.getName());
    assertEquals("Order by direction should be ascending", Direction.ASCENDING, orderByField.getDirection());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
  }


  /**
   * Tests a simple select with direction on the order by clause
   */
  @Test
  public void testSimpleSelectWithOrderByDescendingClause() {
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber"))
                                              .from(new TableReference("schedule"))
                                              .orderBy(new FieldReference("agreementNumber", Direction.DESCENDING));

    assertEquals("Should have one order by clause", 1, stmt.getOrderBys().size());

    FieldReference orderByField = (FieldReference) stmt.getOrderBys().get(0);

    assertNotNull("Order by field should be set", orderByField);
    assertEquals("Order by field should be agreement number", "agreementNumber", orderByField.getName());
    assertEquals("Order by direction should be descending", Direction.DESCENDING, orderByField.getDirection());

    // Check that no side-effects have occurred
    assertNull("Should have no where clause", stmt.getWhereCriterion());
    assertEquals("Should have no group by clause", 0, stmt.getGroupBys().size());
    assertNull("Should have no having clause", stmt.getHaving());
  }


  /**
   * Tests where clauses
   */
  @Test
  public void testVariousWhereCriteriaClauses() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement"))
                                                .where(Criterion.lessThan(new FieldReference("startDate"), new Integer(20090101)));

    Criterion whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.LT, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof Integer);
    assertEquals("Where criterion value should be set", 20090101, ((Integer)whereCriterion.getValue()).intValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.lessThanOrEqualTo(new FieldReference("startDate"), new Integer(20090101)));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.LTE, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof Integer);
    assertEquals("Where criterion value should be set", 20090101, ((Integer)whereCriterion.getValue()).intValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.like(new FieldReference("agreementNumber"), "A%"));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.LIKE, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "agreementNumber", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof String);
    assertEquals("Where criterion value should be set", "A%", whereCriterion.getValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.neq(new FieldReference("startDate"), new Integer(20090101)));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.NEQ, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof Integer);
    assertEquals("Where criterion value should be set", 20090101, ((Integer)whereCriterion.getValue()).intValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.greaterThan(new FieldReference("startDate"), new Integer(20090101)));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.GT, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof Integer);
    assertEquals("Where criterion value should be set", 20090101, ((Integer)whereCriterion.getValue()).intValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.greaterThanOrEqualTo(new FieldReference("startDate"), new Integer(20090101)));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.GTE, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());
    assertTrue("Where criterion value should be integer", whereCriterion.getValue() instanceof Integer);
    assertEquals("Where criterion value should be set", 20090101, ((Integer)whereCriterion.getValue()).intValue());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.isNull(new FieldReference("startDate")));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.ISNULL, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());

    stmt = new SelectStatement().from(new TableReference("agreement"))
                                .where(Criterion.isNotNull(new FieldReference("startDate")));

    whereCriterion = stmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.ISNOTNULL, whereCriterion.getOperator());
    assertNotNull("Where criterion field should be set", whereCriterion.getField());
    assertEquals("Where criterion field should be startDate", "startDate", ((FieldReference) whereCriterion.getField()).getName());

    SelectStatement existsStmt = new SelectStatement().from(new TableReference("receivable"))
                                                      .where(Criterion.exists(stmt));

    whereCriterion = existsStmt.getWhereCriterion();
    assertNotNull("Where criterion should not be null", whereCriterion);
    assertEquals("Where criterion should be less than", Operator.EXISTS, whereCriterion.getOperator());
    assertNotNull("Where criterion statement should be set", whereCriterion.getSelectStatement());
    assertEquals("Where criterion statement should be correct", stmt, whereCriterion.getSelectStatement());
  }


  /**
   * Tests use of aliases in selects
   */
  @Test
  public void testTableAliasInSelect() {
    SelectStatement stmt = new SelectStatement().from(new TableReference("agreement").as("my_agreement"));

    assertNotNull("Table should be set", stmt.getTable());
    assertNotNull("Table should have alias set", stmt.getTable().getAlias());
    assertEquals("Table should have correct alias", "my_agreement", stmt.getTable().getAlias());
  }


  /**
   * Tests the operator negation (opposite) method
   */
  @Test
  public void testOppsiteOperator() {
    assertEquals(Operator.LT, Operator.opposite(Operator.GTE));
    assertEquals(Operator.GT, Operator.opposite(Operator.LTE));
    assertEquals(Operator.LTE, Operator.opposite(Operator.GT));
    assertEquals(Operator.GTE, Operator.opposite(Operator.LT));
    assertEquals(Operator.EQ, Operator.opposite(Operator.NEQ));
    assertEquals(Operator.NEQ, Operator.opposite(Operator.EQ));
    assertEquals(Operator.ISNULL, Operator.opposite(Operator.ISNOTNULL));
    assertEquals(Operator.ISNOTNULL, Operator.opposite(Operator.ISNULL));

    try {
      Operator.opposite(Operator.OR);

      fail("Should not be able to find the opposite of OR");
    } catch (UnsupportedOperationException e) {
      // OK
    }

    try {
      Operator.opposite(Operator.AND);

      fail("Should not be able to find the opposite of AND");
    } catch (UnsupportedOperationException e) {
      // OK
    }
  }


  /**
   * Tests a null in the where clause
   */
  @Test
  public void testNullCriterionWhereClause() {
    try {
      new SelectStatement().from(new TableReference("agreement"))
                                          .where((Criterion)null);

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }


  /**
   * Tests a null in the where clause
   */
  @Test
  public void testNullCriterionWhereClause1() {
    try {
      new SelectStatement().from(new TableReference("agreement"))
                                          .where((List<Criterion>)null);

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }




  /**
   * Tests an empty iterable in the where clause
   */
  @Test
  public void testNoCriterionsWhereClause() {
      SelectStatement statement = new SelectStatement().from(new TableReference("agreement")).where(ImmutableSet.<Criterion>of());
      assertNull(statement.getWhereCriterion());
  }


  /**
   * Tests a null in the group by clause
   */
  @Test
  public void testNullGroupByClause() {
    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .groupBy(null);

      fail("Should have raised an exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }


  /**
   * Tests a null in the having clause
   */
  @Test
  public void testNullHavingClause() {
    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .groupBy(new FieldReference("agreementNumber"))
                                    .having(null);

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }


  /**
   * Tests a null in the order by clause
   */
  @Test
  public void testNullOrderByClause() {
    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .orderBy((AliasedField[]) null);

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }


  /**
   * Tests a null in the criteria clause
   */
  @Test
  public void testNullCriteriaClause() {
    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .where(Criterion.and(null));

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }

    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .where(Criterion.eq(null, null));

      fail("Should have raised a null pointer exception");
    } catch (IllegalArgumentException e) {
      // No action
    }
  }


  /**
   * Tests multiple having clauses
   */
  @Test
  public void testMultipleHavingClauses() {
    try {
      new SelectStatement(new FieldReference("agreementNumber"))
                                    .from(new TableReference("schedule"))
                                    .groupBy(new FieldReference("agreementNumber"))
                                    .having(Criterion.eq(new FieldReference("blah"), "X"))
                                    .having(Criterion.greaterThan(new FieldReference("foo"), "A"));

      fail("Should have raised an unsupported operation exception");
    } catch (UnsupportedOperationException e) {
      // No action
    }
  }

  /**
   * Test that deep copy works as expected.
   */
  @Test
  public void testDeepCopy(){
    SelectStatement stmt = new SelectStatement(new FieldReference("agreementNumber")).from(new TableReference("schedule"))
      .innerJoin(new TableReference("schedule"), Criterion.eq(new FieldReference(new TableReference("agreement"), "agreementNumber"), new FieldReference(new TableReference("schedule"), "schAgreementNumber")))
      .leftOuterJoin(new TableReference("receivable"), Criterion.eq(new FieldReference(new TableReference("schedule"), "schAgreementNumber"), new FieldReference( new TableReference("receivable"), "agreementNumber")))
      .groupBy(new FieldReference("agreementNumber"))
      .having(Criterion.eq(new FieldReference("blah"), "X"));

    SelectStatement stmtCopy = stmt.deepCopy();

    assertTrue("Should be different instance of field", stmt.getFields().get(0) != stmtCopy.getFields().get(0));
    assertEquals("Field should match", ((FieldReference) stmt.getFields().get(0)).getName(), ((FieldReference) stmtCopy.getFields().get(0)).getName());

    for(int i = 0; i < stmt.getJoins().size(); i++){
      assertTrue("Should be different instance of Join", stmt.getJoins().get(i) != stmtCopy.getJoins().get(i));
      assertEquals("Join table name should match", stmt.getJoins().get(i).getTable().getName(), stmtCopy.getJoins().get(i).getTable().getName());
      assertEquals("Join operator should match", stmt.getJoins().get(i).getCriterion().getOperator(), stmtCopy.getJoins().get(i).getCriterion().getOperator());
      assertEquals("Table name shoudl match", ((FieldReference) stmt.getJoins().get(i).getCriterion().getField()).getTable().getName(), ((FieldReference) stmtCopy.getJoins().get(i).getCriterion().getField()).getTable().getName());
      assertEquals("Field name should match", ((FieldReference) stmt.getJoins().get(i).getCriterion().getField()).getName(), ((FieldReference) stmtCopy.getJoins().get(i).getCriterion().getField()).getName());
      assertEquals("Value shoudl match", ((FieldReference) stmt.getJoins().get(i).getCriterion().getValue()).getName(), ((FieldReference) stmtCopy.getJoins().get(i).getCriterion().getValue()).getName());
    }

    assertTrue("Should be different instance of group by", stmt.getGroupBys().get(0) != stmtCopy.getGroupBys().get(0));
    assertEquals("Field name for group by should match", ((FieldReference) stmt.getGroupBys().get(0)).getName(), ((FieldReference) stmtCopy.getGroupBys().get(0)).getName());

    assertTrue("Should be different instance of having", stmt.getHaving() != stmtCopy.getHaving());
    assertEquals("Field name for having should match", ((FieldReference) stmt.getHaving().getField()).getName(), ((FieldReference) stmtCopy.getHaving().getField()).getName());

  }

}
