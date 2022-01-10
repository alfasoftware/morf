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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Tests for field from select.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@RunWith(Parameterized.class)
public class TestFieldFromSelect extends AbstractAliasedFieldTest<FieldFromSelect> {

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    SelectStatement stmt1 = mockSelectStatement();
    SelectStatement stmt2 = mockSelectStatement();
    return ImmutableList.of(
      testCase(
        "simple",
        () -> new FieldFromSelect(stmt1),
        () -> new FieldFromSelect(stmt2)
      )
    );
  }


  /**
   * Verify that field from select can only have one field in the select
   * statement.
   */
  @Test
  public void testNumberOfFields() {
    SelectStatement statementWithTwoFields = new SelectStatement(new FieldReference("assetCost"), new FieldReference("agreementNumber")).from(
      new TableReference("Schedule")).where(Criterion.eq(new FieldReference(new TableReference("Schedule"), "agreementNumber"), "A001003657"));

    try {
      new FieldFromSelect(statementWithTwoFields);
      fail("FieldFromSelect can not have more than one fields in the select statement");
    } catch (IllegalArgumentException e) {
      // OK to get here
    }
  }


  /**
   * Verify that deep copy works as expected for field from select.
   */
  @Test
  public void testDeepCopyDetail() {
    SelectStatement statementWithTwoFields = new SelectStatement(new FieldReference("agreementNumber")).from(new TableReference("Schedule"))
        .where(Criterion.eq(new FieldReference(new TableReference("Schedule"), "agreementNumber"), "A001003657"));

    FieldFromSelect ffs = new FieldFromSelect(statementWithTwoFields);
    ffs.as("field_alias");
    FieldFromSelect ffsCopy = (FieldFromSelect)ffs.deepCopy();

    assertTrue("Should be different instances of SelectStatement", ffs.getSelectStatement() != ffsCopy.getSelectStatement());
    assertEquals("Field names should match", ((FieldReference)ffs.getSelectStatement().getFields().get(0)).getName(), ((FieldReference)ffsCopy.getSelectStatement().getFields().get(0)).getName());
    assertEquals("Table names should match", ffs.getSelectStatement().getTable().getName(), ffsCopy.getSelectStatement().getTable().getName());
    assertEquals("Operators should match", ffs.getSelectStatement().getWhereCriterion().getOperator(), ffsCopy.getSelectStatement().getWhereCriterion().getOperator());
    assertEquals("Criterion field names should match", ((FieldReference)ffs.getSelectStatement().getWhereCriterion().getField()).getName(), ((FieldReference)ffsCopy.getSelectStatement().getWhereCriterion().getField()).getName());
    assertEquals("Criterion values should match", ((String) ffs.getSelectStatement().getWhereCriterion().getValue()).toUpperCase(), ((String) ffsCopy.getSelectStatement().getWhereCriterion().getValue()).toUpperCase());
    assertEquals("Alias", ffs.getAlias(), ffsCopy.getAlias());
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    SelectStatement selectStatement = mock(SelectStatement.class);
    FieldFromSelect onTest = new FieldFromSelect(selectStatement);
    ResolvedTables res = new ResolvedTables();

    //when
    onTest.resolveTables(res);

    //then
    verify(selectStatement).resolveTables(res);
  }
}
