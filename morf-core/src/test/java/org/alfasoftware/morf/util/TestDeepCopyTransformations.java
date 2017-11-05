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

package org.alfasoftware.morf.util;

import static org.alfasoftware.morf.sql.SqlUtils.caseStatement;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.merge;
import static org.alfasoftware.morf.sql.SqlUtils.nullLiteral;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.selectDistinct;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.SqlUtils.when;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.like;
import static org.alfasoftware.morf.sql.element.Function.coalesce;
import static org.alfasoftware.morf.sql.element.Function.count;
import static org.alfasoftware.morf.sql.element.Function.max;
import static org.alfasoftware.morf.sql.element.Function.mod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.List;

import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


/**
 * Various deep copy transformation test cases
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestDeepCopyTransformations {

  private static final TableReference tableRefOne = tableRef("table1");
  private static final TableReference tableRefOneCaps = tableRef("TABLE1");

  private static final TableReference tableRefTwo = tableRef("table2");
  private static final TableReference tableRefTwoCaps = tableRef("TABLE2");

  private static final SelectStatement selectStatementOne = select(field("one"), field("two")).from(tableRefTwo).innerJoin(tableRefOne,field("a").eq(field("b")));
  private static final SelectStatement selectStatementOneCapitalised = select(field("one"), field("two")).from(tableRefTwoCaps).innerJoin(tableRefOneCaps,field("a").eq(field("b")));

  private static final MergeStatement mergeStatementOne = merge().from(select().from(tableRefOne).having(and(max(tableRefTwo.field("X")).eq(literal(5)),like(tableRefOne.field("b"), literal(true)))));
  private static final MergeStatement mergeStatementOneCapitalised = merge().from(select().from(tableRefOneCaps).having(and(max(tableRefTwoCaps.field("X")).eq(literal(5)),like(tableRefOneCaps.field("b"), literal(true)))));

  private static final UpdateStatement updateStatement = update(tableRefOne).set(caseStatement(when(tableRefOne.field("1").eq(nullLiteral())).then(tableRefOne.field("b"))).otherwise(literal(false))).where(tableRefOne.field("x").eq(mod(tableRefOne.field("b"),tableRefOne.field("c"))));
  private static final UpdateStatement updateStatementCapitalised = update(tableRefOneCaps).set(caseStatement(when(tableRefOneCaps.field("1").eq(nullLiteral())).then(tableRefOneCaps.field("b"))).otherwise(literal(false))).where(tableRefOneCaps.field("x").eq(mod(tableRefOneCaps.field("b"),tableRefOneCaps.field("c"))));

  private static final InsertStatement insertStatementOne = insert().into(tableRefOne).from(select(caseStatement(when(nullLiteral().eq(tableRefOne.field("T"))).then(tableRefOne.field("T"))).otherwise(5).as("X")).from(tableRefTwo));
  private static final InsertStatement insertStatementOneCapitalised = insert().into(tableRefOneCaps).from(select(caseStatement(when(nullLiteral().eq(tableRefOneCaps.field("T"))).then(tableRefOneCaps.field("T"))).otherwise(5).as("X")).from(tableRefTwoCaps));

  private static final SelectStatement selectStatementTwo = select(
                                                                max(tableRefTwo.field("one").plus(coalesce(tableRefOne.field("three"),tableRefOne.field("four"),literal(5)))),
                                                                field("two"),
                                                                select(count()).from(selectDistinct(tableRefOne.field("four")).from(tableRefOne)).asField().as("c"))
                                                            .from(tableRefTwo)
                                                            .innerJoin(tableRefOne,
                                                              and(
                                                                tableRefOne.field("a").eq(tableRefTwo.field("b")),
                                                                tableRefOne.field("c").greaterThanOrEqualTo(tableRefOne.field("d"))))
                                                            .where(tableRefOne.field("where1").greaterThan(tableRefTwo.field("where2")))
                                                            .groupBy(tableRefOne.field("group1"), tableRefTwo.field("group2"))
                                                            .orderBy(tableRefOne.field("group1"), tableRefTwo.field("group2"));

  private static final SelectStatement selectStatementTwoCapitalised = select(
                                                                max(tableRefTwoCaps.field("one").plus(coalesce(tableRefOneCaps.field("three"),tableRefOneCaps.field("four"),literal(5)))),
                                                                field("two"),
                                                                select(count()).from(selectDistinct(tableRefOneCaps.field("four")).from(tableRefOneCaps)).asField().as("c"))
                                                            .from(tableRefTwoCaps)
                                                            .innerJoin(tableRefOneCaps,
                                                              and(
                                                                tableRefOneCaps.field("a").eq(tableRefTwoCaps.field("b")),
                                                                tableRefOneCaps.field("c").greaterThanOrEqualTo(tableRefOneCaps.field("d"))))
                                                            .where(tableRefOneCaps.field("where1").greaterThan(tableRefTwoCaps.field("where2")))
                                                            .groupBy(tableRefOneCaps.field("group1"), tableRefTwoCaps.field("group2"))
                                                            .orderBy(tableRefOneCaps.field("group1"), tableRefTwoCaps.field("group2"));


  @Parameter(value = 0)
  public DeepCopyTransformation transformation;

  @Parameter(value = 1)
  public DeepCopyableWithTransformation<?,?> statementBefore;

  @Parameter(value = 2)
  public DeepCopyableWithTransformation<?,?> statementAfter;

  @Parameters
  public static List<Object[]> data() {
   return Lists.newArrayList(
     new Object[] {DeepCopyTransformations.noTransformation(),selectStatementOne,selectStatementOne},
     new Object[] {capitaliseTableNames(),selectStatementOne,selectStatementOneCapitalised},
     new Object[] {DeepCopyTransformations.noTransformation(),insertStatementOne,insertStatementOne},
     new Object[] {capitaliseTableNames(),insertStatementOne,insertStatementOneCapitalised},
     new Object[] {DeepCopyTransformations.noTransformation(),updateStatement,updateStatement},
     new Object[] {capitaliseTableNames(),updateStatement,updateStatementCapitalised},
     new Object[] {DeepCopyTransformations.noTransformation(),mergeStatementOne,mergeStatementOne},
     new Object[] {capitaliseTableNames(),mergeStatementOne,mergeStatementOneCapitalised},
     new Object[] {DeepCopyTransformations.noTransformation(),selectStatementTwo,selectStatementTwo},
     new Object[] {capitaliseTableNames(),selectStatementTwoCapitalised,selectStatementTwoCapitalised}
     );
  }


  /**
   * Applies the transformation and verifies that the output matches the expected result.
   */
  @Test
  public void testTransformation() {
    Object copy = statementBefore.deepCopy(transformation).build();

    assertNotSame(copy,statementAfter);
    assertEquals(statementAfter.toString(),copy.toString());
  }



  private static DeepCopyTransformation capitaliseTableNames() {
    return new DeepCopyTransformation() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> T deepCopy(DeepCopyableWithTransformation<T,? extends Builder<T>> element) {

        if(element == null) {
          return null;
        }

        if(element instanceof TableReference) {
          return (T) new TableReference(((TableReference) element).getName().toUpperCase());
        }

        return element.deepCopy(this).build();
      }
    };
  }
}