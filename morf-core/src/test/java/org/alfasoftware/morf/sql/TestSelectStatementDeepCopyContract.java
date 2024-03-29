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

import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.AliasedFieldBuilder;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Checks that {@link SelectStatement} satisfies equals, hashcode, deep copy, shallow copy and driver contracts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestSelectStatementDeepCopyContract extends AbstractShallowAndDeepCopyableTest<SelectStatement> {

  private static final SelectStatement SELECT_1 = mockSelectStatement();
  private static final SelectStatement SELECT_2 = mockSelectStatement();
  private static final Criterion CRITERION_1 = mockOf(Criterion.class);
  private static final Criterion CRITERION_2 = mockOf(Criterion.class);
  private static final TableReference TABLE_1 = mockOf(TableReference.class);
  private static final TableReference TABLE_2 = mockOf(TableReference.class);


  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(
      testCase("select()", () -> select()),
      testCase("select().distinct()", () -> new SelectStatement(new AliasedFieldBuilder[] {}, true)),
      testCase("select().alias(\"A\")", () -> select().alias("A")),
      testCase("select().forUpdate()", () -> select().forUpdate()),
      testCase("select().useImplicitJoinOrder()", () -> select().useImplicitJoinOrder()),
      testCase("select().optimiseForRowCount(1)", () -> select().optimiseForRowCount(1)),
      testCase("select().optimiseForRowCount(2)", () -> select().optimiseForRowCount(2)),
      testCase("select().useIndex(TABLE_1, \"A\")", () -> select().useIndex(TABLE_1, "A")),
      testCase("select().useIndex(TABLE_1, \"B\")", () -> select().useIndex(TABLE_1, "B")),
      testCase("select().useIndex(TABLE_2, \"A\")", () -> select().useIndex(TABLE_2, "A")),
      testCase("select(literal(1))", () -> select(literal(1))),
      testCase("select(literal(2))", () -> select(literal(2))),
      testCase("select(literal(1), literal(2))", () -> select(literal(1), literal(2))),
      testCase("select(literal(1), literal(3))", () -> select(literal(1), literal(3))),
      testCase("select().from(\"A\")", () -> select().from("A")),
      testCase("select().from(\"B\")", () -> select().from("B")),
      testCase("select().from(SELECT_1)", () -> select().from(SELECT_1)),
      testCase("select().from(SELECT_2)", () -> select().from(SELECT_2)),
      testCase("select().from(SELECT_1, SELECT_1)", () -> select().from(SELECT_1, SELECT_1)),
      testCase("select().from(SELECT_1, SELECT_2)", () -> select().from(SELECT_1, SELECT_2)),
      testCase("select().from(\"A\").where(CRITERION_1)", () -> select().from("A").where(CRITERION_1)),
      testCase("select().from(\"A\").where(CRITERION_2)", () -> select().from("A").where(CRITERION_2)),
      testCase("select().from(\"A\").innerJoin(TABLE_1)", () -> select().from("A").crossJoin(TABLE_1)),
      testCase("select().from(\"A\").innerJoin(TABLE_2)", () -> select().from("A").crossJoin(TABLE_2)),
      testCase("select().from(\"A\").innerJoin(TABLE_1).innerJoin(TABLE_1).innerJoin(TABLE_1)", () -> select().from("A").crossJoin(TABLE_1).crossJoin(TABLE_1).crossJoin(TABLE_1)),
      testCase("select().from(\"A\").innerJoin(TABLE_1).innerJoin(TABLE_2)", () -> select().from("A").crossJoin(TABLE_1).crossJoin(TABLE_2)),
      testCase("select().from(\"A\").innerJoin(TABLE_1, CRITERION_1).innerJoin(TABLE_2, CRITERION_1)", () -> select().from("A").innerJoin(TABLE_1, CRITERION_1).innerJoin(TABLE_2, CRITERION_1)),
      testCase("select().from(\"A\").leftOuterJoin(TABLE_1, CRITERION_1)", () -> select().from("A").leftOuterJoin(TABLE_1, CRITERION_1)),
      testCase("select().from(\"A\").leftOuterJoin(TABLE_1, CRITERION_2)", () -> select().from("A").leftOuterJoin(TABLE_1, CRITERION_2)),
      testCase("select().from(\"A\").leftOuterJoin(TABLE_2, CRITERION_1)", () -> select().from("A").leftOuterJoin(TABLE_2, CRITERION_1)),
      testCase("select().from(\"A\").fullOuterJoin(TABLE_1, CRITERION_1)", () -> select().from("A").fullOuterJoin(TABLE_1, CRITERION_1)),
      testCase("select().from(\"A\").fullOuterJoin(TABLE_1, CRITERION_2)", () -> select().from("A").fullOuterJoin(TABLE_1, CRITERION_2)),
      testCase("select().orderBy(literal(1), literal(2))", () -> select().orderBy(literal(1), literal(2))),
      testCase("select().orderBy(literal(1), literal(3))", () -> select().orderBy(literal(1), literal(3))),
      testCase("select().having(CRITERION_1)", () -> select().having(CRITERION_1)),
      testCase("select().having(CRITERION_2)", () -> select().having(CRITERION_2)),
      testCase("select().union(SELECT_1)", () -> select().union(SELECT_1)),
      testCase("select().union(SELECT_2)", () -> select().union(SELECT_2)),
      testCase("select().except(SELECT_1)", () -> select().except(SELECT_1)),
      testCase("select().except(SELECT_2)", () -> select().except(SELECT_2)),
      testCase("select().unionAll(SELECT_1)", () -> select().unionAll(SELECT_1)),
      testCase("select().unionAll(SELECT_2)", () -> select().unionAll(SELECT_2))
    );
  }

}