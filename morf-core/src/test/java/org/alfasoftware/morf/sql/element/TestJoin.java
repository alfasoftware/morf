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

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.alfasoftware.morf.sql.SelectStatement;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link Join}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestJoin extends AbstractDeepCopyableTest<Join> {

  public static final TableReference TABLE_1 = mockOf(TableReference.class);
  public static final TableReference TABLE_2 = mockOf(TableReference.class);
  public static final Criterion CRITERION_1 = mockOf(Criterion.class);
  public static final Criterion CRITERION_2 = mockOf(Criterion.class);
  public static final SelectStatement SELECT_1 = mockSelectStatement();
  public static final SelectStatement SELECT_2 = mockSelectStatement();

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(
      testCase("inner table 1", () -> new Join(JoinType.INNER_JOIN, TABLE_1, CRITERION_1)),
      testCase("inner table 2", () -> new Join(JoinType.INNER_JOIN, TABLE_1, CRITERION_2)),
      testCase("inner table 3", () -> new Join(JoinType.INNER_JOIN, TABLE_2, CRITERION_1)),
      testCase("left table 1", () -> new Join(JoinType.LEFT_OUTER_JOIN, TABLE_1, CRITERION_1)),
      testCase("left table 2", () -> new Join(JoinType.LEFT_OUTER_JOIN, TABLE_1, CRITERION_2)),
      testCase("left table 3", () -> new Join(JoinType.LEFT_OUTER_JOIN, TABLE_2, CRITERION_1)),
      testCase("inner select 1", () -> new Join(JoinType.INNER_JOIN, SELECT_1, CRITERION_1)),
      testCase("inner select 2", () -> new Join(JoinType.INNER_JOIN, SELECT_1, CRITERION_2)),
      testCase("inner select 3", () -> new Join(JoinType.INNER_JOIN, SELECT_2, CRITERION_1)),
      testCase("left select 1", () -> new Join(JoinType.LEFT_OUTER_JOIN, SELECT_1, CRITERION_1)),
      testCase("left select 2", () -> new Join(JoinType.LEFT_OUTER_JOIN, SELECT_1, CRITERION_2)),
      testCase("left select 3", () -> new Join(JoinType.LEFT_OUTER_JOIN, SELECT_2, CRITERION_1))
    );
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    Join join = new Join(JoinType.INNER_JOIN, tableRef("table1"), CRITERION_1);
    ResolvedTables res = new ResolvedTables();

    //when
    join.resolveTables(res);

    //then
    verify(CRITERION_1).resolveTables(res);
    assertThat(res.getReadTables(), Matchers.contains("TABLE1"));
  }


  @Test
  public void tableResolutionDetectsAllTables2() {
    //given
    Join join = new Join(JoinType.INNER_JOIN, SELECT_1, CRITERION_1);
    ResolvedTables res = new ResolvedTables();

    //when
    join.resolveTables(res);

    //then
    verify(CRITERION_1).resolveTables(res);
    verify(SELECT_1).resolveTables(res);
  }
}