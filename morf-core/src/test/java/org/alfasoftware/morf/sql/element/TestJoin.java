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

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.SelectStatement;
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

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    TableReference table1 = mockOf(TableReference.class);
    TableReference table2 = mockOf(TableReference.class);
    Criterion criterion1 = mockOf(Criterion.class);
    Criterion criterion2 = mockOf(Criterion.class);
    SelectStatement select1 = mockOf(SelectStatement.class);
    SelectStatement select2 = mockOf(SelectStatement.class);

    return Arrays.asList(
      testCase("inner table 1", () -> new Join(JoinType.INNER_JOIN, table1, criterion1)),
      testCase("inner table 2", () -> new Join(JoinType.INNER_JOIN, table1, criterion2)),
      testCase("inner table 3", () -> new Join(JoinType.INNER_JOIN, table2, criterion1)),
      testCase("left table 1", () -> new Join(JoinType.LEFT_OUTER_JOIN, table1, criterion1)),
      testCase("left table 2", () -> new Join(JoinType.LEFT_OUTER_JOIN, table1, criterion2)),
      testCase("left table 3", () -> new Join(JoinType.LEFT_OUTER_JOIN, table2, criterion1)),
      testCase("inner select 1", () -> new Join(JoinType.INNER_JOIN, select1, criterion1)),
      testCase("inner select 2", () -> new Join(JoinType.INNER_JOIN, select1, criterion2)),
      testCase("inner select 3", () -> new Join(JoinType.INNER_JOIN, select2, criterion1)),
      testCase("left select 1", () -> new Join(JoinType.LEFT_OUTER_JOIN, select1, criterion1)),
      testCase("left select 2", () -> new Join(JoinType.LEFT_OUTER_JOIN, select1, criterion2)),
      testCase("left select 3", () -> new Join(JoinType.LEFT_OUTER_JOIN, select2, criterion1))
    );
  }
}