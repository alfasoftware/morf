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

import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link Criterion}'s implementation of deep copy and equals contracts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestCriterionDeepCopyAndEquals extends AbstractDeepCopyableTest<Criterion> {

  private static final Criterion MOCK_1 = mockOf(Criterion.class);
  private static final Criterion MOCK_2 = mockOf(Criterion.class);
  private static final Criterion MOCK_3 = mockOf(Criterion.class);
  private static final SelectStatement SELECT_1 = mockSelectStatement();
  private static final SelectStatement SELECT_2 = mockSelectStatement();

  static {
    when(SELECT_1.getFields()).thenReturn(ImmutableList.of(literal('A')));
    when(SELECT_2.getFields()).thenReturn(ImmutableList.of(literal('A')));
  }

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(
      testCase("eq 1", () -> Criterion.eq(literal(1), literal(2))),
      testCase("eq 2", () -> Criterion.eq(literal(1), literal(3))),
      testCase("and 1", () -> Criterion.and(MOCK_1, MOCK_2)),
      testCase("and 2", () -> Criterion.and(MOCK_1, MOCK_3)),
      testCase("and 3", () -> Criterion.and(MOCK_1, MOCK_2, MOCK_3)),
      testCase("exists 1", () -> Criterion.exists(SELECT_1)),
      testCase("exists 2", () -> Criterion.exists(SELECT_2)),
      testCase("greaterThanInteger 1", () -> Criterion.greaterThan(literal(1), 1)),
      testCase("greaterThanInteger 2", () -> Criterion.greaterThan(literal(1), 2)),
      testCase("greaterThanLiteral 1", () -> Criterion.greaterThan(literal(1), literal(2))),
      testCase("greaterThanLiteral 2", () -> Criterion.greaterThan(literal(1), literal(3))),
      testCase("isNull 1", () -> Criterion.isNull(literal(1))),
      testCase("isNull 2", () -> Criterion.isNull(literal(2))),
      testCase("inList 1", () -> Criterion.in(literal(1), literal(2), literal(3))),
      testCase("inList 2", () -> Criterion.in(literal(1), literal(2), literal(4))),
      testCase("inSelect 1", () -> Criterion.in(literal(1), SELECT_1)),
      testCase("inSelect 2", () -> Criterion.in(literal(1), SELECT_2))
    );
  }
}