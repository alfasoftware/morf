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
import static org.alfasoftware.morf.sql.SqlUtils.windowFunction;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the WindowFunction DSL
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestWindowFunctions extends AbstractAliasedFieldTest<WindowFunction> {

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Collections.singletonList(
      testCase(
        "Full",
        () -> windowFunction(sum(literal(1))).partitionBy(literal(2)).orderBy(literal(3)).build(),
        () -> windowFunction(sum(literal(1))).partitionBy(literal(2)).orderBy(literal(4)).build(),
        () -> windowFunction(sum(literal(1))).partitionBy(literal(3)).orderBy(literal(3)).build(),
        () -> windowFunction(sum(literal(2))).partitionBy(literal(2)).orderBy(literal(3)).build(),
        () -> windowFunction(sum(literal(1))).orderBy(literal(3)).build(),
        () -> windowFunction(sum(literal(1))).partitionBy(literal(2)).build()
      )
    );
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    AliasedField field3 = mock(AliasedField.class);
    Function func = Function.count(field3);
    AliasedField field = mock(AliasedField.class);
    AliasedField field2 = mock(AliasedField.class);
    WindowFunction onTest = WindowFunction.over(func).orderBy(field).partitionBy(field2).build();
    ResolvedTables res = new ResolvedTables();

    //when
    onTest.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(field2).resolveTables(res);
    verify(field3).resolveTables(res);
  }
}