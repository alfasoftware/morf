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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link WhenCondition}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestWhenCondition extends AbstractDeepCopyableTest<WhenCondition> {

  public static final Criterion CRITERION_1 = mockOf(Criterion.class);
  public static final Criterion CRITERION_2 = mockOf(Criterion.class);

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(
      testCase("1", () -> new WhenCondition(CRITERION_1, literal(1))),
      testCase("2", () -> new WhenCondition(CRITERION_1, literal(2))),
      testCase("3", () -> new WhenCondition(CRITERION_2, literal(1)))
    );
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    AliasedField field = mock(AliasedField.class);
    WhenCondition when = new WhenCondition(CRITERION_1, field);
    ResolvedTables res = new ResolvedTables();

    //when
    when.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(CRITERION_1).resolveTables(res);
  }
}