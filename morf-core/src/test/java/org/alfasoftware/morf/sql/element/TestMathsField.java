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

import java.util.List;

import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableList;

/**
 * Tests {@link MathsField}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
@RunWith(Parameterized.class)
public class TestMathsField extends AbstractAliasedFieldTest<MathsField> {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return ImmutableList.of(
      testCase(
        "Test 1",
        () -> MathsField.plus(literal(1), literal(2)),
        () -> MathsField.multiply(literal(1), literal(2)),
        () -> MathsField.plus(literal(1), literal(3))
      )
    );
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    AliasedField field = mock(AliasedField.class);
    AliasedField field2 = mock(AliasedField.class);
    MathsField onTest = MathsField.plus(field, field2);

    //when
    onTest.accept(res);

    //then
    verify(res).visit(onTest);
    verify(field).accept(res);
    verify(field2).accept(res);
  }
}