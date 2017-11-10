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

import static org.alfasoftware.morf.sql.element.BracketedExpression.bracket;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.element.MathsField.plus;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;

import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests {@link BracketedExpression}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
@RunWith(Parameterized.class)
public class TestBracketedExpression extends AbstractAliasedFieldTest<BracketedExpression> {

  public final BracketedExpression onTest = bracket(plus(literal(1), literal(2)));

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Collections.singletonList(
      testCase(
        "Test 1",
        () -> bracket(plus(literal(1), literal(2))),
        () -> bracket(plus(literal(1), literal(3)))
      )
    );
  }


  @Test
  public void testInnerExpression() {
    assertEquals(plus(literal(1), literal(2)), onTest.getInnerExpression());
  }


  @Test
  public void testDrive() {
    ObjectTreeTraverser.Callback callback = mock(ObjectTreeTraverser.Callback.class);
    onTest.drive(ObjectTreeTraverser.forCallback(callback));
    verify(callback).visit(onTest.getInnerExpression());
  }
}