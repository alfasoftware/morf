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

import static org.alfasoftware.morf.sql.element.FieldLiteral.literal;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.List;

import org.joda.time.LocalDate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Tests for field literals
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@RunWith(Parameterized.class)
public class TestFieldLiteral extends AbstractAliasedFieldTest<FieldLiteral> {

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return ImmutableList.of(
      testCase(
          "BigDecimal",
          () -> literal(new BigDecimal("1234567890123456789.0123456789")),
          () -> literal(BigDecimal.ZERO),
          () -> literal(true)
      ),
      testCase(
          "String",
          () -> literal("1"),
          () -> literal("0"),
          () -> literal(true),
          () -> literal(1)
      ),
      testCase(
          "Boolean",
          () -> literal(true),
          () -> literal(false),
          () -> literal(1)
      ),
      testCase(
          "Character",
          () -> literal('a'),
          () -> literal('b'),
          () -> literal(1)
      ),
      testCase(
          "Double",
          () -> literal(1.23D),
          () -> literal(1)
      ),
      testCase(
          "Long",
          () -> literal(1L),
          () -> literal('b')
      ),
      testCase(
          "LocalDate",
          () -> literal(new LocalDate(2010,1,2)),
          () -> literal(new LocalDate(2010,1,1)),
          () -> literal('b')
      ),
      testCase(
          "Integer",
          () -> literal(1),
          () -> literal(2),
          () -> literal('b')
      )
    );
  }


  /**
   * Verify that deep copy actually copies the individual properties.
   */
  @Test
  public void testDeepCopyDetail() {
    FieldLiteral f1 = (FieldLiteral) onTestAliased;
    FieldLiteral flCopy = (FieldLiteral)onTestAliased.deepCopy();

    assertEquals("Field literal value matches", f1.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", f1.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", f1.getAlias(), flCopy.getAlias());
  }
}
