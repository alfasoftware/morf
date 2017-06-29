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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.joda.time.LocalDate;
import org.junit.Test;

/**
 * Tests for field literals
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestFieldLiteral {


  /**
   * Verify that deep copy works as expected for string field literal.
   */
  @Test
  public void testDeepCopyWithString() {
    FieldLiteral fl = new FieldLiteral("TEST1");
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for Boolean field literal.
   */
  @Test
  public void testDeepCopyWithBoolean() {
    FieldLiteral fl = new FieldLiteral(true);
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for Character field literal.
   */
  @Test
  public void testDeepCopywithCharacter() {
    FieldLiteral fl = new FieldLiteral('A');
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for Double field literal.
   */
  @Test
  public void testDeepCopyWithDouble() {
    FieldLiteral fl = new FieldLiteral(1.23d);
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for BigDecimal field literal.
   */
  @Test
  public void testDeepCopyWithBigDecimal() {
    FieldLiteral fl = new FieldLiteral(new BigDecimal("1234567890123456789.0123456789"));
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal value is correct", "1234567890123456789.0123456789", flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for Long field literal.
   */
  @Test
  public void testDeepCopyWithLong() {
    FieldLiteral fl = new FieldLiteral(234234L);
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for {@link LocalDate} field literal.
   */
  @Test
  public void testDeepCopyWithLocalDate() {
    FieldLiteral fl = new FieldLiteral(new LocalDate(2010,1,2));
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for Double field literal.
   */
  @Test
  public void testDeepCopyWithInteger() {
    FieldLiteral fl = new FieldLiteral(1);
    fl.as("testName");
    FieldLiteral flCopy = (FieldLiteral)fl.deepCopy();

    assertEquals("Field literal value matches", fl.getValue(), flCopy.getValue());
    assertEquals("Field literal data type matches", fl.getDataType(), flCopy.getDataType());
    assertEquals("Field literal alias matches", fl.getAlias(), flCopy.getAlias());
  }

  /**
   * Confirms that the implied name returns the alias
   */
  @Test
  public void testImpliedName() {
    FieldLiteral fl = new FieldLiteral(1);
    assertEquals("Field literal implied name correctly initialised", fl.getImpliedName(), "");
    fl.as("testName");
    assertEquals("Field literal implied name matches alias", fl.getImpliedName(), "testName");
  }

}
