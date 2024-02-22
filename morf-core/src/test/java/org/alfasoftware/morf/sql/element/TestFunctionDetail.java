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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * Tests for Function
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestFunctionDetail {

  /**
   * Verify that deep copy works as expected for functions with arguments.
   */
  @Test
  public void testDeepCopyOneArg() {
    Function func = Function.max(new FieldReference("startDate"));
    func.as("testName");
    Function funcCopy = (Function)func.deepCopy();

    assertEquals("Function type matches", func.getType(), funcCopy.getType());
    assertEquals("Function type matches", func.getAlias(), funcCopy.getAlias());

    assertNotNull("Copy should have arguments", funcCopy.getArguments());
    assertEquals("Copy should have right number of arguments", 1, funcCopy.getArguments().size());
    assertEquals("Fields in copy should be the same", func.getArguments().get(0).getAlias(), funcCopy.getArguments().get(0).getAlias());
  }


  /**
   * Tests the indirect usage of the maximum function
   */
  @Test
  public void testMax() {
    Function func = Function.max(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type MAX", FunctionType.MAX, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have two arguments", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests the indirect usage of the maximum function
   */
  @Test
  public void testMin() {
    Function func = Function.min(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type MIN", FunctionType.MIN, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have two arguments", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests the indirect usage of the sum function
   */
  @Test
  public void testSum() {
    Function func = Function.sum(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type SUM", FunctionType.SUM, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have one argument", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests the indirect usage of the length function
   */
  @Test
  public void testLength() {
    Function func = Function.length(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type LENGTH", FunctionType.LENGTH, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have one argument", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests the indirect usage of the length-of-blob function
   */
  @Test
  public void testBlobLength() {
    Function func = Function.blobLength(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type BLOB_LENGTH", FunctionType.BLOB_LENGTH, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have one argument", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests indirect usage of the substring function
   */
  @Test
  public void testSubstring() {
    Function func = Function.substring(new FieldReference("agreementNumber"), new FieldLiteral(2), new FieldLiteral(3));

    assertEquals("Function should be of type SUBSTRING", FunctionType.SUBSTRING, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have three arguments", 3, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    AliasedField secondArgument = func.getArguments().get(1);
    AliasedField thirdArgument = func.getArguments().get(2);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
    assertTrue("Second argument should be a field literal", secondArgument instanceof FieldLiteral);
    assertEquals("Second argument should have correct value", "2", ((FieldLiteral)secondArgument).getValue());
    assertTrue("Third argument should be a field literal", thirdArgument instanceof FieldLiteral);
    assertEquals("Third argument should have correct value", "3", ((FieldLiteral)thirdArgument).getValue());
  }

  /**
   * Tests indirect usage of the YYYYMMDDToDate function
   */
  @Test
  public void testYYYYMMDDToDate() {
    Function func = Function.yyyymmddToDate(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type YYYYMMDDToDate", FunctionType.YYYYMMDD_TO_DATE, func.getType());
    assertNotNull("Function should have arguments", func.getArguments());
    assertEquals("Function should have one argument", 1, func.getArguments().size());
    AliasedField firstArgument = func.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests indirect usage of the trim function
   */
  @Test
  public void testTrim() {
    Function function = Function.trim(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type Trim", FunctionType.TRIM, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have one argument", 1, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests indirect usage of the leftTrim function
   */
  @Test
  public void testLeftTrim() {
    Function function = Function.leftTrim(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type Left Trim", FunctionType.LEFT_TRIM, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have one argument", 1, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests indirect usage of the rightTrim function
   */
  @Test
  public void testRightTrim() {
    Function function = Function.rightTrim(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type Right Trim", FunctionType.RIGHT_TRIM, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have one argument", 1, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference)firstArgument).getName());
  }


  /**
   * Tests the indirect usage of leftPad
   */
  @Test
  public void testLeftPad() {
    Function function = Function.leftPad(new FieldReference("invoiceNumber"), new FieldLiteral(10), new FieldLiteral('j'));

    assertEquals("Function must be of type LEFT_PAD", FunctionType.LEFT_PAD, function.getType());
    assertEquals("Function should have 3 arguments", 3, function.getArguments().size());
  }


  /**
   * Tests the indirect usage of leftPad
   */
  @Test
  public void testLeftPadConvenientMethod() {
    Function function = Function.leftPad(new FieldReference("invoiceNumber"), 10, "j");

    assertEquals("Function must be of type LEFT_PAD", FunctionType.LEFT_PAD, function.getType());
    assertEquals("Function should have 3 arguments", 3, function.getArguments().size());
  }


  /**
   * Tests the indirect usage of rightPad
   */
  @Test
  public void testRightPad() {
    Function function = Function.rightPad(new FieldReference("invoiceNumber"), new FieldLiteral(10), new FieldLiteral('j'));

    assertEquals("Function must be of type RIGHT_PAD", FunctionType.RIGHT_PAD, function.getType());
    assertEquals("Function should have 3 arguments", 3, function.getArguments().size());
  }


  /**
   * Tests the indirect usage of rightPad
   */
  @Test
  public void testRightPadConvenientMethod() {
    Function function = Function.rightPad(new FieldReference("invoiceNumber"), 10, "j");

    assertEquals("Function must be of type RIGHT_PAD", FunctionType.RIGHT_PAD, function.getType());
    assertEquals("Function should have 3 arguments", 3, function.getArguments().size());
  }


  /**
   * Tests indirect usage of the mod function
   */
  @Test
  public void testMod() {
    Function function = Function.mod(new FieldReference("scheduleNumber"), new FieldLiteral(2));

    assertEquals("Function should be of type MOD", FunctionType.MOD, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have two argument", 2, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "scheduleNumber", ((FieldReference)firstArgument).getName());
    AliasedField secondArgument = function.getArguments().get(1);
    assertTrue("Second argument should be a field literal", secondArgument instanceof FieldLiteral);
    assertEquals("Second argument should have correct value", "2", ((FieldLiteral)secondArgument).getValue());
  }


  /**
   * Tests indirect usage of the <code>lowerCase</code> function
   */
  @Test
  public void testLowerCase() {
    Function function = Function.lowerCase(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type LOWER", FunctionType.LOWER, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have one argument", 1, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference) firstArgument).getName());
  }


  /**
   * Tests indirect usage of the <code>upperCase</code> function
   */
  @Test
  public void testUpperCase() {
    Function function = Function.upperCase(new FieldReference("agreementNumber"));

    assertEquals("Function should be of type UPPER", FunctionType.UPPER, function.getType());
    assertNotNull("Function should have arguments", function.getArguments());
    assertEquals("Function should have one argument", 1, function.getArguments().size());
    AliasedField firstArgument = function.getArguments().get(0);
    assertTrue("First argument should be a field reference", firstArgument instanceof FieldReference);
    assertEquals("First argument should have correct name", "agreementNumber", ((FieldReference) firstArgument).getName());
  }
}