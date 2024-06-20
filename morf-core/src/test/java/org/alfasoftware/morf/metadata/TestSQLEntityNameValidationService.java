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

package org.alfasoftware.morf.metadata;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;


/**
 * <p>
 * Tests for {@link SQLEntityNameValidationService}
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
@RunWith(JUnitParamsRunner.class)
public class TestSQLEntityNameValidationService {

  private SQLEntityNameValidationService sqlEntityNameValidationService;


  @Before
  public void setUp() {
    sqlEntityNameValidationService = new SQLEntityNameValidationService();
  }


  Object[][] getNamePatternTestSubjects() {
    return new Object[][] {
      { "123", false },
      { "_aB1", false },
      { "abc", true },
      { "aB1_", true },
      };
  }

  Object[][] getReservedWordTestSubjects() {
    return new Object[][] {
      { "SELECT", true },
      { "select", true },
      { "FROM", true },
      { "abc", false },
      { "aB1_", false },
    };
  }


  Object[][] getNameLengthTestSubjects() {
    return new Object[][] {
      { "1234567890123456789012345678901234567890123456789012345678901", false },
      { "123456789012345678901234567890123456789012345678901234567890", true }
      };
  }


  /**
   * Tests to ensure that string names are tested correctly to comply with the proper nmaing convention.
   */
  @Test
  @Parameters(method="getNamePatternTestSubjects")
  public void testValidNamePattern(String name, boolean expectedResult) {
    // Given

    // When
    boolean result = sqlEntityNameValidationService.isNameConventional(name);

    // Then
    assertEquals(format("%s should have been valid=%s but was valid=%s", name, expectedResult, result), expectedResult, result);
  }


  /**
   * Tests to ensure that words are tested correctly against the list of reserved words.
   */
  @Test
  @Parameters(method="getReservedWordTestSubjects")
  public void testReservedWord(String name, boolean expectedResult) {
    // Given

    // When
    boolean result = sqlEntityNameValidationService.isReservedWord(name);

    // Then
    assertEquals(format("%s should have been reserved=%s but was reserved=%s", name, expectedResult, result), expectedResult, result);
  }


  /**
   * Tests to ensure that string names are tested correctly to comply with the maxiumum length.
   */
  @Test
  @Parameters(method="getNameLengthTestSubjects")
  public void testValidNameLength(String name, boolean expectedResult) {
    // Given

    // When
    boolean result = sqlEntityNameValidationService.isEntityNameLengthValid(name);

    // Then
    assertEquals(format("%s should have been valid=%s but was valid=%s", name, expectedResult, result), expectedResult, result);
  }

}
