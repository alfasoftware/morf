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
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

/**
 * Unit tests {@link NullFieldLiteral}
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestNullFieldLiteral {


  /**
   * Verify that deep copy works as expected for an alias null field literal.
   */
  @Test
  public void testDeepCopyWithAlias() {
    NullFieldLiteral nullFieldLiteral = new NullFieldLiteral();
    nullFieldLiteral.as("testName");
    NullFieldLiteral nullCopy = (NullFieldLiteral)nullFieldLiteral.deepCopy();

    assertEquals("Field literal alias does not match", nullFieldLiteral.getAlias(), nullCopy.getAlias());
  }


  /**
   * Verify that deep copy works as expected for a null field literal, and
   * returns a different object to the one that is passed in.
   */
  @Test
  public void testDeepCopyReturnsDifferentObjects() {
    NullFieldLiteral nullFieldLiteral = new NullFieldLiteral();
    NullFieldLiteral nullCopy = (NullFieldLiteral)nullFieldLiteral.deepCopy();

    assertNotSame("Same object is returned at deep copy", nullFieldLiteral, nullCopy);
  }

}
