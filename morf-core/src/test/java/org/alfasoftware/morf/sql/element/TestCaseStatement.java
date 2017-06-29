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

import org.junit.Test;

/**
 * Tests {@link CaseStatement}
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TestCaseStatement {


  /**
   * Verify that deep copy works as expected for an alias null field literal.
   */
  @Test
  public void testDeepCopyWithAlias() {
    CaseStatement cs = new CaseStatement(
      new FieldLiteral(111),
      new WhenCondition(
        Criterion.eq(new FieldLiteral('A'), new FieldLiteral('B')),
        new FieldLiteral('C')
      )
    );
    cs.as("testAlias");
    CaseStatement csCopy = (CaseStatement)cs.deepCopy();

    assertEquals("Field literal alias does not match", cs.getAlias(), csCopy.getAlias());
  }
}
