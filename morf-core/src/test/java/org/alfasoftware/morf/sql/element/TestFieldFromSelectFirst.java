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

import java.util.List;

import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Tests for field from select first.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@RunWith(Parameterized.class)
public class TestFieldFromSelectFirst extends AbstractAliasedFieldTest<FieldFromSelectFirst> {

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    SelectFirstStatement stmt1 = mockOf(SelectFirstStatement.class);
    SelectFirstStatement stmt2 = mockOf(SelectFirstStatement.class);
    return ImmutableList.of(
      testCase(
        "simple",
        () -> new FieldFromSelectFirst(stmt1),
        () -> new FieldFromSelectFirst(stmt2)
      )
    );
  }

}