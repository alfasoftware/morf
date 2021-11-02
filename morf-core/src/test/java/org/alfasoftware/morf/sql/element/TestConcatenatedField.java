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

import static org.alfasoftware.morf.sql.SqlUtils.concat;
import static org.alfasoftware.morf.sql.SqlUtils.literal;

import java.util.List;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;

/**
 * Tests {@link ConcatenatedField}.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
@RunWith(Parameterized.class)
public class TestConcatenatedField extends AbstractAliasedFieldTest<ConcatenatedField> {

  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return ImmutableList.of(
      testCase(
        "asString",
        () -> concat(literal(1), literal(2)),
        () -> concat(literal(1), literal(3)),
        () -> concat(ImmutableList.of(literal(1), literal(3))),
        () -> concat(literal(1), literal(2), literal(3))
      )
    );
  }
}