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

package org.alfasoftware.morf.sql;

import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Checks that {@link DeleteStatement} satisfies equals, hashcode and deep copy contracts
 * if we construct one using the builder.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
@RunWith(Parameterized.class)
public class TestDeleteStatementDeepCopyContractUsingBuilder extends AbstractShallowAndDeepCopyableTest<DeleteStatement> {

  private static final Criterion CRITERION_1 = mockOf(Criterion.class);
  private static final Criterion CRITERION_2 = mockOf(Criterion.class);
  private static final TableReference TABLE_1 = mockOf(TableReference.class);
  private static final TableReference TABLE_2 = mockOf(TableReference.class);


  @Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(
      testCaseWithBuilder(DeleteStatement.delete(TABLE_1)),
      testCaseWithBuilder(DeleteStatement.delete(TABLE_2)),
      testCaseWithBuilder(DeleteStatement.delete(TABLE_1).where(CRITERION_1)),
      testCaseWithBuilder(DeleteStatement.delete(TABLE_1).where(CRITERION_2)),
      testCaseWithBuilder(DeleteStatement.delete(TABLE_1).limit(1000)),
      testCaseWithBuilder(DeleteStatement.delete(TABLE_1).where(CRITERION_2).limit(1000))
    );
  }

}