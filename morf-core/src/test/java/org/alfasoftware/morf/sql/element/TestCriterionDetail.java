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

import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.alfasoftware.morf.sql.element.Criterion.isNull;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;

/**
 * Tests for {@link Criterion}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestCriterionDetail {

  /**
   * We should be able to add criteria if mutability is enabled.
   */
  @Test
  public void testCanMutateCriteriaWhenMutable() {
    AliasedField.withImmutableBuildersDisabled(() -> {
      Criterion criterion = and(isNull(literal(1)), isNull(literal(2)));
      criterion.getCriteria().add(isNull(literal(3)));
      assertThat(criterion.getCriteria(), hasSize(3));

      Criterion deepCopy = criterion.deepCopy();
      deepCopy.getCriteria().add(mock(Criterion.class));
      assertThat(deepCopy.getCriteria(), hasSize(4));
    });
  }


  /**
   * We should be NOT able to add criteria if mutability is disabled.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCannotMutateCriteriaWhenImmutable() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      Criterion criterion = Criterion.and(isNull(literal(1)), isNull(literal(2)));
      criterion.getCriteria().add(isNull(literal(3)));
    });
  }
}