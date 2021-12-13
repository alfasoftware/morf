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

import static org.alfasoftware.morf.sql.UnionSetOperator.UnionStrategy.ALL;
import static org.alfasoftware.morf.util.DeepCopyTransformations.noTransformation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.FieldReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link UnionSetOperator}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestUnionSetOperator {

  /**
   * Exception verifier.
   */
  @Rule
  public ExpectedException exception = ExpectedException.none();


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if the {@code parentSelect} reference
   * is null.
   */
  @Test
  public void testUnionSetOperatorWithNullParentSelect() {
    // Given
    SelectStatement parentSelect = null;
    SelectStatement childSelect = new SelectStatement();
    exception.expect(IllegalArgumentException.class);

    // When
    new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if the {@code childSelect} reference
   * is null.
   */
  @Test
  public void testConstructorWithNullChildSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement();
    SelectStatement childSelect = null;
    exception.expect(IllegalArgumentException.class);

    // When
    new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if {@code parentSelect} and
   * {@code childSelect} number of fields don't match.
   */
  @Test
  public void testConstructorWithDifferentNumberFields() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"), new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF"));
    exception.expect(IllegalArgumentException.class);

    // When
    new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if {@code parentSelect} and
   * {@code childSelect} number of fields don't match.
   */
  @Test
  public void testConstructorWithSameNumberOfFields() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF"));

    // When
    UnionSetOperator union = new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    assertNotNull(union);
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor throws an
   * {@linkplain IllegalArgumentException} if the {@code childSelect} parameter
   * contains an order-by statement.
   */
  @Test
  public void testConstructorWithSortedChildSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement(new FieldReference("DEF")).orderBy(new FieldReference("XYZ"));
    exception.expect(IllegalArgumentException.class);

    // When
    new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    // IllegalArgumentException thrown
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator} constructor allows the
   * creation of a union where the {@code parentSelect} contains an order-by
   * statement.
   */
  @Test
  public void testConstructorWithSortedParentSelect() {
    // Given
    SelectStatement parentSelect = new SelectStatement().orderBy(new FieldReference("ABC"));
    SelectStatement childSelect = new SelectStatement();

    // When
    UnionSetOperator union = new UnionSetOperator(ALL, parentSelect, childSelect);

    // Then
    assertNotNull(union);
  }


  /**
   * Verifies that the {@linkplain UnionSetOperator#deepCopy()} method returns a
   * new instance of the original object, but with different objects.
   * <p>
   * Equality cannot be tested since the {@link Object#equals(Object)} method is
   * not overridden in {@link SelectStatement} and {@link UnionSetOperator}.
   * </p>
   */
  @Test
  public void testDeepCopy() {
    // Given
    SelectStatement parentSelect = new SelectStatement();
    SelectStatement childSelect = new SelectStatement();
    UnionSetOperator original = new UnionSetOperator(ALL, parentSelect, childSelect);

    // When
    UnionSetOperator copy = (UnionSetOperator) original.deepCopy(noTransformation()).build();

    // Then
    assertNotSame(original, copy);
    assertEquals(original.getUnionStrategy(), copy.getUnionStrategy());
    assertNotSame(childSelect, copy.getSelectStatement());
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    SelectStatement parentSelect = mock(SelectStatement.class);
    SelectStatement childSelect = mock(SelectStatement.class);
    UnionSetOperator original = new UnionSetOperator(ALL, parentSelect, childSelect);
    ResolvedTables res = new ResolvedTables();

    //when
    original.resolveTables(res);

    //then
    verify(childSelect).resolveTables(res);
  }

}