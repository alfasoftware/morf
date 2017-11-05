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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectFirstStatementBuilder;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SelectStatementBuilder;
import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mockito;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Common tests that all SQL elements must satisfy.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public abstract class AbstractAliasedFieldTest<T extends AliasedField> {

  @Parameter(value = 0)
  public String testName;

  @Parameter(value = 1)
  public Supplier<T> onTestSupplier;

  @Parameter(value = 2)
  public ImmutableList<Supplier<T>> notEqualSupplier;

  protected T onTest;
  private ImmutableList<T> notEqual;
  private T isEqual;
  protected AliasedField onTestAliased;
  private AliasedField isEqualAliased;
  private AliasedField notEqualDueToAlias;

  private static int mockCounter;


  /**
   * Creates a test case for the test parameters.
   *
   * @param name The test name.
   * @param onTest A lambda which will create an instance of the {@link AliasedField}
   *          under test. This will be called multiple times to confirm that
   *          identical copies are equivalent.
   * @param notEqual A lambda which will create an instance that does NOT equal
   *          {@code onTest}. This will be confirmed.
   * @return The test case.
   */
  @SafeVarargs
  protected static <T> Object[] testCase(
      String name,
      Supplier<T> onTest,
      Supplier<T>... notEqual) {
    return new Object[] { name, onTest, ImmutableList.copyOf(notEqual) };
  }


  @Before
  public void setup() {
    this.onTest = onTestSupplier.get();
    this.isEqual = onTestSupplier.get();
    this.notEqual = FluentIterable.from(notEqualSupplier).transform(Supplier::get).toList();
    this.onTestAliased = onTestSupplier.get().as("A");
    this.isEqualAliased = onTestSupplier.get().as("A");
    this.notEqualDueToAlias = onTestSupplier.get().as("B");
  }


  protected static SelectFirstStatement mockSelectFirstStatement() {
    SelectFirstStatement mock = mock(SelectFirstStatement.class);
    SelectFirstStatementBuilder builder = mock(SelectFirstStatementBuilder.class);
    when(builder.build()).thenReturn(mock);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(builder);
    when(mock.toString()).thenReturn("SelectFirstStatement" + mockCounter++);
    return mock;
  }


  protected static SelectStatement mockSelectStatement() {
    SelectStatement mock = mock(SelectStatement.class);
    SelectStatementBuilder builder = mock(SelectStatementBuilder.class);
    when(builder.build()).thenReturn(mock);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(builder);
    when(mock.toString()).thenReturn("SelectStatement" + mockCounter++);
    return mock;
  }


  protected static TableReference mockTableReference() {
    return mockOf(TableReference.class);
  }

  @SuppressWarnings("unchecked")
  protected static <T extends DeepCopyableWithTransformation<T, U>, U extends Builder<T>> T mockOf(Class<T> clazz) {
    T mock = mock(clazz);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn((U) TempTransitionalBuilderWrapper.wrapper(mock));
    when(mock.toString()).thenReturn(mock.getClass().getSimpleName() + mockCounter++);
    return mock;
  }

  /**
   * Confirms that hashcodes for equivalent objects match.
   */
  @Test
  public void testHashCode() {
    assertEquals(isEqual.hashCode(), onTest.hashCode());
    assertEquals(isEqualAliased.hashCode(), onTestAliased.hashCode());
  }

  /**
   * Confirms correct behaviour of the "as" method.
   */
  @Test
  public void testAs() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      assertEquals(isEqual.as("A"), onTest.as("A"));
      assertEquals(isEqual.as("B").as("A"), onTest.as("A"));
      assertNotEquals(isEqual.as("A"), onTest.as("B"));
      assertNotSame(onTest, onTest.as("A"));
    });

    // Should get the same object with immutable builders off
    assertSame(onTest, onTest.as("A"));
  }

  /**
   * Compares the toString to the default implementation to make sure it's been defined.
   */
  @Test
  public void testToStringHasBeenDefined() {
    assertNotEquals(defaultToString(onTest), onTest.toString());
  }

  /**
   * Tests that the assumption {@link #testToStringHasBeenDefined()} makes
   * (about the behaviour of the default toString method) is correct.  If this
   * predicate fails, we won't be correctly testing that toString works.
   */
  @Test
  public void testToStringAssumptionTrue() {
    Object o = new Object();
    assertEquals(defaultToString(o), o.toString());
  }


  private String defaultToString(Object o) {
    return o.getClass().getName() + "@" + Integer.toHexString(o.hashCode());
  }

  /**
   * Ensures that identically constructed instances equal each other, non-identical ones
   * don't, and that alias is taken into account.
   */
  @Test
  public void testEquals() {
    assertEquals(isEqual, onTest);
    assertFalse(onTest.equals(null));
    notEqual.forEach(ne -> assertNotEquals(ne, onTest));
    assertEquals(isEqualAliased, onTestAliased);
    assertNotEquals(notEqualDueToAlias, onTestAliased);
  }

  /**
   * Ensures that deep copies match.
   */
  @Test
  public void testDeepCopy() {
    AliasedField deepCopy = onTest.deepCopy();
    assertEquals(deepCopy, onTest);
    assertNotSame(deepCopy, onTest);
  }

  /**
   * Ensures that deep copies with aliases match.
   */
  @Test
  public void testDeepCopyAliased() {
    AliasedField deepCopy = onTestAliased.deepCopy();
    assertEquals(deepCopy, onTestAliased);
    assertNotSame(deepCopy, onTestAliased);
  }

  /**
   * Confirms that the implied name returns the alias, assuming we
   * are maintain the old, mutable behaviour of the as method
   */
  @Test
  public void testImpliedNameMutableBehaviour() {
    onTest.as("QWERTY");
    assertEquals(onTest.getImpliedName(), "QWERTY");
  }


  /**
   * Confirms that the implied name returns the alias, assuming
   * the as method now returns a new instance.
   */
  @Test
  public void testImpliedNameImmutableBehaviour() {
    AliasedField.withImmutableBuildersEnabled(() -> {
      String originalImpliedName = onTest.getImpliedName();
      onTest.as("QWERTY");
      assertEquals(onTest.getImpliedName(), originalImpliedName);
      assertEquals(onTest.as("QWERTY").getImpliedName(), "QWERTY");
    });
  }
}