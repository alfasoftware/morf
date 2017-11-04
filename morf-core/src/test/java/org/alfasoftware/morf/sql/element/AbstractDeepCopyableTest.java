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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.function.Supplier;

import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mockito;

/**
 * Tests that must pass for anything implementing {@link DeepCopyableWithTransformation}.
 * There are more specific tests for {@link AbstractAliasedFieldTest} and
 * {@link TestTableReference}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public abstract class AbstractDeepCopyableTest<T extends DeepCopyableWithTransformation<T, Builder<T>>> {

  @Parameter(value = 0) public String testName;
  @Parameter(value = 1) public Supplier<T> onTest;

  private static int mockCounter;


  protected static <T extends DeepCopyableWithTransformation<T, Builder<T>>> Object[] testCase(String name, Supplier<T> onTest) {
    return new Object[] { name, onTest };
  }

  protected static <T extends DeepCopyableWithTransformation<T, Builder<T>>> T mockOf(Class<T> clazz) {
    T mock = mock(clazz);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(TempTransitionalBuilderWrapper.wrapper(mock));
    when(mock.toString()).thenReturn(mock.getClass().getSimpleName() + mockCounter++);
    return mock;
  }


  /**
   * Tests that {@link #hashCode()} contract is honoured.
   */
  @Test
  public void testHashCode() {
    assertEquals(onTest.get().hashCode(), onTest.get().hashCode());
  }


  @SuppressWarnings("unchecked")
  private List<Object[]> parameterData() {
    try {
      return (List<Object[]>) this.getClass().getMethod("data").invoke(this);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Tests that {@link #equals(Object)} contract is honoured.
   */
  @Test
  public void testEquals() {
    assertEquals(onTest.get(), onTest.get());
    assertFalse(onTest.get().equals(null));
    for (Object[] data : parameterData()) {
      if (data[0].equals(testName))
        continue;
      @SuppressWarnings("unchecked")
      T other = ((Supplier<T>) data[1]).get();
      assertNotEquals("Must not equal " + other, other, onTest.get());
    }
  }

  /**
   * Tests that deep copying works correctly.
   */
  @Test
  public void testDeepCopy() {
    T original = onTest.get();
    T deepCopy = original.deepCopy(DeepCopyTransformations.noTransformation()).build();
    assertEquals(deepCopy, original);
    assertNotSame(deepCopy, original);
  }

  /**
   * Compares the toString to the default implementation to make sure it's been defined.
   */
  @Test
  public void testToStringHasBeenDefined() {
    T t = onTest.get();
    assertNotEquals(defaultToString(t), t.toString());
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
}