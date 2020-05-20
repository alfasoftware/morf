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

import static java.lang.reflect.Modifier.isStatic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.InsertStatementBuilder;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SelectStatementBuilder;
import org.alfasoftware.morf.sql.TempTransitionalBuilderWrapper;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.alfasoftware.morf.util.DeepCopyTransformations;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ObjectTreeTraverser;
import org.alfasoftware.morf.util.ObjectTreeTraverser.Driver;
import org.apache.commons.lang3.ClassUtils;
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
public abstract class AbstractDeepCopyableTest<T extends DeepCopyableWithTransformation<T, ? extends Builder<T>>> {

  @Parameter(value = 0) public String testName;
  @Parameter(value = 1) public Supplier<T> onTest;

  private static int mockCounter;

  protected static <T extends DeepCopyableWithTransformation<T, U>, U extends Builder<T>> Object[] testCaseWithBuilder(U onTest) {
    Supplier<T> supplier = () -> onTest.build();
    return testCase(supplier);
  }

  protected static <T extends DeepCopyableWithTransformation<T, U>, U extends Builder<T>> Object[] testCase(Supplier<T> onTest) {
    return new Object[] { onTest.get().toString(), onTest };
  }

  protected static <T extends DeepCopyableWithTransformation<T, U>, U extends Builder<T>> Object[] testCaseWithBuilder(String name, U onTest) {
    Supplier<T> supplier = () -> onTest.build();
    return testCase(name, supplier);
  }

  protected static <T extends DeepCopyableWithTransformation<T, U>, U extends Builder<T>> Object[] testCase(String name, Supplier<T> onTest) {
    return new Object[] { name, onTest };
  }


  protected static SelectStatement mockSelectStatement() {
    SelectStatement mock = mock(SelectStatement.class);
    SelectStatementBuilder builder = mock(SelectStatementBuilder.class);
    when(builder.build()).thenReturn(mock);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(builder);
    when(mock.toString()).thenReturn("SelectStatement" + mockCounter++);
    return mock;
  }


  protected static InsertStatement mockInsertStatement() {
    InsertStatement mock = mock(InsertStatement.class);
    InsertStatementBuilder builder = mock(InsertStatementBuilder.class);
    when(builder.build()).thenReturn(mock);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(builder);
    when(mock.toString()).thenReturn("InsertStatement" + mockCounter++);
    return mock;
  }


  protected static <T extends DeepCopyableWithTransformation<T, Builder<T>>> T mockOf(Class<T> clazz) {
    T mock = mock(clazz);
    when(mock.deepCopy(Mockito.any(DeepCopyTransformation.class))).thenReturn(TempTransitionalBuilderWrapper.wrapper(mock));
    when(mock.toString()).thenReturn(clazz.getSimpleName() + mockCounter++);
    return mock;
  }


  /**
   * Tests that {@link #hashCode()} contract is honoured.
   */
  @Test
  public void testHashCode() {
    assertEquals(onTest.get().hashCode(), onTest.get().hashCode());
    T instance = onTest.get();

    // Repeated calls to hashcode (to allow for oddness that might be created by caching).
    assertEquals(instance.hashCode(), instance.hashCode());
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
    T that = onTest.get();
    T tother = onTest.get();

    // Creating the same thing twice produces equal results
    assertEquals(tother, that);

    // And on a second attempt (in case of caching)
    assertEquals(tother, that);

    // Even when running immutably
    AliasedField.withImmutableBuildersEnabled(() -> {
      assertEquals(onTest.get(), that);
      assertEquals(onTest.get(), onTest.get());
    });

    // Must never equal null
    assertFalse(that.equals(null));

    // Must not match any of the others
    for (Object[] data : parameterData()) {
      if (data[0].equals(testName))
        continue;

      // Mutably
      {
        @SuppressWarnings("unchecked")
        T other = ((Supplier<T>) data[1]).get();
        assertNotEquals("Must not equal " + other, other, that);
      }

      // Or immutably
      AliasedField.withImmutableBuildersEnabled(() -> {
        @SuppressWarnings("unchecked")
        T other = ((Supplier<T>) data[1]).get();
        assertNotEquals("Must not equal " + other, other, onTest.get());
      });

    }
  }

  /**
   * Tests that deep copying works correctly.
   */
  @Test
  public void testDeepCopy() {
    T original = onTest.get();
    {
      T deepCopy = original.deepCopy(DeepCopyTransformations.noTransformation()).build();
      assertEquals(original, deepCopy);
      assertNotSame(deepCopy, original);
    }
    AliasedField.withImmutableBuildersEnabled(() -> {
      T deepCopy = original.deepCopy(DeepCopyTransformations.noTransformation()).build();
      assertEquals(original, deepCopy);
      assertNotSame(deepCopy, original);
    });
  }


  /**
   * Ensures that we {@link ObjectTreeTraverser.Driver#drive(ObjectTreeTraverser)}
   * through all non-primitive, non-enum fields on the object if it is a {@link Driver}.
   * This test is not complete and may break in some cases.  If it does, don't assume
   * your code is wrong.  The exclusions just may need tweaking.
   */
  @Test
  public void testDrive() throws IllegalArgumentException, IllegalAccessException {
    T instance = onTest.get();

    if (!Driver.class.isInstance(instance))
      return;

    ObjectTreeTraverser.Callback callback = mock(ObjectTreeTraverser.Callback.class);
    ObjectTreeTraverser.forCallback(callback).dispatch(instance);

    verify(callback, times(1)).visit(instance);

    Map<Object, AtomicInteger> appearances = new HashMap<>();

    scanObjectForUniqueFieldValues(instance, instance.getClass(), appearances);

    appearances.entrySet().forEach(entry -> {
      Object value = entry.getKey();
      int instances = entry.getValue().get();
      verify(callback, times(instances)).visit(value);
    });
  }


  private void scanObjectForUniqueFieldValues(T instance, Class<?> clazz, Map<Object, AtomicInteger> appearances) throws IllegalAccessException {
    for (Field f : clazz.getDeclaredFields()) {

      if (isStatic(f.getModifiers()) || !typeIsOfInterest(f.getType()))
        continue;

      f.setAccessible(true);
      Object value = f.get(instance);
      if (value == null)
        continue;

      if (value instanceof Optional) {
        if (!((Optional) value).isPresent())
          continue;

        if (typeIsOfInterest(((Optional) value).get().getClass())) {
          incAppearanceCount(appearances, ((Optional) value).get());
        }
      }

      if (value instanceof Iterable) {
        for (Object o : (Iterable<?>) value) {
          if (typeIsOfInterest(o.getClass())) {
            incAppearanceCount(appearances, o);
          }
        }
      } else if (value instanceof Map) {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          if (typeIsOfInterest(entry.getKey().getClass())) {
            incAppearanceCount(appearances, entry.getKey());
          }
          if (typeIsOfInterest(entry.getValue().getClass())) {
            incAppearanceCount(appearances, entry.getValue());
          }
        }
      } else {
        incAppearanceCount(appearances, value);
      }
    }

    if (clazz.getSuperclass() != null) {
      scanObjectForUniqueFieldValues(instance, clazz.getSuperclass(), appearances);
    }
  }


  private boolean typeIsOfInterest(Class<?> clazz) {
    if (clazz == String.class ||
        clazz.isEnum() ||
        clazz.isArray() ||
        clazz.isPrimitive() ||
        ClassUtils.wrapperToPrimitive(clazz) != null) {
      return false;
    }
    return true;
  }


  private void incAppearanceCount(Map<Object, AtomicInteger> appearances, Object value) {
    AtomicInteger atomicInteger = appearances.get(value);
    if (atomicInteger == null) {
      appearances.put(value, new AtomicInteger(1));
    } else {
      atomicInteger.incrementAndGet();
    }
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