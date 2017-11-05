package org.alfasoftware.morf.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.alfasoftware.morf.sql.element.AbstractDeepCopyableTest;
import org.alfasoftware.morf.util.Builder;
import org.alfasoftware.morf.util.DeepCopyableWithTransformation;
import org.alfasoftware.morf.util.ShallowCopyable;
import org.junit.Test;

/**
 * Extends {@link AbstractDeepCopyableTest} to add additional tests which must pass for a class
 * which also supports {@link CopyOnWriteable}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 *
 * @param <T> The type under test.
 */
public abstract class AbstractShallowAndDeepCopyableTest<T extends DeepCopyableWithTransformation<T, ? extends Builder<T>>> extends AbstractDeepCopyableTest<T> {

  /**
   * Ensures that we can perform a shallow copy to a builder and use that to
   * create an identical copy of the original.  Relies on the tests in
   * {@link AbstractDeepCopyableTest} to know that we can rely on {@link #equals(Object)}
   * as a valid test.
   */
  @Test
  public void testShallowCopy() {
    T original = onTest.get();
    @SuppressWarnings("unchecked")
    Builder<T> builder = ((ShallowCopyable<T, Builder<T>>) original).shallowCopy();
    assertNotSame(builder, original);
    T copy = builder.build();
    assertNotSame(original, copy);
    assertEquals(original, copy);
  }
}