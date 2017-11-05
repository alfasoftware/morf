package org.alfasoftware.morf.util;

/**
 * Defines an object which can be shallow copied to a builder, allowing its
 * properties to be modified before it is constructed.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 *
 * @param <T> The object type.
 * @param <U> The builder type.
 */
public interface ShallowCopyable<T, U extends Builder<T>> {

  /**
   * Performs a shallow copy to a builder, allowing the copy's properties to be
   * modified before it is constructed.
   *
   * @return The builder.
   */
  public U shallowCopy();
}