package org.alfasoftware.morf.metadata;

/**
 * A name/value pair.  Note that the name is considered case-insensitive.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
public interface DataValue {

  /**
   * @return The name of the value. This is considered case-insensitive.
   */
  CaseInsensitiveString getName();

  /**
   * @return The value.
   */
  Object getObject();

  /**
   * Two {@link DataValue} instances are considered equal if their
   * values are equal (and thus have the same type) and their
   * names are equal (ignoring case).
   *
   * @see Object#equals(Object)
   */
  @Override
  boolean equals(Object obj);

}
