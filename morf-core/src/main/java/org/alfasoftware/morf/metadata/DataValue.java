package org.alfasoftware.morf.metadata;

import java.util.Arrays;
import java.util.List;

/**
 * A name/value pair.  Note that the name is considered case-insensitive.
 *
 * <p>{@code equals} and {@code hashCode} have a strictly defined behaviour which should hold true
 * for all implementations, regardless of implementation class, similarly to the Collections classes
 * such as {@link List}.  These are implemented in {@link #defaultEquals(DataValue, Object)}
 * and {@link #defaultHashCode(DataValue)} and can simply be called by implementations
 * in a single line.</p>
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


  /**
   * Default hashCode implementation for instances.
   *
   * @param obj The object.
   * @return The hashCode.
   */
  public static int defaultHashCode(DataValue obj) {
    final int prime = 31;
    int result = 1;
    result = prime * result + obj.getName().hashCode();
    if (obj.getObject() == null) {
      result = prime * result;
    } else if (obj.getObject().getClass().isArray()) {
      result = prime * result + Arrays.hashCode((byte[]) obj.getObject());
    } else {
      result = prime * result + obj.getObject().hashCode();
    }
    return result;
  }


  /**
   * Default equals implementation for instances.
   *
   * @param obj1 this
   * @param obj2 the other
   * @return true if equivalent.
   */
  public static boolean defaultEquals(DataValue obj1, Object obj2) {
    if (obj1 == obj2) return true;
    if (obj2 == null) return false;
    if (!(obj2 instanceof DataValue)) return false;
    DataValue other = (DataValue) obj2;
    if (!obj1.getName().equals(other.getName())) return false;
    if (obj1.getObject() == null) {
      if (other.getObject() != null) return false;
    } else if (obj1.getObject().getClass().isArray()) {
      if (!other.getObject().getClass().isArray()) return false;
      return Arrays.equals((byte[]) obj1.getObject(), (byte[]) other.getObject());
    } else {
      if (!obj1.getObject().equals(other.getObject())) return false;
    }
    return true;
  }
}
