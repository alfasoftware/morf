package org.alfasoftware.morf.sql;

import java.util.Objects;

/**
 * Represents the hint for a direct path query.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class DirectPathQueryHint implements Hint {


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return Objects.hash(getClass());
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object oth) {
    return oth != null && this.getClass() == oth.getClass();
  }

}

