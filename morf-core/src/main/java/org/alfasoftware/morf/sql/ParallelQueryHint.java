package org.alfasoftware.morf.sql;

import java.util.Objects;
/**
 * Represents the hint for a parallel query.
 *
 * @author Copyright (c) Alfa Financial Software 2018
 */
public final class ParallelQueryHint implements Hint {

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass());
  }

  @Override
  public boolean equals(Object oth) {
    return oth != null && this.getClass() == oth.getClass();
  }

}

