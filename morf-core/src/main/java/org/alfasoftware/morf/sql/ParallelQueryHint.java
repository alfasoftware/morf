package org.alfasoftware.morf.sql;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents the hint for a parallel query.
 *
 * @author Copyright (c) Alfa Financial Software 2018
 */
public final class ParallelQueryHint implements Hint {

  private final Optional<Integer> degreeOfParallelism;

  public ParallelQueryHint(Optional<Integer> degreeOfParallelism) {
    this.degreeOfParallelism = degreeOfParallelism;
  }


  public ParallelQueryHint() {
    this(Optional.empty());
  }


  /**
   * @return the degree of parallelism for this PARALLEL query hint.
   */
  public Optional<Integer> getDegreeOfParallelism() {
    return degreeOfParallelism;
  }


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
