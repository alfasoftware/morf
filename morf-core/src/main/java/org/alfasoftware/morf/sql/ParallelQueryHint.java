package org.alfasoftware.morf.sql;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;

import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents the hint for a parallel query.
 *
 * @author Copyright (c) Alfa Financial Software 2018
 */
public final class ParallelQueryHint implements Hint {

  private final Integer degreeOfParallelism;

  public ParallelQueryHint(int degreeOfParallelism) {
    this.degreeOfParallelism = degreeOfParallelism;
  }


  public ParallelQueryHint() {
    this.degreeOfParallelism = null;
  }


  /**
   * @return the degree of parallelism for this PARALLEL query hint.
   */
  public Optional<Integer> getDegreeOfParallelism() {
    return Optional.ofNullable(degreeOfParallelism);
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return degreeOfParallelism == null ? getClass().getSimpleName() : format("ParallelQueryHint [degreeOfParallelism=%s]", degreeOfParallelism.toString());
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(ofNullable(degreeOfParallelism).orElse(null)).build();
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ParallelQueryHint)) return false;
    ParallelQueryHint other = (ParallelQueryHint) obj;
    return new EqualsBuilder().append(ofNullable(degreeOfParallelism).orElse(null), ofNullable(other.degreeOfParallelism).orElse(null)).isEquals();
  }

}
