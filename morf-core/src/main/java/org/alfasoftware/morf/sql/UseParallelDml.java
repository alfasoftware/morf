package org.alfasoftware.morf.sql;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents the hint for executing a data manipulation language (DML) statement using a parallel plan, where supported.
 * 
 * <p>Implementation note: On Oracle this will apply two hints: 
 * <ol>
 * <li><em>ENABLE_PARALLEL_DML</em> - by default parallel DML is disabled and parallel hints are ignored. This temporarily enables it for the statement.
 * <li><em>PARALLEL</em> - the actual hint to use a parallel execution mode.
 * </ol>
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class UseParallelDml implements Hint {

  private final Optional<Integer> degreeOfParallelism;

  public UseParallelDml(Integer degreeOfParallelism) {
    this.degreeOfParallelism = Optional.ofNullable(degreeOfParallelism);
  }

  public UseParallelDml() {
    this.degreeOfParallelism = Optional.empty();
  }

  /**
   * @return the degree of parallelism for this PARALLEL query hint.
   */
  public Optional<Integer> getDegreeOfParallelism() {
    return degreeOfParallelism;
  }

  @Override
  public String toString() {
    return "UseParallelDml{" +
            "degreeOfParallelism=" + degreeOfParallelism +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UseParallelDml that = (UseParallelDml) o;
    return degreeOfParallelism.equals(that.degreeOfParallelism);
  }

  @Override
  public int hashCode() {
    return Objects.hash(degreeOfParallelism);
  }
}

