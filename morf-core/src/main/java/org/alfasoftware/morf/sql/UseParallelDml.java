package org.alfasoftware.morf.sql;

import java.util.Objects;

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

