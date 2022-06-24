package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.sql.Hint;

/**
 * Represents the hint for a parallel DML query.
 *
 * While {@link org.alfasoftware.morf.sql.UseParallelDml} is to be used on DML statements,
 * on some dialects this DML enabling hint needs to be added to Select statements within DML statements.
 *
 * @author Copyright (c) Alfa Financial Software 2022
 */
public class AllowParallelDmlHint implements Hint {

  public AllowParallelDmlHint() {
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return  getClass().getSimpleName();
  }


  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return AllowParallelDmlHint.class.hashCode();
  }


  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return AllowParallelDmlHint.class.equals(obj);
  }

}
