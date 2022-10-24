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

  private AllowParallelDmlHint() {
    super();
  }

  public final static AllowParallelDmlHint INSTANCE = new AllowParallelDmlHint();

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return  getClass().getSimpleName();
  }


}
