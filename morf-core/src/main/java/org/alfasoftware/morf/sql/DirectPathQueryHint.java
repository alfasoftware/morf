package org.alfasoftware.morf.sql;

/**
 * Represents the hint for a direct path query.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class DirectPathQueryHint implements Hint {


  private DirectPathQueryHint() {}


  private static DirectPathQueryHint INSTANCE = new DirectPathQueryHint();


  /**
   * @return the {@link DirectPathQueryHint} instance.
   */
  public static DirectPathQueryHint getInstance() {
    return INSTANCE;
  }

}

