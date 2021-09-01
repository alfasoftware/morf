package org.alfasoftware.morf.sql;

/**
 * Singleton class Representing the hint for a direct path query.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public final class DirectPathQueryHint implements Hint {


  private DirectPathQueryHint() {
    super();
  }


  private static DirectPathQueryHint INSTANCE = new DirectPathQueryHint();


  /**
   * @return the {@link DirectPathQueryHint} instance.
   */
  public static DirectPathQueryHint getInstance() {
    return INSTANCE;
  }

}

