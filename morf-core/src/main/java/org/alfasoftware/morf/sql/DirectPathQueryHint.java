package org.alfasoftware.morf.sql;

/**
 * Singleton class representing the hint for a direct path query.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public final class DirectPathQueryHint implements Hint {

  private DirectPathQueryHint() {
    super();
  }

  public static final DirectPathQueryHint INSTANCE = new DirectPathQueryHint();

}

