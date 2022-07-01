package org.alfasoftware.morf.sql;

/**
 * Singleton class representing the hint for avoiding using direct path query.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public final class NoDirectPathQueryHint implements Hint {

  private NoDirectPathQueryHint() {
    super();
  }

  public static final NoDirectPathQueryHint INSTANCE = new NoDirectPathQueryHint();

}
