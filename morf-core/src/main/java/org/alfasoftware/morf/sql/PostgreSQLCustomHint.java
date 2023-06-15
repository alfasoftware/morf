package org.alfasoftware.morf.sql;

/**
 * Represents a custom hint for a query
 */
public final class PostgreSQLCustomHint implements CustomHint {

  private final String customHint;

  public PostgreSQLCustomHint(String customHint) {
    this.customHint = customHint;
  }

  @Override
  public String getCustomHint() {
    return customHint;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
