package org.alfasoftware.morf.sql;

/**
 * Represents a custom hint for a query
 *
 * @deprecated This class should be removed in the near future as platform specific classes should be outside of core project
 */
@Deprecated
public final class PostgreSQLCustomHint extends CustomHint {

  public PostgreSQLCustomHint(String customHint) {
    super(customHint);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
