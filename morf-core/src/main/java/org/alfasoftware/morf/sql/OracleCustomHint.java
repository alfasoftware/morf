/* Copyright 2021 Alfa Financial Software */

 package org.alfasoftware.morf.sql;

/**
 * Represents a custom hint for a query
 *
 * @deprecated This class should be removed in the near future as platform specific classes should be outside of core project
 */
@Deprecated
public final class OracleCustomHint extends CustomHint {

  public OracleCustomHint(String customHint) {
    super(customHint);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}

