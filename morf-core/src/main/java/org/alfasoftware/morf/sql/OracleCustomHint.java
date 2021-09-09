/* Copyright 2021 Alfa Financial Software */

 package org.alfasoftware.morf.sql;

import java.util.Objects;

/**
 * Represents a custom hint for a query
 */
public final class OracleCustomHint implements CustomHint {

  private final String customHint;

  public OracleCustomHint(String customHint) {
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

  @Override
  public int hashCode() {
    return Objects.hash(getClass());
  }

  @Override
  public boolean equals(Object oth) {
    return oth != null && this.getClass() == oth.getClass();
  }

}


