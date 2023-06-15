package org.alfasoftware.morf.sql;

import java.util.Objects;

public class DialectSpecificHint {

  private final String databaseType;

  private final String hintContents;

  /**
   * A generic class that holds hints for a given database type.
   *
   * @param databaseType a database type identifier. Eg: ORACLE, PGSQL, SQL_SERVER
   * @param hintContents the hint contents themselves, without the delimiters. Eg: without /*+ and *"/ * for Oracle hints
   */
  public DialectSpecificHint(String databaseType, String hintContents) {
    super();
    this.databaseType = databaseType;
    this.hintContents = hintContents;
  }


  public String getDatabaseType() {
    return databaseType;
  }


  public String getHintContents() {
    return hintContents;
  }


  @Override
  public int hashCode() {
    return Objects.hash(databaseType, hintContents);
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DialectSpecificHint other = (DialectSpecificHint) obj;
    return Objects.equals(databaseType, other.databaseType) && Objects.equals(hintContents, other.hintContents);
  }
}

