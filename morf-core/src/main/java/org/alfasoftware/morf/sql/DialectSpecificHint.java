package org.alfasoftware.morf.sql;

import java.util.Objects;

/**
 * A generic {@link org.alfasoftware.morf.sql.Hint} class that holds custom hints for a given database type.
 * It should be used instead of {@link org.alfasoftware.morf.sql.CustomHint}
 *
 */
public class DialectSpecificHint implements Hint {

  private final String databaseType;

  private final String hintContents;

  /**
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

