package org.alfasoftware.morf.sql;

import java.util.Objects;

/**
 * Represents a hint for executing given data manipulation language (DML) statement
 * as a create-table-as-select (CTAS) statement instead, where supported.
 *
 * Important: Given DML where this hint is applied is expected to be translatable to a CTAS.
 * For example, updating all the table records, as opposed to updating of just some records.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public class CtasDuringUpgrade implements Hint {

  private static final String ANY = "<ANY>";

  private final String useForDatabaseType;


  /**
   * Enable CTAS transformation for all database types.
   */
  public CtasDuringUpgrade() {
    this(true);
  }


  /**
   * Enable CTAS transformation for all database types.
   * @param useCtasDuringUpgrade True to enable, false to disable.
   */
  public CtasDuringUpgrade(boolean useCtasDuringUpgrade) {
    this.useForDatabaseType = useCtasDuringUpgrade ? ANY : null;
  }


  /**
   * Enable CTAS transformation for given database type.
   * @param databaseType Database type to enable this hint for.
   */
  public CtasDuringUpgrade(String databaseType) {
    this.useForDatabaseType = databaseType;
  }


  /**
   * Tells whether CTAS transformation is enabled for any database type.
   */
  public boolean getUseCtasDuringUpgrade() {
    return ANY.equals(useForDatabaseType);
  }


  /**
   * Tells whether CTAS transformation is enabled for given database type.
   */
  public boolean getUseCtasDuringUpgrade(String databaseType) {
    return ANY.equals(useForDatabaseType)
        || databaseType.equals(useForDatabaseType);
  }


  @Override
  public String toString() {
    return "CtasDuringUpgrade{" +
            "useForDatabaseType=" + useForDatabaseType +
            "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || this.getClass() != obj.getClass()) return false;
    CtasDuringUpgrade that = (CtasDuringUpgrade) obj;
    return Objects.equals(this.useForDatabaseType, that.useForDatabaseType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(useForDatabaseType);
  }
}
