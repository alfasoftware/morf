package org.alfasoftware.morf.sql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.upgrade.PortableSqlStatement;

/**
 * Stores information about table reads and/or modification which happen in a
 * single SQL/DDL element. It also stores information bout potential
 * {@link PortableSqlStatement} usage. Read and modified sets are mutually
 * exclusive and given table name can be stored in only one of those sets.
 * Modification takes precedent over read.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class ResolvedTables {
  private final Set<String> modifiedTables = new HashSet<>();
  private final Set<String> readTables = new HashSet<>();
  private boolean portableSqlStatementUsed;

  /**
   * Store information about modification of given table.
   *
   * @param tableName modified table
   */
  public void addModifiedTable(String tableName) {
    modifiedTables.add(tableName.toUpperCase());
    readTables.remove(tableName.toUpperCase());
  }


  /**
   * Store information about read of given table.
   *
   * @param tableName read table
   */
  public void addReadTable(String tableName) {
    if (!modifiedTables.contains(tableName.toUpperCase())) {
      readTables.add(tableName.toUpperCase());
    }
  }


  /**
   * Store information about usage of {@link PortableSqlStatement}.
   */
  public void portableSqlStatementUsed() {
    portableSqlStatementUsed = true;
  }


  /**
   * @return Unmodifiable set of modified tables
   */
  public Set<String> getModifiedTables() {
    return Collections.unmodifiableSet(modifiedTables);
  }


  /**
   * @return Unmodifiable set of read tables
   */
  public Set<String> getReadTables() {
    return Collections.unmodifiableSet(readTables);
  }


  /**
   * @return true if {@link PortableSqlStatement} has been used in the analysed SQL/DDL element.
   */
  public boolean isPortableSqlStatementUsed() {
    return portableSqlStatementUsed;
  }
}
