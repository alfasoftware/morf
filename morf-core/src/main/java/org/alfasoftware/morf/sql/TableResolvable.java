package org.alfasoftware.morf.sql;

/**
 * Interface which enables automatic resolution/discovery of tables used in the
 * implementing SQL/DDL element.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public interface TableResolvable {

  /**
   * Resolve tables used in read-only and write modes and store the information in
   * the {@link ResolvedTables} object.
   *
   * @param resolvedTables where the information will be stored
   */
  void resolveTables(ResolvedTables resolvedTables);
}
