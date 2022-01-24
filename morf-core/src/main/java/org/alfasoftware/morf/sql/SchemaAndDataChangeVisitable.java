package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.upgrade.SchemaAndDataChangeVisitor;

/**
 * Enables visit by {@link SchemaAndDataChangeVisitor}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public interface SchemaAndDataChangeVisitable {

  /**
   * Accepts visit by {@link SchemaAndDataChangeVisitor} implementation.
   */
  void accept(SchemaAndDataChangeVisitor visitor);
}
