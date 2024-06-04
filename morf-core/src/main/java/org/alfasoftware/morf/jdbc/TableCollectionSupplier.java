package org.alfasoftware.morf.jdbc;

import java.util.Collection;

import org.alfasoftware.morf.metadata.Table;

/**
 * Can supply a collection of {@link Table}.
 */
public interface TableCollectionSupplier {

  /**
   * Gets the collection of tables.
   * @return the collection of {@link Table}
   */
  Collection<Table> tables();
}
