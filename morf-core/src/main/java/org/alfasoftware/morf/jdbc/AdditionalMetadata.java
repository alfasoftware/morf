package org.alfasoftware.morf.jdbc;

import java.util.Collection;

import org.alfasoftware.morf.metadata.Table;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Provides additional metadata.
 */
public interface AdditionalMetadata {

  /**
   * Gets the collection of tables.
   * @return the collection of {@link Table}
   */
  Collection<Table> tables();


  /**
   * The names of all the primary key indexes in the database,
   * @return A collection of the names of all the primary key indexes.
   */
  default Collection<String> primaryKeyIndexNames() {
    throw new NotImplementedException("Not implemented yet.");
  };
}
