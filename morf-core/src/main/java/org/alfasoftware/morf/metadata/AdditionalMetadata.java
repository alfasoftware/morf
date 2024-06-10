package org.alfasoftware.morf.metadata;

import java.util.Collection;

import org.apache.commons.lang3.NotImplementedException;

/**
 * Provides additional metadata.
 */
public interface AdditionalMetadata extends Schema {

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
