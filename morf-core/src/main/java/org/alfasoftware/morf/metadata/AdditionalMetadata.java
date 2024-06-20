package org.alfasoftware.morf.metadata;

import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;

/**
 * Provides additional metadata.
 */
public interface AdditionalMetadata extends Schema {

  /**
   * The names of all the primary key indexes in the database,
   * @return A collection of the names of all the primary key indexes.
   */
  default Map<String, String> primaryKeyIndexNames() {
    throw new NotImplementedException("Not implemented yet.");
  }
}
