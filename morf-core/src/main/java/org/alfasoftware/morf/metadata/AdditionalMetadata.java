package org.alfasoftware.morf.metadata;

import java.util.List;
import java.util.Collection;
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

  default Map<String, List<Index>> ignoredIndexes() {
    return Map.of();
  }

  /**
   * Provides the names of all partition tables in the database. This applies for now for postgres. Note that the order of
   * the tables in the result is not specified. The case of the
   * table names may be preserved when logging progress, but should not be relied on for schema
   * processing.
   *
   * @return A collection of all partitioned table names available in the database.
   */
  default Collection<String> partitionedTableNames() { throw new NotImplementedException("Not implemented yet."); }

  /**
   * Provides the names of all partition tables in the database. This applies for now for postgres. Note that the order of
   * the tables in the result is not specified. The case of the
   * table names may be preserved when logging progress, but should not be relied on for schema
   * processing.
   *
   * @return A collection of all partition table names available in the database.
   */
  default Collection<String> partitionTableNames() { throw new NotImplementedException("Not implemented yet."); }
}
