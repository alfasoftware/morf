package org.alfasoftware.morf.metadata;

/**
 * Defines a partition by range on a table.
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public interface PartitionByHash extends Partition {
  String hashFunction();
  String divider();
  String remainder();
}
