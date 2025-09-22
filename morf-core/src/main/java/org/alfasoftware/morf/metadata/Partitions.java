package org.alfasoftware.morf.metadata;

import java.util.List;

/**
 * Defines the partition collection on a table.
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public interface Partitions {
  Column column();
  PartitioningRuleType partitioningType();
  PartitioningRule partitioningRule();
  List<Partition> getPartitions();
}
