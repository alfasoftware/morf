package org.alfasoftware.morf.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines the bean for the partitions collection on a table. {@link Partitions}
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class PartitionsBean implements Partitions {
  Column column;
  PartitioningRuleType partitioningType;
  PartitioningRule partitioningRule;
  List<Partition> partitions;

  public PartitionsBean() {
    this.column = null;
    this.partitions = new ArrayList<>();
  }

  public PartitionsBean(Column column, PartitioningRuleType partitioningType) {
    this(column, partitioningType, null, null);
  }

  public PartitionsBean(Column column, PartitioningRuleType partitioningType, PartitioningRule partitioningRule) {
    this(column, partitioningType, partitioningRule, null);
  }

  public PartitionsBean(Column column, PartitioningRuleType partitioningType, PartitioningRule partitioningRule, List<Partition> partitions) {
    this.column = column;
    this.partitioningType = partitioningType;
    this.partitioningRule = partitioningRule;
    this.partitions = partitions;
  }


  @Override
  public Column column() {
    return column;
  }

  @Override
  public PartitioningRuleType partitioningType() {
    return partitioningType;
  }

  @Override
  public PartitioningRule partitioningRule() {
    return partitioningRule;
  }

  @Override
  public List<Partition> getPartitions() {
    return partitions;
  }
}
