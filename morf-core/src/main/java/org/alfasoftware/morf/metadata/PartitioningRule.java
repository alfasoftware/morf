package org.alfasoftware.morf.metadata;

/**
 * Represents a partitioning rule.
 */
public interface PartitioningRule {
    String getColumn();
    PartitioningRuleType getPartitioningType();
}
