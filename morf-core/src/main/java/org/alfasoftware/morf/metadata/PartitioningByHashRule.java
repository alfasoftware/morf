package org.alfasoftware.morf.metadata;

import java.util.ArrayList;
import java.util.List;


public class PartitioningByHashRule implements PartitioningRule {
  private String columnName;
  private int hashDivider;
  private List<Integer> hashRemainders;
  private int count;



  public PartitioningByHashRule(String columnName, int hashDivider) {
    this.columnName = columnName;
    this.hashDivider = hashDivider;
    this.count = hashDivider;
    this.hashRemainders = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      this.hashRemainders.add(i);
    }
  }


  @Override
  public String getColumn() {
    return columnName;
  }


  @Override
  public PartitioningRuleType getPartitioningType() {
    return PartitioningRuleType.hashPartitioning;
  }


  public int getHashDivider() {
    return hashDivider;
  }


  public List<Integer> getHashRemainders() {
    return hashRemainders;
  }


  public int getCount() {
    return count;
  }
}
