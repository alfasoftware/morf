package org.alfasoftware.morf.metadata;

/**
 * Defines the bean for one partition range on a table. {@link PartitionByRange}
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class PartitionByRangeBean extends PartitionBean implements PartitionByRange {
  protected String start;
  protected String end;

  public PartitionByRangeBean(String name, String start, String end) {
    super(name);
    this.start = start;
    this.end = end;
  }

  @Override
  public String start() {
    return start;
  }

  @Override
  public String end() {
    return end;
  }
}
