package org.alfasoftware.morf.metadata;

/**
 * Defines the bean for one partitions on a table. {@link Partition}
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public abstract class PartitionBean implements Partition {

  protected String name;

  public PartitionBean(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }
}
