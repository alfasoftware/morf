package org.alfasoftware.morf.metadata;

/**
 * Defines the bean for one partition range on a table. {@link PartitionByHash}
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class PartitionByHashBean extends PartitionBean implements PartitionByHash {
  protected String divider;
  protected String remainder;

  public PartitionByHashBean(String name, String divider, String remainder) {
    super(name);
    this.divider = divider;
    this.remainder = remainder;
  }

  @Override
  public String hashFunction() {
    return "MOD";
  }

  @Override
  public String divider() {
    return divider;
  }

  @Override
  public String remainder() {
    return remainder;
  }
}
