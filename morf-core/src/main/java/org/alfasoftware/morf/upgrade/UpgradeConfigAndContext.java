package org.alfasoftware.morf.upgrade;

import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * Configuration and context bean for the {@link Upgrade} process.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2024
 */
public class UpgradeConfigAndContext {

  /**
   * Set of full names of upgrade step classes which should be executed non-parallel.
   * No other other upgrade step will be running while listed upgrade steps are being executed.
   * This impacts Graph-Based Upgrade only.
   */
  private Set<String> exclusiveExecutionSteps = Set.of();

  /**
   * A schema change adaptor to be used during upgrade.
   */
  private SchemaChangeAdaptor schemaChangeAdaptor = new SchemaChangeAdaptor.NoOp();

  /**
   * A schema auto-healer to be used before upgrade.
   */
  private SchemaAutoHealer schemaAutoHealer = new SchemaAutoHealer.NoOp();


  /**
   * @see #exclusiveExecutionSteps
   */
  public Set<String> getExclusiveExecutionSteps() {
    return exclusiveExecutionSteps;
  }

  /**
   * @see #exclusiveExecutionSteps
   */
  public UpgradeConfigAndContext setExclusiveExecutionSteps(Set<String> exclusiveExecutionSteps) {
    this.exclusiveExecutionSteps = exclusiveExecutionSteps;
    return this;
  }


  /**
   * @see #schemaChangeAdaptor
   */
  public SchemaChangeAdaptor getSchemaChangeAdaptor() {
    return schemaChangeAdaptor;
  }

  /**
   * @see #schemaChangeAdaptor
   */
  public void setSchemaChangeAdaptor(SchemaChangeAdaptor schemaChangeAdaptor) {
    Preconditions.checkNotNull(schemaChangeAdaptor);
    this.schemaChangeAdaptor = schemaChangeAdaptor;
  }


  /**
   * @see #schemaAutoHealer
   */
  public SchemaAutoHealer getSchemaAutoHealer() {
    return schemaAutoHealer;
  }

  /**
   * @see #schemaAutoHealer
   */
  public void setSchemaAutoHealer(SchemaAutoHealer schemaAutoHealer) {
    Preconditions.checkNotNull(schemaAutoHealer);
    this.schemaAutoHealer = schemaAutoHealer;
  }
}
