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
  private SchemaChangeAdaptor upgradeChangeAdaptor = new SchemaChangeAdaptor.NoOp();


  public Set<String> getExclusiveExecutionSteps() {
    return exclusiveExecutionSteps;
  }

  public UpgradeConfigAndContext setExclusiveExecutionSteps(Set<String> exclusiveExecutionSteps) {
    this.exclusiveExecutionSteps = exclusiveExecutionSteps;
    return this;
  }


  public SchemaChangeAdaptor getSchemaChangeAdaptor() {
    return upgradeChangeAdaptor;
  }

  public void setSchemaChangeAdaptor(SchemaChangeAdaptor upgradeChangeAdaptor) {
    Preconditions.checkNotNull(upgradeChangeAdaptor);
    this.upgradeChangeAdaptor = upgradeChangeAdaptor;
  }
}
