package org.alfasoftware.morf.upgrade;

import java.util.Set;

/**
 * Configuration bean for the {@link Upgrade} process.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2024
 */
public class UpgradeConfiguration {

  /**
   * Set of full names of upgrade step classes which should be executed non-parallel.
   * No other other upgrade step will be running while listed upgrade steps are being executed.
   * This impacts Graph-Based Upgrade only.
   */
  private Set<String> exclusiveExecutionSteps = Set.of();


  public Set<String> getExclusiveExecutionSteps() {
    return exclusiveExecutionSteps;
  }

  public UpgradeConfiguration setExclusiveExecutionSteps(Set<String> exclusiveExecutionSteps) {
    this.exclusiveExecutionSteps = exclusiveExecutionSteps;
    return this;
  }


}
