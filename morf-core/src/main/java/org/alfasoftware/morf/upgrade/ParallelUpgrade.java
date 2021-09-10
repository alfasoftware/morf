package org.alfasoftware.morf.upgrade;

import java.util.List;

public class ParallelUpgrade {
  private final UpgradeNode root;
  private final List<String> preUpgradeStatements;
  private final List<String> postUpgradeStatements;
  private final int nodesNumber;

  public ParallelUpgrade(UpgradeNode root, List<String> preUpgradeStatements, List<String> postUpgradeStatements, int nodesNumber) {
    super();
    this.root = root;
    this.preUpgradeStatements = preUpgradeStatements;
    this.postUpgradeStatements = postUpgradeStatements;
    this.nodesNumber = nodesNumber;
  }

  public UpgradeNode getRoot() {
    return root;
  }

  public List<String> getPreUpgradeStatements() {
    return preUpgradeStatements;
  }

  public List<String> getPostUpgradeStatements() {
    return postUpgradeStatements;
  }

  public int getNodesNumber() {
    return nodesNumber;
  }

}
