package org.alfasoftware.morf.upgrade;

import java.util.List;

public class ParallelUpgrade {
  private final UpgradeNode root;
  private final List<String> preUpgradeStatements;
  private final List<String> postUpgradeStatements;
  private final List<UpgradeNode> nodes;

  public ParallelUpgrade(UpgradeNode root, List<String> preUpgradeStatements, List<String> postUpgradeStatements, List<UpgradeNode> nodes) {
    super();
    this.root = root;
    this.preUpgradeStatements = preUpgradeStatements;
    this.postUpgradeStatements = postUpgradeStatements;
    this.nodes = nodes;
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
    return nodes.size();
  }

  public List<UpgradeNode> getNodes() {
    return nodes;
  }

}
