package org.alfasoftware.morf.upgrade;

import java.util.Map;

public class ParallelUpgrade {
  private final UpgradeNode root;
  private final Map<String, UpgradeNode> upgradeNodes;

  public ParallelUpgrade(UpgradeNode root, Map<String, UpgradeNode> upgradeNodes) {
    super();
    this.root = root;
    this.upgradeNodes = upgradeNodes;
  }

  public UpgradeNode getRoot() {
    return root;
  }

  public Map<String, UpgradeNode> getUpgradeNodes() {
    return upgradeNodes;
  }


}
