package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UpgradeNode {
  final String name;
  final long sequence;
  final Set<String> reads;
  final Set<String> modifies;
  final Set<UpgradeNode> children = new HashSet<>();
  final Set<UpgradeNode> parents = new HashSet<>();
  final List<String> upgradeStatements = new ArrayList<>();

  public UpgradeNode(String name, long sequence, Set<String> reads, Set<String> modifies) {
    super();
    this.name = name;
    this.sequence = sequence;
    this.reads = reads;
    this.modifies = modifies;
  }


  public String getName() {
    return name;
  }


  public boolean noDependenciesDefined() {
    return reads.isEmpty() && modifies.isEmpty();
  }


  public long getSequence() {
    return sequence;
  }


  public Set<UpgradeNode> getChildren() {
    return children;
  }


  public Set<UpgradeNode> getParents() {
    return parents;
  }


  public Set<String> getReads() {
    return reads;
  }


  public Set<String> getModifies() {
    return modifies;
  }


  public boolean isRoot() {
    return sequence == 0;
  }

  public void addUpgradeStatement(String statement) {
    upgradeStatements.add(statement);
  }

  public void addAllUpgradeStatements(Collection<String> statements) {
    upgradeStatements.addAll(statements);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (name == null ? 0 : name.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    UpgradeNode other = (UpgradeNode) obj;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "UpgradeNode [name=" + name + ", sequence=" + sequence + ", reads=" + reads + ", modifies=" + modifies + ", root="
        + isRoot() + ", children=" + nodeListToStringOfNames(children) + ", parents=" + nodeListToStringOfNames(parents) + "]";
  }


  private String nodeListToStringOfNames(Collection<UpgradeNode> nodes) {
    return nodes.stream()
        .map(n -> n.getName())
        .collect(Collectors.joining(", "));
  }

}
