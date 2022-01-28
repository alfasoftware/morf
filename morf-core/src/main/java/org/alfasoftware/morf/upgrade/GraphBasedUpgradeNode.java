package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a single node of the graph which is also a single upgrade step.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class GraphBasedUpgradeNode {
  final String name;
  final long sequence;
  final Set<String> reads;
  final Set<String> modifies;
  final Set<GraphBasedUpgradeNode> children = new HashSet<>();
  final Set<GraphBasedUpgradeNode> parents = new HashSet<>();
  final List<String> upgradeStatements = new ArrayList<>();

  /**
   * Default constructor.
   *
   * @param name     name of the upgrade node - usually name of class of the
   *                   corresponding {@link UpgradeStep}
   * @param sequence sequence number of this upgrade node
   * @param reads    all the tables which are read by this upgrade node
   * @param modifies all the tables which are modified by this upgrade node
   */
  public GraphBasedUpgradeNode(String name, long sequence, Set<String> reads, Set<String> modifies) {
    super();
    this.name = name;
    this.sequence = sequence;
    this.reads = reads;
    this.modifies = modifies;
  }


  /**
   * @return name of the upgrade node
   */
  public String getName() {
    return name;
  }


  /**
   * @return true if there are no dependencies defined for this upgrade step
   */
  public boolean noDependenciesDefined() {
    return reads.isEmpty() && modifies.isEmpty();
  }


  /**
   * @return sequence number
   */
  public long getSequence() {
    return sequence;
  }


  /**
   * @return upgrade nodes which depend on this upgrade node
   */
  public Set<GraphBasedUpgradeNode> getChildren() {
    return children;
  }


  /**
   * @return upgrade nodes on which this upgrade node depends on
   */
  public Set<GraphBasedUpgradeNode> getParents() {
    return parents;
  }


  /**
   * @return all the tables which are read by this upgrade node
   */
  public Set<String> getReads() {
    return reads;
  }


  /**
   * @return all the tables which are modified by this upgrade node
   */
  public Set<String> getModifies() {
    return modifies;
  }


  /**
   * @return true if this node is a no-op root node of the graph
   */
  public boolean isRoot() {
    return sequence == 0;
  }


  /**
   * Add upgrade statement to be executed by this upgrade node
   *
   * @param statement
   */
  public void addUpgradeStatements(String statement) {
    upgradeStatements.add(statement);
  }


  /**
   * Add upgrade statements to be executed by this upgrade node
   *
   * @param statement
   */
  public void addAllUpgradeStatements(Collection<String> statements) {
    upgradeStatements.addAll(statements);
  }


  /**
   * @return ordered list of statements to be executed by this upgrade node
   */
  public List<String> getUpgradeStatements() {
    return upgradeStatements;
  }


  /**
   * The hashCode of this class depends only on the name.
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (name == null ? 0 : name.hashCode());
    return result;
  }


  /**
   * Only the name property is considered while checking equality of this class.
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    GraphBasedUpgradeNode other = (GraphBasedUpgradeNode) obj;
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


  /**
   * @param nodes
   * @return String representation of the given nodes
   */
  private String nodeListToStringOfNames(Collection<GraphBasedUpgradeNode> nodes) {
    return nodes.stream().map(n -> n.getName()).collect(Collectors.joining(", "));
  }
}
