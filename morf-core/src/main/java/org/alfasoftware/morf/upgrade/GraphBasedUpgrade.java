package org.alfasoftware.morf.upgrade;

import java.util.List;

/**
 * Represents fully executable upgrade in the form of the graph which can be
 * executed in a multithreaded way.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class GraphBasedUpgrade {

  private final GraphBasedUpgradeNode root;
  private final List<String> preUpgradeStatements;
  private final List<String> postUpgradeStatements;
  private final List<GraphBasedUpgradeNode> nodes;

  /**
   * Default constructor.
   *
   * @param root                  no-op upgrade node which is the root of the graph
   * @param preUpgradeStatements  statements which must be executed before the upgrade
   * @param postUpgradeStatements statements which must be executed after the upgrade
   * @param nodes                 all the nodes of this upgrade as a list, without the no-op root
   */
  public GraphBasedUpgrade(GraphBasedUpgradeNode root, List<String> preUpgradeStatements, List<String> postUpgradeStatements,
      List<GraphBasedUpgradeNode> nodes) {
    super();
    this.root = root;
    this.preUpgradeStatements = preUpgradeStatements;
    this.postUpgradeStatements = postUpgradeStatements;
    this.nodes = nodes;
  }


  /**
   * @return no-op upgrade node which is the root of the graph
   */
  public GraphBasedUpgradeNode getRoot() {
    return root;
  }


  /**
   * @return statements which must be executed before the upgrade
   */
  public List<String> getPreUpgradeStatements() {
    return preUpgradeStatements;
  }


  /**
   * @return statements which must be executed after the upgrade
   */
  public List<String> getPostUpgradeStatements() {
    return postUpgradeStatements;
  }


  /**
   * @return number of upgrade nodes in this graph, without the no-op root
   */
  public int getNodesNumber() {
    return nodes.size();
  }


  /**
   * @return nodes all the nodes of this upgrade as a list, without the no-op root
   */
  public List<GraphBasedUpgradeNode> getNodes() {
    return nodes;
  }
}
