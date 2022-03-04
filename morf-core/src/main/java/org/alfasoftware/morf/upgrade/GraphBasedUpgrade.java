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
  private final int numberOfNodes;

  /**
   * Default constructor.
   *
   * @param root                  no-op upgrade node which is the root of the graph
   * @param preUpgradeStatements  statements which must be executed before the upgrade
   * @param postUpgradeStatements statements which must be executed after the upgrade
   * @param numberOfNodes            number of nodes in this upgrade, without the no-op root
   */
  public GraphBasedUpgrade(GraphBasedUpgradeNode root, List<String> preUpgradeStatements, List<String> postUpgradeStatements, int numberOfNodes) {
    super();
    this.root = root;
    this.preUpgradeStatements = preUpgradeStatements;
    this.postUpgradeStatements = postUpgradeStatements;
    this.numberOfNodes = numberOfNodes;
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
  public int getNumberOfNodes() {
    return numberOfNodes;
  }
}
