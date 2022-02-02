package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.BasicNode;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.Node;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.PrintableGraph;

/**
 * Adapts GraphBasedUpgrade graph to the form of {@link PrintableGraph}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class GraphBasedUpgradeNodeDrawIOAdapter implements PrintableGraph<Node> {
  private final List<GraphBasedUpgradeNode> nodes;

  /**
   * Default constructor.
   *
   * @param nodes all the nodes of the graph
   * @param root  of the graph
   */
  public GraphBasedUpgradeNodeDrawIOAdapter(List<GraphBasedUpgradeNode> nodes, GraphBasedUpgradeNode root) {
    this.nodes = new ArrayList<>(nodes);
    this.nodes.add(root);
  }


  @Override
  public Set<Node> getNodes() {
    return nodes.stream().map(step -> {
      return new BasicNode("" + step.getName() + "-" + step.getSequence(), step.getSequence());
    }).collect(Collectors.toSet());
  }


  @Override
  public Set<Node> getNodesThisNodeLinksTo(Node node) {
    return nodes.stream().filter(step -> step.getSequence() == node.getSequence()).findFirst().get().getParents().stream()
        .map(dep -> {
          return new BasicNode("" + dep.getName() + "-" + dep.getSequence(), dep.getSequence());
        }).collect(Collectors.toSet());
  }

}
