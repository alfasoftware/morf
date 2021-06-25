package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class ParallelUpgrade extends Upgrade {

  private static final Log log = LogFactory.getLog(ParallelUpgrade.class);

  ParallelUpgrade(ConnectionResources connectionResources, DataSource dataSource, UpgradePathFactory factory,
      UpgradeStatusTableService upgradeStatusTableService) {
    super(connectionResources, dataSource, factory, upgradeStatusTableService);

  }


  public void execute(SqlDialect dialect, Schema sourceSchema, Schema targetSchema, List<String> upgradeStatements,
      ViewChanges viewChanges, List<UpgradeStep> upgradesToApply) {
    List<UpgradeNode> nodes = produceNodes(upgradesToApply);
    UpgradeNode root = prepareGraph(nodes);
    logGraph(root);
  }


  UpgradeNode prepareGraph(List<UpgradeNode> nodes) {
    UpgradeNode root = new UpgradeNode("root", 0, new HashSet<>(), new HashSet<>());
    List<UpgradeNode> processedNodes = new ArrayList<>();
    for(UpgradeNode node : nodes) {
      // Case where there are no annotations on the node
      if(node.noDependenciesDefined()) {
        handleNotAnnotatedNode(processedNodes, node, root);
      } else {
        handleAnnotatedNode(processedNodes, node, root);
      }
      processedNodes.add(node);
    }
    return root;
  }


  private void handleAnnotatedNode(List<UpgradeNode> processedNodes, UpgradeNode node, UpgradeNode root) {
    // if nothing has been processed add node as child of the root
    if(processedNodes.isEmpty()) {
      log.debug("Root empty, adding node: " + node.getName() + " as child of the root");
      addEdge(root, node);
      return;
    }

    // for each processed node
    for(UpgradeNode processed : processedNodes) {
      // if it's not annotated node make an edge only when it's a leaf
      if(processed.noDependenciesDefined() && processed.getChildren().isEmpty()) {
        addEdge(processed, node);
      }
      // if it's annotated and there is dependency add an edge
      if(!processed.noDependenciesDefined() && checkDependency(processed, node)) {
        addEdge(processed, node);
      }
    }

    // if no dependency found add as child of the root
    if(node.getParents().isEmpty()) {
      log.debug("No dependencies found for node: " + node.getName() + " - adding as child of the root");
      addEdge(root, node);
    }
  }


  private boolean checkDependency(UpgradeNode processed, UpgradeNode node) {
    // processed writes intersection with any category of node
    SetView<String> view1 =
        Sets.intersection(
          processed.getModifies(),
          Stream.concat(node.getModifies().stream(), node.getReads().stream()).collect(Collectors.toSet()));

    // processed reads intersection with writes of node
    SetView<String> view2 =
        Sets.intersection(
          processed.getReads(),
          node.getModifies());

    if (view1.isEmpty() && view2.isEmpty()) {
      log.debug("No intersection found between potential parent: " + processed.getName() + " and node: " + node.getName());
      return false;
    }

    Set<String> allMatches = new HashSet<>(view1);
    allMatches.addAll(view2);
    log.debug("Node: " + node.getName() + " depends on " + processed.getName() + " because of entities: "
        + Arrays.toString(allMatches.toArray()));
    return true;

  }


  private void handleNotAnnotatedNode(List<UpgradeNode> processedNodes, UpgradeNode node, UpgradeNode root) {
    // if nothing has been processed add node as a child of a root
    if(processedNodes.isEmpty()) {
      addEdge(root, node);
      return;
    }

    // else add it a child of all leafs
    for(UpgradeNode processed : processedNodes) {
      if(processed.getChildren().isEmpty()) {
        addEdge(processed, node);
      }
    }
  }


  private void addEdge(UpgradeNode parent, UpgradeNode child) {
    parent.getChildren().add(child);
    child.getParents().add(parent);
  }


  List<UpgradeNode> produceNodes(List<UpgradeStep> upgradesToApply) {
    return upgradesToApply.stream().map(upg -> {
      Set<String> modifies, reads;
      if (upg.getClass().isAnnotationPresent(UpgradeModifies.class)) {
        UpgradeModifies annotation = upg.getClass().getAnnotation(UpgradeModifies.class);
        modifies = new HashSet<>(Arrays.asList(annotation.value()));
      } else {
        modifies = new HashSet<>();
      }
      if (upg.getClass().isAnnotationPresent(UpgradeReads.class)) {
        UpgradeReads annotation = upg.getClass().getAnnotation(UpgradeReads.class);
        reads = new HashSet<>(Arrays.asList(annotation.value()));
      } else {
        reads = new HashSet<>();
      }
      return new UpgradeNode(upg.getClass().getSimpleName(), upg.getClass().getAnnotation(Sequence.class).value(), reads, modifies);

    }).sorted(Comparator.comparing(UpgradeNode::getSequence)).collect(Collectors.toList());
  }

  void logGraph(UpgradeNode node) {
    logGraph(node, new HashSet<UpgradeNode>());
  }

  private void logGraph(UpgradeNode node, Set<UpgradeNode> visited) {
    if(!visited.contains(node)) {
      log.info(node.toString());
      visited.add(node);
      for(UpgradeNode child : node.children) {
        logGraph(child);
      }
    }
  }

  public static class UpgradeNode {
    final String name;
    final long sequence;
    final Set<String> reads;
    final Set<String> modifies;
    final Set<UpgradeNode> children = new HashSet<>();
    final Set<UpgradeNode> parents = new HashSet<>();

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
      return "UpgradeNode [name=" + name + ", sequence=" + sequence + ", reads=" + reads + ", modifies=" + modifies + ", children="
          + nodeListToStringOfNames(children) + ", parents=" + nodeListToStringOfNames(parents) + "]";
    }

    private String nodeListToStringOfNames(Collection<UpgradeNode> nodes) {
      return nodes.stream()
          .map(n -> n.getName())
          .collect(Collectors.joining(", "));
    }

  }

}
