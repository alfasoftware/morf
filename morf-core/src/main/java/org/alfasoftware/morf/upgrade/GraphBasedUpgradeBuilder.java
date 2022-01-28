package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeSchemaChangeVisitor.GraphBasedUpgradeSchemaChangeVisitorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

/**
 * Builds {@link GraphBasedUpgrade} instance which is ready to be executed.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class GraphBasedUpgradeBuilder {

  private static final Log LOG = LogFactory.getLog(GraphBasedUpgradeBuilder.class);

  private final GraphBasedUpgradeSchemaChangeVisitorFactory visitorFactory;
  private final GraphBasedUpgradeScriptGenerator scriptGenerator;
  private final Schema sourceSchema;
  private final SqlDialect sqlDialect;
  private final Table idTable;

  /**
   * Default constructor
   *
   * @param visitorFactory factory of {@link GraphBasedUpgradeSchemaChangeVisitor} instances
   * @param scriptGenerator creates pre- and post- upgrade statements
   * @param sourceSchema source schema
   * @param sqlDialect dialect to be used
   * @param idTable autonumber tracking table
   */
  public GraphBasedUpgradeBuilder(
      GraphBasedUpgradeSchemaChangeVisitorFactory visitorFactory,
      GraphBasedUpgradeScriptGenerator scriptGenerator,
      Schema sourceSchema,
      SqlDialect sqlDialect,
      Table idTable) {
    this.visitorFactory = visitorFactory;
    this.scriptGenerator = scriptGenerator;
    this.sourceSchema = sourceSchema;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
  }


  /**
   * Builds {@link GraphBasedUpgrade} instance based on the given {@link SchemaChangeSequence}.
   * @param schemaChangeSequence to be used to build a {@link GraphBasedUpgrade}
   * @return ready to execute {@link GraphBasedUpgrade} instance
   */
  public GraphBasedUpgrade prepareParallelUpgrade(SchemaChangeSequence schemaChangeSequence) {
    UpgradeStepToUpgradeNode mapper = new UpgradeStepToUpgradeNode(schemaChangeSequence.getUpgradeTableResolution());
    List<GraphBasedUpgradeNode> nodes = produceNodes(schemaChangeSequence.getUpgradeSteps(), mapper);

    GraphBasedUpgradeNode root = prepareGraph(nodes);
    GraphBasedUpgradeSchemaChangeVisitor visitor = visitorFactory.create(
      sourceSchema,
      sqlDialect,
      idTable,
      nodes.stream().collect(Collectors.toMap(GraphBasedUpgradeNode::getName, Function.identity())));

    List<String> preUpgStatements = scriptGenerator.generatePreUpgradeStatements();
    schemaChangeSequence.applyTo(visitor);
    List<String> postUpgStatements = scriptGenerator.generatePostUpgradeStatements();

    if(LOG.isDebugEnabled()) {
      logGraph(root);
    }

    return new GraphBasedUpgrade(root, preUpgStatements, postUpgStatements, nodes);
  }


  private List<GraphBasedUpgradeNode> produceNodes(List<UpgradeStep> upgradeSteps, UpgradeStepToUpgradeNode mapper) {
    return upgradeSteps.stream().
        map(mapper).
        sorted(Comparator.comparing(GraphBasedUpgradeNode::getSequence)).
        collect(Collectors.toList());
  }



  private GraphBasedUpgradeNode prepareGraph(List<GraphBasedUpgradeNode> nodes) {
    GraphBasedUpgradeNode root = new GraphBasedUpgradeNode("root", 0, new HashSet<>(), new HashSet<>());
    List<GraphBasedUpgradeNode> processedNodes = new ArrayList<>();
    for (GraphBasedUpgradeNode node : nodes) {
      if (node.noDependenciesDefined()) {
        handleNotAnnotatedNode(processedNodes, node, root);
      } else {
        handleAnnotatedNode(processedNodes, node, root);
      }
      processedNodes.add(node);
    }
    return root;
  }


  private void handleAnnotatedNode(List<GraphBasedUpgradeNode> processedNodes, GraphBasedUpgradeNode node, GraphBasedUpgradeNode root) {
    // if nothing has been processed add node as child of the root
    if(processedNodes.isEmpty()) {
      LOG.debug("Root empty, adding node: " + node.getName() + " as child of the root");
      addEdge(root, node);
      return;
    }

    Set<String> remainingReads = new HashSet<>(node.getReads());
    Set<String> remainingModifies = new HashSet<>(node.getModifies());
    Set<String> removeAtNextModify = new HashSet<>();


    for (int i = processedNodes.size() - 1; i >= 0; i--) {
      GraphBasedUpgradeNode processed = processedNodes.get(i);

      // if it's annotated
      if (!processed.noDependenciesDefined()) {
        analyzeDependency(processed, node, remainingReads, remainingModifies, removeAtNextModify);
      }

      // stop processing if there are no dependencies to consider anymore
      if(remainingModifies.isEmpty() && remainingReads.isEmpty()) {
        break;
      }

      // if not annotated check add edge only if current node has no parents
      if(processed.noDependenciesDefined() && node.getParents().isEmpty()) {
        addEdge(processed, node);
        LOG.info("Node: " + node.getName() + " depends on " + processed.getName() + " because it had no parent and the dependency is a not annotated leaf");
        break;
      }
    }

    // if no dependency found add as child of the root
    if(node.getParents().isEmpty()) {
      LOG.info("No dependencies found for node: " + node.getName() + " - adding as child of the root");
      addEdge(root, node);
    }
  }


  private void analyzeDependency(GraphBasedUpgradeNode processed, GraphBasedUpgradeNode node, Set<String> remainingReads, Set<String> remainingModifies, Set<String> removeAtNextModify) {
    // processed writes intersection with writes of the current node
    SetView<String> view1 = Sets.intersection(processed.getModifies(), remainingModifies);
    view1.stream().forEach(hit -> {
      if(removeAtNextModify.contains(hit)) {
        LOG.debug("Node: " + node.getName() + " does NOT depends on " + processed.getName()
            + " because of writes-writes (current-processed) intersection has been suppressed at: " + hit + ".");
        removeAtNextModify.remove(hit);
      } else {
        addEdge(processed, node);
        LOG.info("Node: " + node.getName() + " depends on " + processed.getName()
            + " because of writes-writes (current-processed) intersection at: " + hit + ".");
      }
      remainingModifies.remove(hit);
    });

    // processed writes intersection with reads of the current node
    SetView<String> view2 = Sets.intersection(processed.getModifies(), remainingReads);
    view2.stream().forEach(hit -> {
      addEdge(processed, node);
      remainingReads.remove(hit);
      LOG.info("Node: " + node.getName() + " depends on " + processed.getName()
      + " because of reads-writes (current-processed) intersection at: " + hit + ".");
    });

    // processed reads intersection with writes of the current node
    SetView<String> view3 = Sets.intersection(processed.getReads(), remainingModifies);
    view3.stream().forEach(hit -> {
      addEdge(processed, node);
      removeAtNextModify.add(hit);
      LOG.info("Node: " + node.getName() + " depends on " + processed.getName()
          + " because of writes-reads (current-processed) intersection at: " + hit + ". Adding this table to removeAtNextModify.");
    });

    if (!node.getParents().contains(processed)) {
      LOG.debug("No dependenciees found between potential parent: " + processed.getName() + " and node: " + node.getName());
    }
  }


  private void handleNotAnnotatedNode(List<GraphBasedUpgradeNode> processedNodes, GraphBasedUpgradeNode node, GraphBasedUpgradeNode root) {
    // if nothing has been processed add node as a child of a root
    if(processedNodes.isEmpty()) {
      addEdge(root, node);
      return;
    }

    // else add it as a child of all leafs
    for(GraphBasedUpgradeNode processed : processedNodes) {
      if(processed.getChildren().isEmpty() && !processed.isRoot() ) {
        addEdge(processed, node);
        LOG.info("Node (no annotations): " + node.getName() + " depends on " + processed.getName() + " because it is a leaf");
      }
    }
  }


  private void addEdge(GraphBasedUpgradeNode parent, GraphBasedUpgradeNode child) {
    parent.getChildren().add(child);
    child.getParents().add(parent);
  }


  void logGraph(GraphBasedUpgradeNode node) {
    traverseAndLog(node, new HashSet<GraphBasedUpgradeNode>());
  }

  private void traverseAndLog(GraphBasedUpgradeNode node, Set<GraphBasedUpgradeNode> visited) {
    if(!visited.contains(node)) {
      LOG.debug(node.toString());
      visited.add(node);
      for(GraphBasedUpgradeNode child : node.children) {
        traverseAndLog(child, visited);
      }
    }
  }


  private class UpgradeStepToUpgradeNode implements Function<UpgradeStep, GraphBasedUpgradeNode> {
    private final UpgradeTableResolution upgradeTableResolution;

    public UpgradeStepToUpgradeNode(UpgradeTableResolution upgradeTableResolution) {
      this.upgradeTableResolution = upgradeTableResolution;
    }


    @Override
    public GraphBasedUpgradeNode apply(UpgradeStep upg) {
      String upgradeName = upg.getClass().getSimpleName();

      Set<String> modifies, reads;
      if (upgradeTableResolution.isPortableSqlStatementUsed(upgradeName)) {
        // fallback to annotations
        LOG.debug("Due to use of PortableSqlStatement falling back to annotations for: " + upgradeName);

        if (upg.getClass().isAnnotationPresent(UpgradeModifies.class)) {
          UpgradeModifies annotation = upg.getClass().getAnnotation(UpgradeModifies.class);
          modifies = Arrays.stream(annotation.value()).map(s -> s.toUpperCase()).collect(Collectors.toSet());
        } else {
          modifies = new HashSet<>();
        }

        if (upg.getClass().isAnnotationPresent(UpgradeReads.class)) {
          UpgradeReads annotation = upg.getClass().getAnnotation(UpgradeReads.class);
          reads = Arrays.stream(annotation.value()).map(s -> s.toUpperCase()).collect(Collectors.toSet());
        } else {
          reads = new HashSet<>();
        }
      } else {
        modifies = upgradeTableResolution.getModifiedTables(upgradeName);
        reads = upgradeTableResolution.getReadTables(upgradeName);
      }
      return new GraphBasedUpgradeNode(upgradeName, upg.getClass().getAnnotation(Sequence.class).value(), reads, modifies);
    }
  }

}

