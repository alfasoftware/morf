package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.alfasoftware.morf.upgrade.ParallelUpgrade.UpgradeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestParallelUpgrade {

  private static final Log log = LogFactory.getLog(ParallelUpgrade.class);

  @Test
  public void testGraphBuilding() {
    //given
    ParallelUpgrade parallelUpgrade = new ParallelUpgrade(null, null, null, null);
    List<UpgradeStep> upgradesToApply = Lists.newArrayList(
      new UpgradeStep10(),
      new UpgradeStep13(),
      new UpgradeStep12(),
      new UpgradeStep11(),
      new UpgradeStep1(),
      new UpgradeStep2(),
      new UpgradeStep5(),
      new UpgradeStep4(),
      new UpgradeStep21(),
      new UpgradeStep20(),
      new UpgradeStep19(),
      new UpgradeStep18(),
      new UpgradeStep3(),
      new UpgradeStep9(),
      new UpgradeStep7(),
      new UpgradeStep8(),
      new UpgradeStep14(),
      new UpgradeStep15(),
      new UpgradeStep16(),
      new UpgradeStep17(),
      new UpgradeStep6());

    //when
    List<UpgradeNode> nodes = parallelUpgrade.produceNodes(upgradesToApply);
    UpgradeNode root = parallelUpgrade.prepareGraph(nodes);

    //then
    assertGraph(root, expectedNodes());
    parallelUpgrade.logGraph(root);
  }

  private void assertGraph(UpgradeNode root, Set<UpgradeNode> expectedNodes) {
    boolean fail = false;
    Set<UpgradeNode> actualNodes = readAllNodes(root);

    // Check if all expectedNodes exist and have correct edges
    for(UpgradeNode expectedNode : expectedNodes) {
      Optional<UpgradeNode> foundNode = actualNodes.stream().filter(node -> node.getName().equals(expectedNode.getName())).findFirst();
      if(foundNode.isPresent()) {
        if(!foundNode.get().getChildren().equals(expectedNode.getChildren())) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different children than actual node:\n" + foundNode.get());
        }
        if(!foundNode.get().getParents().equals(expectedNode.getParents())) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different parents than actual node:\n" + foundNode.get());
        }
        if(!foundNode.get().getReads().equals(expectedNode.getReads())) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different read tables than actual node:\n" + foundNode.get());
        }
        if(!foundNode.get().getModifies().equals(expectedNode.getModifies())) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different modified tables than actual node:\n" + foundNode.get());
        }
        if(foundNode.get().isRoot() != expectedNode.isRoot()) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different root property than actual node:\n" + foundNode.get());
        }
        if(foundNode.get().getSequence() != expectedNode.getSequence()) {
          fail = true;
          log.error("Expected node:\n" + expectedNode + "\nhas different sequence than actual node:\n" + foundNode.get());
        }
      }
      else {
        fail = true;
        log.error("Expected node: " + expectedNode + "doesn't exist.");
      }
    }


    // Check if all actualNodes are expected
    for(UpgradeNode actualNode : actualNodes) {
      Optional<UpgradeNode> foundNode = expectedNodes.stream().filter(node -> node.getName().equals(actualNode.getName())).findFirst();
      if (!foundNode.isPresent()) {
        fail = true;
        log.error("Actual node:\n" + actualNode + "\nis not expected.");
      }
    }

    assertFalse("Errors in the graph, see above", fail);
  }

  private Set<UpgradeNode> readAllNodes(UpgradeNode root) {
    Set<UpgradeNode> nodes = new HashSet<>();
    visitNode(root, nodes);
    return nodes;
  }

  private void visitNode(UpgradeNode node, Set<UpgradeNode> visitedNodes) {
    if(!visitedNodes.contains(node)) {
      visitedNodes.add(node);
      node.getChildren().stream().forEach(child -> visitNode(child, visitedNodes));
    }
  }

  private void addEdge(UpgradeNode parent, UpgradeNode child) {
    parent.getChildren().add(child);
    child.getParents().add(parent);
  }

  private Set<UpgradeNode> expectedNodes() {
    UpgradeNode r = new UpgradeNode("root", 0, Sets.newHashSet(), Sets.newHashSet());

    UpgradeNode n1 = new UpgradeNode("UpgradeStep1", 1, Sets.newHashSet(), Sets.newHashSet("t1"));
    addEdge(r, n1);

    UpgradeNode n2 = new UpgradeNode("UpgradeStep2", 2, Sets.newHashSet(), Sets.newHashSet("t1"));
    addEdge(n1, n2);

    UpgradeNode n3 = new UpgradeNode("UpgradeStep3", 3, Sets.newHashSet("t1"), Sets.newHashSet());
    addEdge(n1, n3);
    addEdge(n2, n3);

    UpgradeNode n4 = new UpgradeNode("UpgradeStep4", 4, Sets.newHashSet("t1"), Sets.newHashSet());
    addEdge(n1, n4);
    addEdge(n2, n4);

    UpgradeNode n5 = new UpgradeNode("UpgradeStep5", 5, Sets.newHashSet(), Sets.newHashSet("t2"));
    addEdge(r, n5);

    UpgradeNode n6 = new UpgradeNode("UpgradeStep6", 6, Sets.newHashSet(), Sets.newHashSet("t1", "t2"));
    addEdge(n1, n6);
    addEdge(n2, n6);
    addEdge(n3, n6);
    addEdge(n4, n6);
    addEdge(n5, n6);

    UpgradeNode n7 = new UpgradeNode("UpgradeStep7", 7, Sets.newHashSet("t1", "t2"), Sets.newHashSet());
    addEdge(n1, n7);
    addEdge(n2, n7);
    addEdge(n5, n7);
    addEdge(n6, n7);

    UpgradeNode n8 = new UpgradeNode("UpgradeStep8", 8, Sets.newHashSet("t2"), Sets.newHashSet());
    addEdge(n5, n8);
    addEdge(n6, n8);

    UpgradeNode n9 = new UpgradeNode("UpgradeStep9", 9, Sets.newHashSet("t2"), Sets.newHashSet("t1"));
    addEdge(n1, n9);
    addEdge(n2, n9);
    addEdge(n3, n9);
    addEdge(n4, n9);
    addEdge(n5, n9);
    addEdge(n6, n9);
    addEdge(n7, n9);

    UpgradeNode n10 = new UpgradeNode("UpgradeStep10", 10, Sets.newHashSet(), Sets.newHashSet());
    addEdge(n8, n10);
    addEdge(n9, n10);

    UpgradeNode n11 = new UpgradeNode("UpgradeStep11", 11, Sets.newHashSet(), Sets.newHashSet());
    addEdge(n10, n11);

    UpgradeNode n12 = new UpgradeNode("UpgradeStep12", 12, Sets.newHashSet(), Sets.newHashSet("t3"));
    addEdge(n11, n12);

    UpgradeNode n13 = new UpgradeNode("UpgradeStep13", 13, Sets.newHashSet("t3"), Sets.newHashSet());
    addEdge(n12, n13);

    UpgradeNode n14 = new UpgradeNode("UpgradeStep14", 14, Sets.newHashSet(), Sets.newHashSet("t3"));
    addEdge(n12, n14);
    addEdge(n13, n14);

    UpgradeNode n15 = new UpgradeNode("UpgradeStep15", 15, Sets.newHashSet("t5"), Sets.newHashSet());
    addEdge(n11, n15);

    UpgradeNode n16 = new UpgradeNode("UpgradeStep16", 16, Sets.newHashSet(), Sets.newHashSet("t4"));
    addEdge(n11, n16);

    UpgradeNode n17 = new UpgradeNode("UpgradeStep17", 17, Sets.newHashSet(), Sets.newHashSet("t6"));
    addEdge(n11, n17);

    UpgradeNode n18 = new UpgradeNode("UpgradeStep18", 18, Sets.newHashSet(), Sets.newHashSet("t6"));
    addEdge(n17, n18);

    UpgradeNode n19 = new UpgradeNode("UpgradeStep19", 19, Sets.newHashSet(), Sets.newHashSet("t7"));
    addEdge(n11, n19);

    UpgradeNode n20 = new UpgradeNode("UpgradeStep20", 20, Sets.newHashSet(), Sets.newHashSet());
    addEdge(n14, n20);
    addEdge(n15, n20);
    addEdge(n16, n20);
    addEdge(n18, n20);
    addEdge(n19, n20);

    UpgradeNode n21 = new UpgradeNode("UpgradeStep21", 21, Sets.newHashSet(), Sets.newHashSet("t3"));
    addEdge(n12, n21);
    addEdge(n13, n21);
    addEdge(n14, n21);
    addEdge(n20, n21);

    return Sets.newHashSet(r, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16, n17, n18, n19, n20, n21);
  }
}

@Sequence(1)
@UpgradeModifies("t1")
class UpgradeStep1 implements UpgradeStep {

  @Override
  public String getJiraId() {
    return null;
  }

  @Override
  public String getDescription() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
  }
}

@Sequence(2)
@UpgradeModifies("t1")
class UpgradeStep2 extends UpgradeStep1{}

@Sequence(3)
@UpgradeReads("t1")
class UpgradeStep3 extends UpgradeStep1{}

@Sequence(4)
@UpgradeReads("t1")
class UpgradeStep4 extends UpgradeStep1{}

@Sequence(5)
@UpgradeModifies("t2")
class UpgradeStep5 extends UpgradeStep1{}

@Sequence(6)
@UpgradeModifies({"t1", "t2"})
class UpgradeStep6 extends UpgradeStep1{}

@Sequence(7)
@UpgradeReads({"t1", "t2"})
class UpgradeStep7 extends UpgradeStep1{}

@Sequence(8)
@UpgradeReads("t2")
class UpgradeStep8 extends UpgradeStep1{}

@Sequence(9)
@UpgradeModifies("t1")
@UpgradeReads("t2")
class UpgradeStep9 extends UpgradeStep1{}

@Sequence(10)
class UpgradeStep10 extends UpgradeStep1{}

@Sequence(11)
class UpgradeStep11 extends UpgradeStep1{}

@Sequence(12)
@UpgradeModifies("t3")
class UpgradeStep12 extends UpgradeStep1{}

@Sequence(13)
@UpgradeReads("t3")
class UpgradeStep13 extends UpgradeStep1{}

@Sequence(14)
@UpgradeModifies("t3")
class UpgradeStep14 extends UpgradeStep1{}

@Sequence(15)
@UpgradeReads("t5")
class UpgradeStep15 extends UpgradeStep1{}

@Sequence(16)
@UpgradeModifies("t4")
class UpgradeStep16 extends UpgradeStep1{}

@Sequence(17)
@UpgradeModifies("t6")
class UpgradeStep17 extends UpgradeStep1{}

@Sequence(18)
@UpgradeModifies("t6")
class UpgradeStep18 extends UpgradeStep1{}

@Sequence(19)
@UpgradeModifies("t7")
class UpgradeStep19 extends UpgradeStep1{}

@Sequence(20)
class UpgradeStep20 extends UpgradeStep1{}

@Sequence(21)
@UpgradeModifies("t3")
class UpgradeStep21 extends UpgradeStep1{}
