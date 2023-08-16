package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.Lists;

/**
 * All tests of {@link GraphBasedUpgradeNode}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeNode {

  private GraphBasedUpgradeNode node, root;

  @Mock
  private GraphBasedUpgradeNode node2, node3;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    node = new GraphBasedUpgradeNode("name", 11, Sets.newSet("x"), Sets.newSet("y"), true);
    node.addAllUpgradeStatements(Lists.newArrayList("z"));
    node.addUpgradeStatements("q");
    node.getChildren().add(node2);
    node.getParents().add(node3);

    root = new GraphBasedUpgradeNode("name", 0, Sets.newSet(), Sets.newSet(), false);
  }


  @Test
  public void testProperties() {
    assertEquals("name", node.getName());
    assertEquals(11, node.getSequence());
    assertThat(node.getReads(), Matchers.containsInAnyOrder("x"));
    assertThat(node.getModifies(), Matchers.containsInAnyOrder("y"));
    assertTrue(node.requiresExclusiveExecution());
    assertFalse(node.isRoot());
    assertThat(node.getUpgradeStatements(), Matchers.contains("z", "q"));
    assertThat(node.getChildren(), Matchers.contains(node2));
    assertThat(node.getParents(), Matchers.contains(node3));

    assertTrue(root.isRoot());
    assertTrue(root.requiresExclusiveExecution());
  }


  @Test
  public void testToString() {
    assertEquals("UpgradeNode [name=name, sequence=11, reads=[x], modifies=[y], root=false, children=null, parents=null]",
      node.toString());
  }
}
