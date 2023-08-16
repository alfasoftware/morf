package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.BasicNode;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests of {@link GraphBasedUpgradeNodeDrawIOAdapter}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeNodeDrawIOAdapter {

  private GraphBasedUpgradeNodeDrawIOAdapter adapter;

  @Mock
  private GraphBasedUpgradeNode root, node2, node3;


  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(root.getName()).thenReturn("root");
    when(node2.getName()).thenReturn("node2");
    when(node3.getName()).thenReturn("node3");
    when(root.getSequence()).thenReturn(0L);
    when(node2.getSequence()).thenReturn(1L);
    when(node3.getSequence()).thenReturn(2L);
    when(node3.getParents()).thenReturn(Sets.newHashSet(node2));

    adapter = new GraphBasedUpgradeNodeDrawIOAdapter(Lists.newArrayList(node2, node3), root);
  }


  @SuppressWarnings("unchecked")
  @Test
  public void shouldReturnAllNodes() {
    assertThat(adapter.getNodes(), Matchers.containsInAnyOrder(
      Matchers.hasProperty("label", equalTo("root-0")),
      Matchers.hasProperty("label", equalTo("node2-1")),
      Matchers.hasProperty("label", equalTo("node3-2"))));
  }


  @Test
  public void shouldReturnParents() {
    // given
    BasicNode basicNode3 = new BasicNode("node3", 2L);

    // when then
    assertThat(adapter.getNodesThisNodeLinksTo(basicNode3), Matchers.contains (
      Matchers.hasProperty("label", equalTo("node2-1"))));
  }

}
