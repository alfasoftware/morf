package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

/**
 * All tests of {@link GraphBasedUpgradeNode}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgrade {

  private GraphBasedUpgrade upg;

  @Mock
  private GraphBasedUpgradeNode node2;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    upg = new GraphBasedUpgrade(node2, Lists.newArrayList("z"), Lists.newArrayList("x"), 5);
  }


  @Test
  public void testProperties() {
    assertEquals(5, upg.getNumberOfNodes());
    assertThat(upg.getPostUpgradeStatements(), Matchers.containsInAnyOrder("x"));
    assertThat(upg.getPreUpgradeStatements(), Matchers.containsInAnyOrder("z"));
    assertEquals(node2, upg.getRoot());
  }
}
