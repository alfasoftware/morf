package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.BasicNode;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.Colour;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.LayoutFormat;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.Node;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.PrintableGraph;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * All tests of {@link DrawIOGraphPrinter}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestDrawIOGraphPrinter {

  @Mock
  private GraphBasedUpgradeNode root, node2, node3;

  @Mock
  private PrintableGraph<Node> graph;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void colorShouldBeAlwaysSameForSameNode() {
    // given
    BasicNode node = new BasicNode("x", 1);

    // when
    Colour c1 = node.getColour();
    Colour c2 = node.getColour();

    // then
    assertEquals(c1.hex, c2.hex);
    assertEquals(c1.whiteText, c2.whiteText);
  }


  @Test
  public void testDefaultPrintMethod() {
    // given
    DrawIOGraphPrinter printer = spy(DrawIOGraphPrinter.class);

    // when
    printer.print(graph);

    // then
    verify(printer).print(ArgumentMatchers.<PrintableGraph<Node>>any(), eq(LayoutFormat.HORIZONTAL_TREE));
  }
}

