package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.BasicNode;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.LayoutFormat;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.Node;
import org.alfasoftware.morf.upgrade.DrawIOGraphPrinter.PrintableGraph;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Sets;

/**
 * All tests of {@link DrawIOGraphPrinterImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestDrawIOGraphPrinterImpl {

  @Mock
  private PrintableGraph<Node> graph;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void testPrint() {
    // given
    DrawIOGraphPrinterImpl printer = new DrawIOGraphPrinterImpl();

    BasicNode n1 = new BasicNode("n1", 1L);
    BasicNode n2 = new BasicNode("n2", 2L);
    BasicNode n3 = new BasicNode("n3", 3L);

    when(graph.getNodes()).thenReturn(Sets.newHashSet(n1,n2,n3));
    when(graph.getNodesThisNodeLinksTo(n1)).thenReturn(new HashSet<>());
    when(graph.getNodesThisNodeLinksTo(n2)).thenReturn(new HashSet<>());
    when(graph.getNodesThisNodeLinksTo(n3)).thenReturn(Sets.newHashSet(n1,n2));

    // when
    String result = printer.print(graph, LayoutFormat.HORIZONTAL_FLOW);

    // then
    String expected = "\n" +
        "\n" +
        "### COPY LINES BELOW THIS (EXCLUDING THIS LINE) ###\n" +
        "\n" +
        "# label: %label%\n" +
        "# style: whiteSpace=wrap;html=1;rounded=1;fillColor=%fill%;strokeColor=#000000;fontColor=%fontColor%;\n" +
        "# namespace: csvimport-\n" +
        "# connect: {\"from\": \"refs\", \"to\": \"id\", \"style\": \"sharp=1;fontSize=14;\"}\n" +
        "# width: auto\n" +
        "# height: auto\n" +
        "# padding: 20\n" +
        "# ignore: fill,fontColor,id,refs\n" +
        "# layout: horizontalflow\n" +
        "label,fill,fontColor,id,refs\n" +
        "n1,#000D83,#FFFFFF,n1,\n" +
        "n2,#000D84,#FFFFFF,n2,\n" +
        "n3,#000D85,#FFFFFF,n3,\"n1,n2\"\n" +
        "\n" +
        "### COPY LINES ABOVE THIS (EXCLUDING THIS LINE) ###\n";

    assertEquals(expected, result);
  }
}

