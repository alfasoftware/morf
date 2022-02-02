package org.alfasoftware.morf.upgrade;


import java.util.Set;

import com.google.common.graph.Graph;
import com.google.common.graph.MutableGraph;
import com.google.inject.ImplementedBy;

/**
 * Outputs a given {@link Graph} of {@link Node} objects in a format suitable for importing into Draw.io.
 * <br /><br />
 * To import the output into draw.io:
 * <ol>
 *  <li>Copy the output from the console into your clipboard</li>
 *  <li>Open the draw.io diagram you want to insert into, in your browser</li>
 *  <li>From the menu bar, select "Arrange > Insert > Advanced > CSV..."</li>
 *  <li>Replace the contents of the textbox with the copied output</li>
 *  <li>Hit "Import"</li>
 * </ol>
 * Note:
 * <ul>
 *  <li>Importing multiple times should not lead to duplicates, but it will remove any changes you have made involving this nodes.</li>
 *  <li>You should be able to import multiple different outputs into the same diagram. Though if the same class is present, it may behave strangely.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2020
 */
@ImplementedBy(DrawIOGraphPrinterImpl.class)
interface DrawIOGraphPrinter {

  /**
   * Prints the given graph to String instance in a format suitable for importing into Draw.io, using the
   * default layout (HORIZONTAL_TREE).
   * @param graph - {@link MutableGraph} of {@link Node}
   */
  default String print(PrintableGraph<Node> graph) {
    return print(graph, LayoutFormat.HORIZONTAL_TREE);
  }


  /**
   * Prints the given graph to String instance in a format suitable for importing into Draw.io
   * @param graph - {@link MutableGraph} of {@link Node}
   * @param layoutFormat - {@link LayoutFormat} - the specific layout format to use
   * @return
   */
  String print(PrintableGraph<Node> graph, LayoutFormat layoutFormat);


  /**
   * Abstract representation of a graph that {@link DrawIOGraphPrinter} can output.
   * <br /><br />
   * Allows us to abstract over the exact graph implementation through the use of Adapter classes.
   *
   * @see GuavaPrintableGraphAdapter
   * @see JGraphTPrintableGraphAdapter
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2020
   * @param <T>
   */
  interface PrintableGraph<T> {

    /**
     * @return the set of all nodes in the graph.
     */
    Set<T> getNodes();

    /**
     * @return the set of all nodes that are directly linked to by the given node.
     */
    Set<T> getNodesThisNodeLinksTo(T node);
  }


  /**
   * Enum of the supported draw.io layout formats.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2020
   */
  enum LayoutFormat {
    HORIZONTAL_TREE("horizontaltree"),
    HORIZONTAL_FLOW("horizontalflow"),
    VERTICAL_FLOW("verticalflow"),
    ORGANIC("organic");

    private final String label;

    /**
     * Constructor
     *
     * @param label of the layout
     */
    private LayoutFormat(String label) {
      this.label = label;
    }

    @Override
    public String toString() {
      return label;
    }
  }


  /**
   * Represents a Node in a Graph, that will be drawn as a box in draw.io,
   * with directed arrows representing the edges.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2020
   */
  static interface Node {

    /**
     * @return the String label to use for the box in draw.io.
     */
    public String getLabel();

    /**
     * @return the {@link Colour} of the box in draw.io.
     */
    public Colour getColour();

    /**
     *
     * @return node sequence number
     */
    public long getSequence();
  }


  /**
   * Implementation of Basic {@link Node} that simply has a given label.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2020
   */
  static class BasicNode implements Node {
    final String label;
    final long sequence;

    /**
     * Default constructor.
     *
     * @param label of this node
     * @param sequence of this node
     */
    public BasicNode(String label, long sequence) {
      this.label = label;
      this.sequence = sequence;
    }


    @Override
    public long getSequence() {
      return sequence;
    }


    @Override
    public String getLabel() {
      return label;
    }


    @Override
    public Colour getColour() {
      return Colour.generateColourForModule(label);
    }


    /**
     * Based on label only.
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (label == null ? 0 : label.hashCode());
      return result;
    }


    /**
     * Based on label only.
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      BasicNode other = (BasicNode) obj;
      if (label == null) {
        if (other.label != null) return false;
      } else if (!label.equals(other.label)) return false;
      return true;
    }


    @Override
    public String toString() {
      return label;
    }

  }


  /**
   * Represents a Colour for a box in the draw.io output.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2020
   */
  static class Colour {
    String hex;
    boolean whiteText;

    /**
     * @param input
     * @return colour based on given input
     */
    static Colour generateColourForModule(String input){
      int i = input.hashCode();

      int r = i>>16&0xFF;
      int g = i>>8&0xFF;
      int b = i&0xFF;

      Colour colour = new Colour();
      colour.hex = "#" + String.format("%02X", r) + String.format("%02X", g) + String.format("%02X", b);

      double luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;

      //Runs from 1 - 256 - Less than 128 = dark, so then use white text
      if(luma < 128) {
        colour.whiteText = true;
      }

      return colour;
    }
  }
}