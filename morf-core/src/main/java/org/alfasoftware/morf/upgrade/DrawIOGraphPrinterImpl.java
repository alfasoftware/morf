package org.alfasoftware.morf.upgrade;



import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.graph.Graph;
import com.google.common.graph.MutableGraph;

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
class DrawIOGraphPrinterImpl implements DrawIOGraphPrinter {

  /**
   * Prints the given graph to the String instance in a format suitable for importing into Draw.io
   * @param graph - {@link MutableGraph} of {@link Node}
   * @param layoutFormat - {@link LayoutFormat} - the specific layout format to use
   */
  @Override
  public String print(DrawIOGraphPrinter.PrintableGraph<DrawIOGraphPrinter.Node> graph, DrawIOGraphPrinter.LayoutFormat layoutFormat) {
    List<String> outputLines = buildOutputLines(graph);

    // Sort the lines so that draw.io is more likely to group them together
    Collections.sort(outputLines);

    /*
     * Output to console
     */
    StringBuilder sb = new StringBuilder();

    sb.append("\n\n### COPY LINES BELOW THIS (EXCLUDING THIS LINE) ###\n");

    sb.append("\n# label: %label%");
    sb.append("\n# style: whiteSpace=wrap;html=1;rounded=1;fillColor=%fill%;strokeColor=#000000;fontColor=%fontColor%;");
    sb.append("\n# namespace: csvimport-");
    sb.append("\n# connect: {\"from\": \"refs\", \"to\": \"id\", \"style\": \"sharp=1;fontSize=14;\"}");
    sb.append("\n# width: auto");
    sb.append("\n# height: auto");
    sb.append("\n# padding: 20");
    sb.append("\n# ignore: fill,fontColor,id,refs");

    if(layoutFormat != DrawIOGraphPrinter.LayoutFormat.HORIZONTAL_FLOW && layoutFormat != DrawIOGraphPrinter.LayoutFormat.VERTICAL_FLOW) {
      sb.append("\n# nodespacing: 200");
      sb.append("\n# levelspacing: 100");
      sb.append("\n# edgespacing: 60");
    }
    sb.append("\n# layout: " + layoutFormat);
    sb.append("\nlabel,fill,fontColor,id,refs");

    for (String outputLine : outputLines) {
      sb.append("\n");
      sb.append(outputLine);
    }

    sb.append("\n\n### COPY LINES ABOVE THIS (EXCLUDING THIS LINE) ###\n");

    return sb.toString();
  }


  private List<String> buildOutputLines(DrawIOGraphPrinter.PrintableGraph<DrawIOGraphPrinter.Node> graph) {
    List<String> outputLines = new ArrayList<>();

    for (DrawIOGraphPrinter.Node currentNode : graph.getNodes()) {
      StringBuilder outputLine = new StringBuilder();

      /*
       * Label
       */
      outputLine.append(currentNode.getLabel());

      /*
       * Fill & font colour
       */
      outputLine.append(",");
      outputLine.append(currentNode.getColour().hex);

      outputLine.append(",");
      if(currentNode.getColour().whiteText) {
        outputLine.append("#FFFFFF");
      }
      else {
        outputLine.append("#000000");
      }


      /*
       * Id
       */
      outputLine.append(",");
      outputLine.append(currentNode.getLabel());

      /*
       * Refs
       */
      outputLine.append(",");

      boolean skipFirstComma = true;

      Set<DrawIOGraphPrinter.Node> typesThisNodeLinksTo = graph.getNodesThisNodeLinksTo(currentNode);

      if (!typesThisNodeLinksTo.isEmpty()) {

        outputLine.append("\"");

        for (DrawIOGraphPrinter.Node typeThisNodeLinksTo : typesThisNodeLinksTo) {

          if (skipFirstComma) {
            skipFirstComma = false;
          } else {
            outputLine.append(",");
          }

          outputLine.append(typeThisNodeLinksTo.getLabel());
        }

        outputLine.append("\"");
      }

      outputLines.add(outputLine.toString());
    }

    return outputLines;
  }

}