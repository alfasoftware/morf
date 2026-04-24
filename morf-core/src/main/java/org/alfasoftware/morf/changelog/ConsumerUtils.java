package org.alfasoftware.morf.changelog;

import java.io.PrintWriter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

public class ConsumerUtils {

  /**
   * Line width to apply wrapping at.
   */
  private static final int LINE_LENGTH = 132;;
  private final PrintWriter outputStream;

  public ConsumerUtils(PrintWriter outputStream) {
    this.outputStream = outputStream;
  }

  public void writeWrapped(String text) {
    String output = "";
    // Handle the case of multiple lines
    if (text.contains(System.lineSeparator())) {
      for (String line : text.split(System.lineSeparator())) {
        writeWrapped(line);
      }
      return;
    }

    // Write anything below the wrapping limit
    if (text.length() < LINE_LENGTH) {
      output = text;
      outputStream.println(output);
      return;
    }

    // Measure the indent to use on the split lines
    int indent = 0;
    while (indent < text.length() && text.charAt(indent) == ' ') {
      indent++;
    }
    indent += 2;

    // Split the line, preserving the indent on new lines
    final String firstLineIndent = text.substring(0, indent);
    final String lineSeparator = System.lineSeparator() + StringUtils.repeat(" ", indent);
    output = firstLineIndent + WordUtils.wrap(text.substring(indent), LINE_LENGTH - indent, lineSeparator, false);
    outputStream.println(output);
  }

}
