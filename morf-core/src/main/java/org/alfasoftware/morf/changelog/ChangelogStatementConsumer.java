/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.changelog;

import java.io.PrintWriter;

import org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.text.WordUtils;

/**
 * Class to consume strings produced by the human readable statement generator and
 * passes them to the target output stream after applying word-wrapping logic.
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
class ChangelogStatementConsumer implements HumanReadableStatementConsumer {

  /**
   * Line width to apply wrapping at.
   */
  private static final int LINE_LENGTH = 132;
  private final PrintWriter outputStream;


  ChangelogStatementConsumer(PrintWriter outputStream) {
    this.outputStream = outputStream;
  }


  /**
   * Writes one or more lines of text, applying line wrapping.
   */
  private void writeWrapped(final String text) {

    // Handle the case of multiple lines
    if (text.contains(System.lineSeparator())) {
      for (String line : text.split(System.lineSeparator())) {
        writeWrapped(line);
      }
      return;
    }

    // Write anything below the wrapping limit
    if (text.length() < LINE_LENGTH) {
      outputStream.println(text);
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
    outputStream.println(firstLineIndent + WordUtils.wrap(text.substring(indent), LINE_LENGTH - indent, lineSeparator, false));
  }


  @Override
  public void versionStart(String versionNumber) {
    outputStream.println(versionNumber);
    outputStream.println(StringUtils.repeat("=", versionNumber.length()));
  }


  @Override
  public void upgradeStepStart(String name, String description, String jiraId) {
    writeWrapped("* " + jiraId + ": " + description);
  }


  @Override
  public void schemaChange(String description) {
    writeWrapped("  o " + description);
  }


  @Override
  public void upgradeStepEnd(String name) {
    outputStream.println();
  }


  @Override
  public void versionEnd(String versionNumber) {
    // nothing to write
  }


  @Override
  public void dataChange(final String description) {
    writeWrapped("  o " + description);
  }

}
