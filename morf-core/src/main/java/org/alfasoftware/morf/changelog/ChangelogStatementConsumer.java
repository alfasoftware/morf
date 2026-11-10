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

/**
 * Class to consume strings produced by the human readable statement generator and
 * passes them to the target output stream after applying word-wrapping logic.
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
class ChangelogStatementConsumer implements HumanReadableStatementConsumer {


  private final PrintWriter outputStream;
  private final ConsumerUtils utils;


  ChangelogStatementConsumer(PrintWriter outputStream) {
    this.outputStream = outputStream;
    utils = new ConsumerUtils(outputStream);
  }


  @Override
  public void versionStart(String versionNumber) {
    outputStream.println(versionNumber);
    outputStream.println(StringUtils.repeat("=", versionNumber.length()));
  }


  @Override
  public void upgradeStepStart(String name, String description, String jiraId) {
    utils.writeWrapped("* " + jiraId + ": " + description);
  }


  @Override
  public void schemaChange(String description) {
    utils.writeWrapped("  o " + description);
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
    utils.writeWrapped("  o " + description);
  }

}
