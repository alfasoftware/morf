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

package org.alfasoftware.morf.upgrade;

/**
 * Defines the contract for a class which can be sent human-readable
 * upgrade statements.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface HumanReadableStatementConsumer {

  /**
   * Indicates a version number has started.
   *
   * @param versionNumber the version number that is starting
   */
  public void versionStart(final String versionNumber);


  /**
   * Indicates an upgrade step is starting.
   *
   * @param name the name of the upgrade step
   * @param description the description of the upgrade step
   * @param jiraId the jira Id for the upgrade step
   */
  public void upgradeStepStart(final String name, final String description, String jiraId);


  /**
   * Indicates a schema change needs to be applied.
   *
   * @param description the description of the schema change
   */
  public void schemaChange(final String description);


  /**
   * Indicates the end of an upgrade step.
   *
   * @param name the name of the upgrade step which is ending
   */
  public void upgradeStepEnd(final String name);


  /**
   * Indicates a version number has ended.
   *
   * @param versionNumber the version number that is ending
   */
  public void versionEnd(final String versionNumber);


  /**
   * Indicates a data change which needs to be applied.
   *
   * @param description the description of the data change
   */
  public void dataChange(final String description);

}
