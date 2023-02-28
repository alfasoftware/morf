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

package org.alfasoftware.morf.upgrade.additions;


import org.alfasoftware.morf.jdbc.ConnectionResources;

/**
 * Implementations provide SQL to be appended to the upgrade.
 *
 * <p>By contrast to Upgrade steps, which are run once, {@link UpgradeScriptAddition}s
 * are re-run at the end of each upgrade run. The primary use case is the re-generation
 * of user authorities in response to new objects created during the course of an upgrade.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public interface UpgradeScriptAddition {

  /**
   * Returns an ordered series of SQL statements to be appended to the upgrade SQL.
   * @param connectionResources the connection details
   * @return SQL statements.
   */
  Iterable<String> sql(ConnectionResources connectionResources);
}
