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

import java.util.Collection;


/**
 * Defines configuration tables within a module which are not created via Domain classes,
 * and any upgrade steps on them.
 *
 * <p>This interface is used for tables which are
 * not represented by Domain classes.</p>
 *
 * <p>Note that not all tables specified via {@link TableContribution#tables()} must be
 * configuration tables. However, any names returned by the {@link #configurationTables()}
 * method must reference tables specified by {@link TableContribution#tables()}.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public interface ConfigurationTableContribution extends TableContribution {

  /**
   * @return All non-domain-class-derived configuration tables present in the module.
   *
   * <p><b>Note: This must be a subset of {@link TableContribution#tables()}.</b></p>
   */
  public Collection<String> configurationTables();
}
