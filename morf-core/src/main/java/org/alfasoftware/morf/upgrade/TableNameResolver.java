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
 * Determines the name for "active" version of the table for subsequent changes.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface TableNameResolver {

  /**
   * Provides the current name for a table.
   *
   * <p><strong>Table names used via this API are case insensitive.</strong></p>
   *
   * @param tableName Name of the table for which the current name is required.
   * @return The current name of the active version of the table.
   */
  public String activeNameFor(String tableName);

}
