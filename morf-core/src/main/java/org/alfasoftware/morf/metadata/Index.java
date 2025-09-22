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

package org.alfasoftware.morf.metadata;

import java.util.List;

import com.google.common.base.Joiner;

/**
 * Defines an index on a table.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface Index {

  /**
   * @return The index name.
   */
  public String getName();

  /**
   * @return The ordered list of column names in the index.
   */
  public List<String> columnNames();

  /**
   * @return True if the index is unique.
   */
  public boolean isUnique();

  /**
   * @return True if the index is globally partitioned
   */
  boolean isGlobalPartitioned();

  /**
   * @return True if the index is locally partitioned
   */
  boolean isLocalPartitioned();

  /**
   * Helper for {@link Object#toString()} implementations.
   *
   * @return String representation of the index.
   */
  public default String toStringHelper() {
    return new StringBuilder()
        .append("Index-").append(getName())
        .append("-").append(isUnique() ? "unique" : "")
        .append("-").append(Joiner.on(',').join(columnNames()))
        .toString();
  }
}
