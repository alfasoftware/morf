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

package org.alfasoftware.morf.upgrade.adapt;

import java.util.Arrays;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * {@link Schema} which adapts an existing schema by overriding a single table definition.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TableOverrideSchema extends TableSetSchema {

  /**
   * Construct a {@link TableOverrideSchema} which is identical to <var>baseSchema</var>
   * save for its having <var>overridingTable</var> in place of its namesake.
   *
   * @param baseSchema base schema to adapt via a single table override.
   * @param overridingTable table to take the place of its namesake in <var>baseSchema</var>.
   */
  @SuppressWarnings("unchecked")
  public TableOverrideSchema(final Schema baseSchema, final Table overridingTable) {
    super(
      CollectionUtils.union(
        // all except the overridden table
        CollectionUtils.selectRejected(baseSchema.tables(), new Predicate() {
          @Override
          public boolean evaluate(Object table) {
            return ((Table)table).getName().toUpperCase().equals(overridingTable.getName().toUpperCase());
          }
        }),
        // plus the override
        Arrays.asList(new Table[] {overridingTable})
      ));
  }
}
