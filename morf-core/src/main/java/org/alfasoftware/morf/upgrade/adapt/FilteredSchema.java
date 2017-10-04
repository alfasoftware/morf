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

import static com.google.common.base.Predicates.not;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * A {@link Schema} adapted from a base schema by selective exclusion (i.e. filtering
 * out) of specified tables.  Views are excluded.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class FilteredSchema extends TableSetSchema {

  /**
   * Construct a new {@link FilteredSchema} identical to the <var>baseSchema</var>
   * in all respects save the removal of the named tables. Note that tables are
   * removed regardless of the case of their names.
   *
   * @param baseSchema base schema to adapt.
   * @param removedTables names of tables to remove.
   */
  public FilteredSchema(final Schema baseSchema, final String... removedTables) {
    super(Collections2.filter(baseSchema.tables(), not(new Predicate<Table>() {
      @Override
      public boolean apply(Table table) {
        // String.CASE_INSENSITIVE_ORDER lets you use case-insensitive .contains(Object)
        Set<String> caseInsensitiveSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Collections.addAll(caseInsensitiveSet, removedTables);
        return caseInsensitiveSet.contains(table.getName());
      }
    })));
  }
}
