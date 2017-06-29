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

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * {@link Schema} which adapts an existing schema by adding new
 * tables to it.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class AugmentedSchema extends TableSetSchema {

  /**
   * Construct an {@link AugmentedSchema} which is identical to
   * <var>baseSchema</var> save for the addition of several new tables.
   *
   * @param baseSchema the schema to adapt through the addition of new tables
   * @param newTables to add over and above those in baseSchema.
   */
  @SuppressWarnings("unchecked")
  public AugmentedSchema(Schema baseSchema, Table... newTables) {
    super(CollectionUtils.union(baseSchema.tables(), Arrays.asList(newTables)));
  }
}
