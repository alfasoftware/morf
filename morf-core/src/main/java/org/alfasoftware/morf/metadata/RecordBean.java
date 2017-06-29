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

import java.util.HashMap;
import java.util.Map;

import org.alfasoftware.morf.dataset.Record;

/**
 * Bean implementation of a {@link Record}. Can be used to copy another {@link Record}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class RecordBean implements Record {

  private final Map<String, String> values;

  /**
   * Copy a {@link Record}.
   *
   * @param table Table of the Record to copy
   * @param record Record to copy.
   */
  public RecordBean(Table table, Record record) {
    this.values = new HashMap<String, String>(table.columns().size());
    for(Column column : table.columns()) {
      this.values.put(column.getName().toUpperCase(), record.getValue(column.getName()));
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
   */
  @Override
  public String getValue(String name) {
    return values.get(name.toUpperCase());
  }
}
