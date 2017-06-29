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

package org.alfasoftware.morf.dataset;

import java.util.HashMap;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Table;


/**
 * Implements {@link Record} as a bean for testing.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class MockRecord implements Record {

  /**
   * Holds all the mocked values.
   */
  private final Map<String, String> values = new HashMap<String, String>();

  /**
   * @param table Meta data for the table to mock.
   * @param values Column values for the record.
   */
  public MockRecord(Table table, String... values) {
    super();

    if (table.columns().size() != values.length) {
      throw new IllegalArgumentException("Number of values must match the number of table columns");
    }

    int index = 0;
    for (Column column : table.columns()) {
      this.values.put(column.getName().toUpperCase(), values[index++]);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
   */
  @Override
  public String getValue(String name) {
    return values.get(name.toUpperCase());
  }


  /**
   * Suitable for JUnit test assertions.
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return values.values().toString();
  }
}
