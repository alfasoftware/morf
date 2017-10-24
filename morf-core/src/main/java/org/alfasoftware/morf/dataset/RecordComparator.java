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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Table;

/**
 * Compares {@link Record} objects using a column order. The value comparisons are done using the type-converted values.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class RecordComparator implements Comparator<Record> {

  private final List<Column> columnSortOrder = new ArrayList<>();

  /**
   * @param table The table being compared.
   * @param columnOrder The column order to apply.
   */
  public RecordComparator(Table table, String... columnOrder) {
    outer: for (String columnName : columnOrder) {
      for (Column column : table.columns()) {
        if (column.getName().equals(columnName)) {
          columnSortOrder.add(column);
          continue outer;
        }
      }

      throw new IllegalArgumentException("No such column ["+columnName+"]");
    }
  }


  /**
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public int compare(Record o1, Record o2) {

    for (Column column : columnSortOrder) {

      Comparable value1 = RecordHelper.convertToComparableType(column, o1);
      Comparable value2 = RecordHelper.convertToComparableType(column, o2);

      if (value1 == null && value2 == null) {
        continue; // next column
      }

      // nulls first
      if (value1 == null ) return -1;
      if (value2 == null ) return  1;

      if (!value1.getClass().equals(value2.getClass())) {
        throw new IllegalStateException("Types do not match: ["+value1 +"] ["+value2+"]");
      }

      int order = value1.compareTo(value2);

      if (order != 0) {
        return order;
      }
    }

    // if we get all the way out, the rows are identical for this comparison
    return 0;
  }

}
