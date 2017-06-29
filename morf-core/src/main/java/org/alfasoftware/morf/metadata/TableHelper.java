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

import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for manipulating and querying table definitions.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class TableHelper {

  /**
   * Finds a column with the specified name in the specified table.
   *
   * @param table the table to find the column in
   * @param columnName the name of the column to look for (case insensitive)
   * @return the matching column, or null if one cannot be found
   */
  public static Column columnWithName(Table table, String columnName) {
    for (Column currentColumn : table.columns()) {
      if (currentColumn.getName().equalsIgnoreCase(columnName)) {
        return currentColumn;
      }
    }

    return null;
  }


  /**
   * Builds a list of column names from the column definitions and optionally
   * includes the ID and VERSION fields at the beginning.
   *
   * @param table the table to get the list of column names for
   * @return a list of column names for the table
   */
  public static List<String> buildColumnNameList(Table table) {
    List<String> result = new ArrayList<String>();

    for (Column currentColumn : table.columns()) {
      result.add(currentColumn.getName());
    }

    return result;
  }


  /**
   * Finds an index with the specified name on the specified table.
   *
   * @param table the table to find the index in.
   * @param indexName the name of the index to look for
   * @return the matching index, or null if one cannot be found
   */
  public static Index indexWithName(Table table, String indexName) {
    for (Index currentIndex : table.indexes()) {
      if (currentIndex.getName().equalsIgnoreCase(indexName)) {
        return currentIndex;
      }
    }

    return null;
  }
}
