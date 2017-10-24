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

package org.alfasoftware.morf.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.alfasoftware.morf.metadata.Column;

import com.google.common.collect.Iterables;

/**
 * Identifies the order in which a set of {@link Column}s appear in a {@link ResultSet}.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public final class ResultSetMetadataSorter {

  /**
   * Creates a copy of an {@link Iterable} of columns, where the copy is sorted such that the
   * columns appear in the same order as they do in the supplied {@link ResultSet}.
   *
   * @param columns Columns expected.
   * @param resultSet The result set containing the values.
   * @return The sorted columns.
   */
  public static Collection<Column> sortedCopy(Iterable<Column> columns, ResultSet resultSet) {
    Column[] result = new Column[Iterables.size(columns)];
    for (Column column : columns) {
      try {
        result[resultSet.findColumn(column.getName()) - 1] = column;
      } catch(SQLException ex) {
        throw new IllegalStateException("Could not retrieve column [" + column.getName() + "]", ex);
      }
    }
    return Collections.unmodifiableCollection(Arrays.asList(result));
  }
}