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

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Table;

import java.util.Collection;

/**
 * Provides a method by which to convert the string value on a {@link Record}
 * to a database-compatible value.  Generally implemented by a {@link SqlDialect}
 * and passed to {@link org.alfasoftware.morf.dataset.RecordHelper#joinRecordValues(Table, Record, Collection, String, RecordValueToDatabaseSafeStringConverter)}.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public interface RecordValueToDatabaseSafeStringConverter {

  /**
   * Converts a {@link Record}'s value to a safe string for inserting into a SQL
   * statement or bulk load operation. The code will replace nulls with the supplied
   * <code>valueForNull</code> and convert decimals into a database safe representation.
   *
   * @param column the column that the source value applies to
   * @param sourceValue the value of the column
   * @return a safe string representation of the column's value
   */
  public String recordValueToDatabaseSafeString(Column column, String sourceValue);
}
