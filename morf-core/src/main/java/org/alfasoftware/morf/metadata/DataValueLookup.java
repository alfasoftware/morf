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


/**
 * Defines a set of values for columns.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface DataValueLookup {

  /**
   * Returns the value of the column with the given name.
   *
   * <p>Implementations should ensure that values are formatted appropriately
   * for the data type of the column as follows:</p>
   *
   * <ul>
   * <li>{@link DataType#STRING} is represented as the string value.</li>
   * <li>{@link DataType#DECIMAL} must be represented as a string using "." as the decimal separator and no thousand separator.</li>
   * <li>{@link DataType#BOOLEAN} must be represented as "1" for true and "0" for false.</li>
   * <li>{@link DataType#DATE} must be represented in the format "YYYY-MM-DD".</li>
   * <li>{@link DataType#BLOB} must be represented in base-64 encoding.</li>
   * </ul>
   *
   * <p>Importantly, implementations of this should be case insensitive to column names (e.g.
   * <code>getValue("agreementNumber")</code> is equivalent to <code>getValue("AGREEMENTNUMBER")</code>)</p>
   *
   * @param name of the column.
   * @return the value of the named column.
   */
  public String getValue(String name);
}
