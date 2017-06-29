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
 * Defines the metadata associated with a database table column.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface ColumnType {

  /**
   * @return the data type of the column.
   */
  public DataType getType();

  /**
   * @return The width (characters or digits) of the column. For numeric columns this is
   * akin to the precision, i.e. the total number of digits used to store the column value.
   */
  public int getWidth();

  /**
   * @return The number of decimal places used by the column (if applicable).
   */
  public int getScale();

  /**
   * @return True if the column may be set to null, False otherwise.
   */
  public boolean isNullable();

}
