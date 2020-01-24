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
 * Defines a database table column.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface Column extends ColumnType {

  /**
   * The name of the column.
   *
   * @return the column name.
   */
  public String getName();


  /**
   * Indicates whether this column is part of the primary key on the table it
   * belongs to.
   *
   * @return {@code true} if the column is part of the primary key,
   *         {@code false} otherwise.
   */
  public boolean isPrimaryKey();


  /**
   * Gets the default value for this column.
   *
   * @return the default value.
   */
  public String getDefaultValue();


  /**
   * Indicates whether this field is auto-assigned a value by the RDBMS.
   *
   * @return whether this field is auto-assigned a value by the RDBMS.
   */
  public boolean isAutoNumbered();


  /**
   * Gets the value from which autonumber assignment should start.  Note
   * that this can only be guaranteed to return the correct value when
   * creating new columns and may not necessarily return the right value
   * when loaded from database metadata.
   *
   * @return the value from which autonumber assignment should start.
   */
  public int getAutoNumberStart();


  /**
   * Helper for {@link #toString()} implementations.
   */
  public default String toStringHelper() {
    return new StringBuilder()
        .append("Column-").append(getName())
        .append("-").append(getType())
        .append("-").append(getType().hasWidth() ? getWidth() : "")
        .append("-").append(getType().hasScale() ? getScale() : "")
        .append("-").append(isNullable() ? "null" : "notNull")
        .append("-").append(isPrimaryKey() ? "pk" : "")
        .append("-").append(isAutoNumbered() ? "autonum" : "")
        .append("-").append(getAutoNumberStart())
        .append("-").append(getDefaultValue())
        .toString();
  }
}
