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

package org.alfasoftware.morf.excel;

import org.alfasoftware.morf.metadata.Table;

/**
 * Optional data which can be included in an Excel dump of the database.
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public interface AdditionalSchemaData {

  /**
   * Fetches the documentation for a column.
   *
   * @param table The table.
   * @param columnName The column name.
   * @return The column documentation.
   */
  public String columnDocumentation(Table table, String columnName);

  /**
   * Fetches the default value for a column.
   *
   * @param table The table.
   * @param columnName The column name.
   * @return The column's default value.
   */
  public String columnDefaultValue(Table table, String columnName);
}
