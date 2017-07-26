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

import java.util.List;

/**
 * Defines a database table.
 * <p>
 * It is assumed that all database tables start with two columns which should
 * not be included in the column list returned by {@link #columns()}:
 * </p>
 * <ol>
 * <li>Id - An auto incrementing big integer that is the primary key.</li>
 * <li>Version - An integer that is used for record locking.</li>
 * </ol>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface Table {

  /**
   * @return the table name.
   */
  public String getName();


  /**
   * @return The column definitions for the table.
   */
  public List<Column> columns();


  /**
   * @return The indexes on this table.
   */
  public List<Index> indexes();


  /**
   * @return Indicates whether the table is temporary
   */
  public boolean isTemporary();

}
