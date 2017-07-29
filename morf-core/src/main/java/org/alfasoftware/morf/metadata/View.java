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

import org.alfasoftware.morf.sql.SelectStatement;

/**
 * Defines a database view - simply a named SQL statement.
 *
 * <p>Instead of implementing this class to define a view, use the DSL provided by
 * {@link SchemaUtils}.
 *
 * @see SchemaUtils#view(String, SelectStatement, String...)
 * @author Copyright (c) Alfa Financial Software 2012
 */
public interface View {

  /**
   * @return the view name
   */
  public String getName();

  /**
   * @return the select statement underlying the view
   * @throws UnsupportedOperationException if {@link #knowsSelectStatement()} has returned false.
   */
  public SelectStatement getSelectStatement();

  /**
   * @return Will {@link #getSelectStatement()} return something useful? If the view
   *   has been loaded from an external store, this may not be the case.
   */
  public boolean knowsSelectStatement();

  /**
   * @return names of views on which this view depends.
   */
  public String[] getDependencies();

  /**
   * @return whether {@link #getDependencies()} will return something useful? If the view
   *   has been loaded from an external store, this may not be the case.
   */
  public boolean knowsDependencies();
}
