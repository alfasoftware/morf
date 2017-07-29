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

package org.alfasoftware.morf.sql.element;


/**
 * An enumeration of the possible nulls position in ordering. This is primarily used
 * by the "ORDER BY" clause of an SQL statement.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public enum NullValueHandling {

  /**
   * no explicit null ordering
   */
  NONE("DEFAULT"),


  /**
   * nulls should go first
   */
  FIRST("FIRST"),


  /**
   * nulls should go last
   */
  LAST("LAST");


  private final String asString;

  private NullValueHandling(String asString) {
    this.asString = asString;

  }

  @Override
  public String toString() {
    return asString;
  }
}