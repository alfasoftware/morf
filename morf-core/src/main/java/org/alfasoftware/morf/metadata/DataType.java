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
 * Enumerates the possible column data types.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public enum DataType {

  /**
   * Boolean data type supports the values true and false.
   */
  BOOLEAN(false, false),

  /**
   * Date supports date fields (no time element).
   */
  DATE(false, false),

  /**
   * Decimal data type supports all numerical fields including integer types.
   */
  DECIMAL(true, true),

  /**
   * String supports all character fields including single character fields.
   */
  STRING(true, false),

  /**
   * A 64-bit integer.
   */
  BIG_INTEGER(false, false),

  /**
   * Standard 32-bit integer.
   */
  INTEGER(false, false),

  /**
   * Blob supports arbitrary binary data types.
   */
  BLOB(false,false),

  /**
   * Clob supports long text strings.
   */
  CLOB(false, false),

  /**
   * Null type.
   */
  NULL(false, false);


  /***/
  private final boolean hasWidth;

  private final boolean hasScale;


  /**
   * @param hasWidth Whether this DataType has a variable width
   * @param hasScale Whether this DataType has a variable width
   */
  private DataType(boolean hasWidth, boolean hasScale) {
    this.hasWidth = hasWidth;
    this.hasScale = hasScale;
  }


  /**
   * @return Whether this DataType has a variable width
   */
  public boolean hasWidth() {
    return hasWidth;
  }


  /**
   * @return Whether this DataType has a variable width
   */
  public boolean hasScale() {
    return hasScale;
  }
}
