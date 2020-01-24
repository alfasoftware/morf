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
 * Implements {@link ColumnType} as a bean.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
class ColumnTypeBean implements ColumnType {

  /**
   * The column type.
   */
  private final DataType type;

  /**
   * The column width.
   */
  private final int width;

  /**
   * The column scale / decimals.
   */
  private final int scale;

  /**
   * Indicates if the column can be null.
   */
  private final boolean nullable;


  /**
   * Creates a column type.
   *
   * @param type Column type.
   * @param width Column width.
   * @param scale Column scale.
   * @param nullable Column can be null.
   */
  ColumnTypeBean(DataType type, int width, int scale, boolean nullable) {
    super();
    this.type = type;
    this.width = type.hasWidth() ? width : 0;
    this.scale = type.hasScale() ? scale : 0;
    this.nullable = nullable;
  }


  /**
   * @return the type
   */
  @Override
  public DataType getType() {
    return type;
  }


  /**
   * @return the width
   */
  @Override
  public int getWidth() {
    return width;
  }


  /**
   * @return the scale
   */
  @Override
  public int getScale() {
    return scale;
  }


  /**
   * @return the nullable
   */
  @Override
  public boolean isNullable() {
    return nullable;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("ColumnType-%s-%d-%d-%s", type, width, scale, nullable);
  }
}
