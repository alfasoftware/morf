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

import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Suppliers;

/**
 * Implements {@link ColumnBean} as a bean.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class ColumnBean extends ColumnTypeBean implements Column {

  private final String name;
  private final Supplier<String> upperCaseName;
  private final boolean  primaryKey;
  private final String defaultValue;
  private final boolean autoNumber;
  private final int autoNumberStart;
  //TODO change to private with appropriate constructors.
  protected boolean partitioned;

  /**
   * Creates a column with zero precision.
   *
   * @param name Column name.
   * @param type Column type.
   * @param width Column width.
   */
  ColumnBean(String name, DataType type, int width) {
    this(name, type, width, 0, true);
  }


  /**
   * Creates a column.
   *
   * @param name Column name.
   * @param type Column type.
   * @param width Column width.
   * @param scale Column scale.
   * @param nullable Column can be null.
   */
  ColumnBean(String name, DataType type, int width, int scale, boolean nullable) {
    this(name, type, width, scale, nullable, "", false);
  }


  /**
   * Creates a column.
   *
   * @param name Column name.
   * @param type Column type.
   * @param width Column width.
   * @param scale Column scale.
   * @param nullable Column can be null.
   * @param defaultValue the default value of the column.
   */
  ColumnBean(String name, DataType type, int width, int scale, boolean nullable, String defaultValue) {
    this(name, type, width, scale, nullable, defaultValue, false);
  }


  /**
   * Creates a column.
   *
   * @param name Column name.
   * @param type Column type.
   * @param width Column width.
   * @param scale Column scale.
   * @param nullable Column can be null.
   * @param defaultValue the default value of the column.
   * @param primaryKey Column is the primary key.
   */
  ColumnBean(String name, DataType type, int width, int scale, boolean nullable, String defaultValue, boolean primaryKey) {
    this(name, type, width, scale, nullable, defaultValue, primaryKey, false, 0);
  }



  /**
   * Creates a column.
   *
   * @param name Column name.
   * @param type Column type.
   * @param width Column width.
   * @param scale Column scale.
   * @param nullable Column can be null.
   * @param defaultValue the default value of the column.
   * @param primaryKey Column is the primary key.
   * @param autonumber Column is an identity column
   * @param autonumberStart Start value for the identity sequence
   */
  ColumnBean(String name, DataType type, int width, int scale, boolean nullable, String defaultValue, boolean primaryKey, boolean autonumber, int autonumberStart) {
    super(type, width, scale, nullable);
    this.name = name == null ? null : name.intern();
    this.upperCaseName = Suppliers.memoize(() -> name == null ? null : name.toUpperCase().intern());
    this.primaryKey = primaryKey;
    this.defaultValue = defaultValue;
    this.autoNumber = autonumber;
    this.autoNumberStart = autonumberStart;
  }


  /**
   * @param toCopy Column to copy.
   */
  ColumnBean(Column toCopy) {
    this(
      toCopy.getName(),
      toCopy.getType(),
      toCopy.getWidth(),
      toCopy.getScale(),
      toCopy.isNullable(),
      toCopy.getDefaultValue(),
      toCopy.isPrimaryKey(),
      toCopy.isAutoNumbered(),
      toCopy.getAutoNumberStart());
  }


  /**
   * Sets up the column as a copy of another column but with the specified
   * overrides
   * @param toCopy The column to copy
   * @param nullable Override nullability
   * @param defaultValue Override default value
   * @param primaryKey Override primary key status
   */
  ColumnBean(Column toCopy, boolean nullable, String defaultValue, boolean primaryKey) {
    this(
      toCopy.getName(),
      toCopy.getType(),
      toCopy.getWidth(),
      toCopy.getScale(),
      nullable,
      defaultValue,
      primaryKey,
      toCopy.isAutoNumbered(),
      toCopy.getAutoNumberStart());
  }


  /**
   * @return the name
   */
  @Override
  public String getName() {
    return name;
  }


  /**
   * @return the upper case name
   */
  @Override
  public String getUpperCaseName() {
    return upperCaseName.get();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Column#isPrimaryKey()
   */
  @Override
  public boolean isPrimaryKey() {
    return primaryKey;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Column#getDefaultValue()
   */
  @Override
  public String getDefaultValue() {
    return StringUtils.defaultString(defaultValue);
  }


  /**
   * @see org.alfasoftware.morf.metadata.Column#isAutoNumbered()
   */
  @Override
  public boolean isAutoNumbered() {
    return autoNumber;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Column#getAutoNumberStart()
   */
  @Override
  public int getAutoNumberStart() {
    return autoNumberStart;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.toStringHelper();
  }

  @Override
  public boolean isPartitioned() {
    return partitioned;
  }


}
