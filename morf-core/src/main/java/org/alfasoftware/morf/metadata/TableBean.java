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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;


/**
 * Implements {@linkplain Table} as a bean.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
class TableBean implements Table {

  /**
   * Stores the table name.
   */
  private final String       tableName;

  /**
   * Stores an ordered list of columns in the meta data.
   */
  private final List<Column> columns = new ArrayList<>();

  /**
   * Stores the ordered list of indexes.
   */
  private final List<Index>  indexes = new ArrayList<>();

  /**
   * Indicates whether the table is temporary.
   */
  private final boolean      isTemporary;

  /**
   * Creates a table bean.
   *
   * @param tableName Name of the table to represent.
   * @param isTemporary Whether the table is temporary.
   */
  private TableBean(String tableName, boolean isTemporary) {
    this.tableName = tableName;
    this.isTemporary = isTemporary;
  }


  /**
   * Creates a table bean.
   *
   * @param tableName Name of the table to represent.
   */
  TableBean(String tableName) {
    this(tableName, false);
  }


  /**
   * Creates a table bean.
   *
   * @param tableName Name of the table to represent.
   * @param columns Columns for the table
   * @param indexes indexes for the table;
   * @param isTemporary Whether the table is a temporary table.
   */
  TableBean(String tableName, Iterable<? extends Column> columns, Iterable<? extends Index> indexes, boolean isTemporary) {
    this(tableName, isTemporary);

    Iterables.addAll(this.columns, columns);
    Iterables.addAll(this.indexes, indexes);
  }


  /**
   * Creates a table bean.
   *
   * @param toCopy Table to copy.
   * @throws RuntimeException Thrown when {@link org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider.UnexpectedDataTypeException} is caught.
   */
  TableBean(Table toCopy) {
    this(toCopy.getName());
    for (Column column : toCopy.columns()) {
      try {
        columns.add(new ColumnBean(column));
      } catch (RuntimeException e) {
        throw new RuntimeException("Exception copying table [" + toCopy.getName() + "]", e);
      }
    }

    for (Index index : toCopy.indexes()) {
      indexes.add(new IndexBean(index));
    }
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#getName()
   */
  @Override
  public String getName() {
    return tableName;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#columns()
   */
  @Override
  public List<Column> columns() {
    return columns;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#indexes()
   */
  @Override
  public List<Index> indexes() {
    return indexes;
  }


  /**
   * Finds a column with the specified name
   *
   * @param name the name of the column to look for
   * @return the first column with the specified name, or null if one cannot be
   *         found
   */
  protected Column findColumnNamed(final String name) {
    for (Column currentColumn : columns) {
      if (currentColumn.getName().equals(name)) {
        return currentColumn;
      }
    }

    // No match found
    return null;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Table#isTemporary()
   */
  @Override
  public boolean isTemporary() {
    return isTemporary;
  }


  @Override
  public String toString() {
    return "Table-" + getName();
  }
}
