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

package org.alfasoftware.morf.upgrade.adapt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;

/**
 * Adapts an existing {@link Table} to return a different set of columns /
 * indexes.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class AlteredTable implements Table {

  /** {@link Table} adapted by this instance. */
  private final Table        baseTable;

  /** {@link Column}s in the adapted table. */
  private final List<Column> columns;

  /** {@link Index} in the adapted table. */
  private final List<Index>  indexes;


  /**
   * Simple constructor for specifying new / removed columns / indexes.
   *
   * @param baseTable table to adapt via column and index adjustments
   * @param columnOrder order of columns in adapted table.
   * @param newColumnDefinitions new definitions for columns (overrides or
   *          supplements those in the base {@link Table})
   * @param indexes indexes in the adapted table
   * @param newIndexDefinitions new definitions for indexes (overrides or
   *          supplements those in the base {@link Table})
   */
  public AlteredTable(Table baseTable, Collection<String> columnOrder, Collection<Column> newColumnDefinitions, Iterable<String> indexes, Iterable<Index> newIndexDefinitions) {
    this.baseTable = baseTable;

    // -- Apply changes to columns...
    //
    if (columnOrder != null) {
      this.columns = new ArrayList<>();
      Map<String, Column> columnMap = new HashMap<>();
      for (Column column : baseTable.columns()) {
        columnMap.put(column.getUpperCaseName(), column);
      }
      if (newColumnDefinitions != null) {
        for (Column column : newColumnDefinitions) {
          columnMap.put(column.getUpperCaseName(), column);
        }
      }
      for (String name : columnOrder) {
        this.columns.add(columnMap.get(name.toUpperCase()));
      }
    } else {
      this.columns = baseTable.columns();
    }
    // -- Apply changes to indexes...
    //
    if (indexes != null) {
      this.indexes = new ArrayList<>();
      Map<String, Index> indexMap = new HashMap<>();
      for (Index index : baseTable.indexes()) {
        indexMap.put(index.getName(), index);
      }
      if (newIndexDefinitions != null) {
        for (Index index : newIndexDefinitions) {
          indexMap.put(index.getName(), index);
        }
      }
      for (String name : indexes) {
        this.indexes.add(indexMap.get(name));
      }
    } else {
      this.indexes = baseTable.indexes();
    }
  }


  /**
   * Simple constructor for specifying new / removed columns. Indexes are
   * maintained as is.
   *
   * @param baseTable table to adapt via column and index adjustments
   * @param columnOrder order of columns in adapted table.
   * @param newColumnDefinitions new definitions for columns (overrides or
   *          supplements those in the base {@link Table})
   */
  public AlteredTable(Table baseTable, Collection<String> columnOrder, Collection<Column> newColumnDefinitions) {
    this(baseTable, columnOrder, newColumnDefinitions, null, null);
  }


  /**
   * Simple constructor for specifying altered tables. Indexes are maintained as
   * is. No new columns will be added.
   *
   * @param baseTable table to adapt via column and index adjustments
   * @param columnOrder order of columns in adapted table.
   */
  public AlteredTable(Table baseTable, Collection<String> columnOrder) {
    this(baseTable, columnOrder, null, null, null);
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#columns()
   */
  @Override
  public List<Column> columns() {
    return columns;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#getName()
   */
  @Override
  public String getName() {
    return baseTable.getName();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#indexes()
   */
  @Override
  public List<Index> indexes() {
    return indexes;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Table#isTemporary()
   */
  @Override
  public boolean isTemporary() {
    return baseTable.isTemporary();
  }
}
