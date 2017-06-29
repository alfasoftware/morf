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
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;

/**
 * Decorator that changes a table name for deploying transitional tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TableNameDecorator implements Table {

  /** Table to decorate */
  private final Table       table;

  /** New table name */
  private final String      name;

  /** Name decorated indexes */
  private final List<Index> decoratedIndexes = new ArrayList<Index>();


  /**
   * @param table Table to decorate.
   * @param name New name.
   */
  public TableNameDecorator(Table table, String name) {
    super();
    this.table = table;
    this.name = name;

    for (Index index : table.indexes()) {
      decoratedIndexes.add(new IndexNameDecorator(index, name + "_" + StringUtils.substringAfter(index.getName(), "_")));
    }
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#columns()
   */
  @Override
  public List<Column> columns() {
    return table.columns();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#getName()
   */
  @Override
  public String getName() {
    return name;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Table#indexes()
   */
  @Override
  public List<Index> indexes() {
    return decoratedIndexes;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Table#isTemporary()
   */
  @Override
  public boolean isTemporary() {
    return false;
  }
}
