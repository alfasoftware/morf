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

import com.google.common.collect.ImmutableList;

/**
 * Implements {@link org.alfasoftware.morf.metadata.Index} as a bean for testing.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class IndexBean implements Index {

  /**
   * Holds the index name.
   */
  private final String name;

  /**
   * Holds the column names in the index.
   */
  private final ImmutableList<String> columnNames;

  /**
   * Flags if the index is unique or not.
   */
  private final boolean unique;


  /**
   * Flags if index is partitioned and global.
   */
  //TODO: change this protected properties from protected to private and add the appropriate constructors to consider them
  protected boolean isGlobalPartitioned;

  /**
   * Flags if index is partitioned and local.
   */
  protected boolean isLocalPartitioned;

  /**
   * Creates an index bean.
   *
   * @param name The index name.
   * @param unique Flag indicating if the index is unique.
   * @param columnNames Column names to order the index.
   */
  IndexBean(String name, boolean unique, String... columnNames) {
    this(name, unique, ImmutableList.copyOf(columnNames));
  }


  /**
   * Creates an index bean.
   *
   * @param name The index name.
   * @param unique Flag indicating if the index is unique.
   * @param columnNames Column names to order the index.
   */
  IndexBean(String name, boolean unique, Iterable<String> columnNames) {
    this(name, unique, ImmutableList.copyOf(columnNames));
  }


  /**
   * Internal constructor.
   */
  private IndexBean(String name, boolean unique, ImmutableList<String> columnNames) {
    super();
    this.name = name;
    this.unique = unique;
    this.columnNames = columnNames;
  }


  /**
   * @param toCopy Index to copy.
   */
  IndexBean(Index toCopy) {
    this(toCopy.getName(), toCopy.isUnique(), toCopy.columnNames());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Index#getName()
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @see org.alfasoftware.morf.metadata.Index#columnNames()
   */
  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  /**
   * @return the unique
   */
  @Override
  public boolean isUnique() {
    return unique;
  }


  @Override
  public boolean isGlobalPartitioned() { return isGlobalPartitioned; }

  @Override
  public boolean isLocalPartitioned() { return isLocalPartitioned; }


  @Override
  public String toString() {
    return this.toStringHelper();
  }
}

