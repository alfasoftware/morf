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

import java.util.List;

import org.alfasoftware.morf.metadata.Index;

/**
 * Decorator that changes am index name for deploying transitional tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class IndexNameDecorator implements Index {

  /** Index to decorate */
  private final Index index;

  /** New index name */
  private final String name;

  /**
   * @param index Index to decorate.
   * @param name New name.
   */
  public IndexNameDecorator(Index index, String name) {
    super();
    this.index = index;
    this.name = name;
  }

  /**
   * @see org.alfasoftware.morf.metadata.Index#columnNames()
   */
  @Override
  public List<String> columnNames() {
    return index.columnNames();
  }

  /**
   * @see org.alfasoftware.morf.metadata.Index#isUnique()
   */
  @Override
  public boolean isUnique() {
    return index.isUnique();
  }

  /**
   * @see org.alfasoftware.morf.metadata.Index#getName()
   */
  @Override
  public String getName() {
    return name;
  }


  @Override
  public String toString() {
    return this.toStringHelper();
  }
}

