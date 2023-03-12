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

package org.alfasoftware.morf.directory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Abstract base for XML DataSet reader implementations.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public abstract class BaseDataSetReader implements DirectoryStreamProvider.DirectoryInputStreamProvider {

  /**
   * Maps table names to upper case
   */
  private final Map<String, String> tableNameToFileNameMap = Maps.newHashMap();
  private final List<String> tableNames = Lists.newArrayList();

  /**
   * Add a table, along with its local filename
   *
   * @param tableName The table name, in it's correct case
   * @param fileName The file name that holds it
   */
  protected void addTableName(String tableName, String fileName) {
    tableNameToFileNameMap.put(tableName.toUpperCase(), fileName);
    tableNames.add(tableName);
  }


  /**
   * Clear the local table lists.
   */
  protected void clear() {
    tableNameToFileNameMap.clear();
    tableNames.clear();
  }


  /**
   * Return the file name for a given table name.
   *
   * @param tableName The table name
   * @return The file name
   */
  protected final String fileNameForTable(String tableName) {
    String filename = tableNameToFileNameMap.get(tableName.toUpperCase());
    if (filename == null) throw new IllegalArgumentException("No such table [" + tableName + "]");
    return filename;
  }


  /**
   * @see org.alfasoftware.morf.directory.DirectoryStreamProvider.DirectoryInputStreamProvider#availableStreamNames()
   */
  @Override
  public Collection<String> availableStreamNames() {
    return Lists.newArrayList(tableNames); // return a copy, as we are about to clear the list
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#tableExists(java.lang.String)
   */
  @Override
  public final boolean tableExists(String name) {
    return tableNameToFileNameMap.containsKey(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#open()
   */
  @Override
  public void open() {
    // no-op
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#close()
   */
  @Override
  public void close() {
    // no-op
  }
}
