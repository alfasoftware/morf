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

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Allows multiple schema to be combined with the results of calls to multiple
 * schema consolidated. Various methods which return true, or return results, will
 * use the first result from a matching schema.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
class CompositeSchema implements Schema {

  private final Schema[] delegates;


  /**
   * @param schema Schema to combine.
   * @return A single schema representing all of {@code schema}.
   */
  CompositeSchema(Schema... schema) {
    super();
    this.delegates = schema.clone();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    for (Schema schema : delegates)
      if (!schema.isEmptyDatabase())
        return false;

    return true;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String name) {
    for (Schema schema : delegates)
      if (schema.tableExists(name))
        return true;

    return false;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String name) {
    for (Schema schema : delegates)
      if (schema.tableExists(name))
        return schema.getTable(name);

    throw new IllegalArgumentException("Unknown table [" + name + "]");
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    Set<String> result = Sets.newHashSet();
    Set<String> seenTables = Sets.newHashSet();
    for (Schema schema : delegates) {
      for (Table table : schema.tables()) {
        if (seenTables.add(table.getName().toUpperCase())) {
          result.add(table.getName());
        }
      }
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    Set<Table> result = Sets.newHashSet();
    Set<String> seenTables = Sets.newHashSet();
    for (Schema schema : delegates) {
      for (Table table : schema.tables()) {
        if (seenTables.add(table.getName().toUpperCase())) {
          result.add(table);
        }
      }
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String name) {
    for (Schema schema : delegates)
      if (schema.viewExists(name))
        return true;

    return false;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String name) {
    for (Schema schema : delegates)
      if (schema.viewExists(name))
        return schema.getView(name);

    throw new IllegalArgumentException("Unknown table [" + name + "]");
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    Set<String> result = Sets.newHashSet();
    Set<String> seenViews = Sets.newHashSet();
    for (Schema schema : delegates) {
      for (View view : schema.views()) {
        if (seenViews.add(view.getName().toUpperCase())) {
          result.add(view.getName());
        }
      }
    }

    return result;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    Set<View> result = Sets.newHashSet();
    Set<String> seenViews = Sets.newHashSet();
    for (Schema schema : delegates) {
      for (View view : schema.views()) {
        if (seenViews.add(view.getName().toUpperCase())) {
          result.add(view);
        }
      }
    }

    return result;
  }


  @Override
  public String toString() {
    return "Schema[" + tables().size() + " tables, " + views().size() + " views]";
  }
}
