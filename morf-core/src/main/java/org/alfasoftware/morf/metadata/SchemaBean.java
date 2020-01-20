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

import static org.alfasoftware.morf.metadata.SchemaUtils.copy;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;


/**
 * Caches a source schema as a bean for efficient reading.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class SchemaBean implements Schema {

  /**
   * Holds all the tables represented in this schema.
   */
  private final Map<String, Table> tables = new HashMap<>();

  /**
   * Holds all the views represented in this schema.
   */
  private final Map<String, View> views = new HashMap<>();


  /**
   * Creates a schema. Views and tables from the schema are cloned.
   *
   * @param schema Schema to copy.
   */
  SchemaBean(Schema schema) {
    super();
    for (Table table : schema.tables()) {
      Table clone = copy(table);
      tables.put(clone.getName().toUpperCase(), clone);
    }

    for (View view : schema.views()) {
      View clone = copy(view);
      views.put(clone.getName().toUpperCase(), clone);
    }
  }


  /**
   * Create an empty schema.
   */
  SchemaBean() {
    this(Collections.<Table>emptyList(), Collections.<View>emptyList());
  }


  /**
   * Creates a schema.
   *
   * @param tables The tables included in the schema.
   */
  SchemaBean(Table... tables) {
    this(ImmutableList.copyOf(tables), Collections.<View>emptyList());
  }


  /**
   * Creates a schema.
   *
   * @param views The views included in the schema.
   */
  SchemaBean(View... views) {
    this(Collections.<Table>emptyList(), ImmutableList.copyOf(views));
  }


  /**
   * Creates a schema.
   *
   * @param tables The tables included in the schema.
   * @param views The views included in the schema.
   */
  SchemaBean(Iterable<Table> tables, Iterable<View> views) {
    super();
    for (Table table : tables) {
      this.tables.put(table.getName().toUpperCase(), table);
    }

    for (View view : views) {
      this.views.put(view.getName().toUpperCase(), view);
    }
  }


  /**
   * Creates a schema.
   *
   * @param tables The tables included in the schema.
   */
  SchemaBean(Iterable<Table> tables) {
    this(tables, Collections.<View>emptyList());
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String name) {
    return tables.get(name.toUpperCase());
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    return tables.isEmpty();
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String name) {
    return tables.containsKey(name.toUpperCase());
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    // Implemented like this rather than tables.keySet() to retain case
    Set<String> names = new HashSet<>();
    for (Table table : tables.values()) {
      names.add(table.getName());
    }
    return names;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return tables.values();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String name) {
    return views.containsKey(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String name) {
    return views.get(name.toUpperCase());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    // Implemented like this rather than views.keySet() to retain case
    Set<String> names = new HashSet<>();
    for (View view : views.values()) {
      names.add(view.getName());
    }
    return names;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return views.values();
  }


  @Override
  public String toString() {
    return "Schema[" + tables().size() + " tables, " + views().size() + " views]";
  }
}
