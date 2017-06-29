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

package org.alfasoftware.morf.dataset;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import com.google.common.collect.Maps;

/**
 * Testing implementation of {@link DataSetProducer} that can return
 * a set of tables and records defined in code in a test case.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class MockDataSetProducer implements DataSetProducer, Schema {

  /**
   * Holds table meta data.
   */
  private final Map<String, Table> tables = Maps.newLinkedHashMap();

  /**
   *
   */
  private final Map<String, View> views = Maps.newLinkedHashMap();

  /**
   * Holds all the mocked data.
   */
  private final Map<String, List<Record>> data = new HashMap<String, List<Record>>();

  /**
   * Adds a table of data to the mock data set producer.
   *
   * @param table The meta data for the table
   * @param records The data for the table
   * @return this
   */
  public MockDataSetProducer addTable(Table table, Record... records) {
    this.tables.put(table.getName().toUpperCase(), table);
    this.data.put(table.getName().toUpperCase(), Arrays.asList(records));
    return this;
  }


  /**
   * Add a view to the mock data set producer.
   * @param view The meta data for the view
   * @return this
   */
  public MockDataSetProducer addView(View view) {
    this.views.put(view.getName().toUpperCase(), view);
    return this;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    // Nothing to do
  }

  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return this;
  }

  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    // Nothing to do
  }

  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(String tableName) {
    return data.get(tableName);
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(String name) {
    return tables.get(name.toUpperCase());
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
   */
  @Override
  public boolean isEmptyDatabase() {
    return tables.isEmpty();
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
   */
  @Override
  public boolean tableExists(String name) {
    return tables.containsKey(name.toUpperCase());
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    return tables.keySet();
  }

  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return tables.values();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    return data.get(tableName).isEmpty();
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
    return views.keySet();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return views.values();
  }
}
