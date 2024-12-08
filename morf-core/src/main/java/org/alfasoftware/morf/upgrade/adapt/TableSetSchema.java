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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;

/**
 * Implementation of {@link Schema} which stores a collection of Tables and sequences.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TableSetSchema implements Schema {

  /** Set of tables that constitute this schema*/
  private final Set<Table> tables;


  /** Set of sequences that constitute this schema*/
  private final Set<Sequence> sequences;


  /**
   * Construct a TableSet which represents the specified set of tables and sequences.
   *
   * @param tables that constitute this schema.
   * @param sequences that constitute this schema
   */
  public TableSetSchema(Collection<Table> tables, Collection<Sequence> sequences) {
    this.tables = new HashSet<>();
    this.sequences = new HashSet<>();

    this.tables.addAll(tables);
    this.sequences.addAll(sequences);
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tables()
   */
  @Override
  public Collection<Table> tables() {
    return Collections.unmodifiableCollection(tables);
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
   */
  @Override
  public Table getTable(final String name) {
    return tables.stream()
        .filter(table -> table.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(String.format("Requested table [%s] does not exist.", name)));
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
  public boolean tableExists(final String name) {
    return tables.stream().anyMatch(table -> table.getName().equalsIgnoreCase(name));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#tableNames()
   */
  @Override
  public Collection<String> tableNames() {
    ArrayList<String> names = new ArrayList<>();
    for (Table table : tables) {
      names.add(table.getName());
    }
    return names;
  }

  @Override
  public Collection<String> partitionedTableNames() {
    return List.of();
  }

  @Override
  public Collection<String> partitionTableNames() {
    return List.of();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
   */
  @Override
  public boolean viewExists(String name) {
    return false;
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
   */
  @Override
  public View getView(String name) {
    throw new IllegalArgumentException("No view [" + name + "]. Views not supported by " + TableSetSchema.class.getSimpleName());
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#viewNames()
   */
  @Override
  public Collection<String> viewNames() {
    return Collections.emptySet();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#views()
   */
  @Override
  public Collection<View> views() {
    return Collections.emptySet();
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#sequenceExists(String)
   */
  @Override
  public boolean sequenceExists(String name) {
    return sequences.stream().anyMatch(sequence -> sequence.getName().equalsIgnoreCase(name));
  }


  /**
   * @see org.alfasoftware.morf.metadata.Schema#getSequence(String)
   */
  @Override
  public Sequence getSequence(String name) {
    return sequences.stream()
      .filter(sequence -> sequence.getName().equalsIgnoreCase(name))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(String.format("Requested sequence [%s] does not exist.", name)));
  }


  /**
   * @see Schema#sequenceNames()
   */
  @Override
  public Collection<String> sequenceNames() {
    ArrayList<String> names = new ArrayList<>();
    for (Sequence sequence : sequences) {
      names.add(sequence.getName());
    }
    return names;
  }


  /**
   * @see Schema#sequences()
   */
  @Override
  public Collection<Sequence> sequences() {
    return Collections.unmodifiableCollection(sequences);
  }
}
