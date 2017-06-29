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

package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;

/**
 * {@link SchemaChange} which consists of adding a new index to an existing
 * table within a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class AddIndex implements SchemaChange {


  /**
   * Name of table to add the index to.
   */
  private final String tableName;

  /**
   * New index to be added.
   */
  private final Index newIndex;


  /**
   * Construct an {@link AddIndex} schema change which can apply the change to
   * metadata.
   *
   * @param tableName name of table to add index to.
   * @param index new index to be added.
   */
  public AddIndex(String tableName, Index index) {
    this.tableName = tableName;
    this.newIndex = index;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    Table original = schema.getTable(tableName);
    if (original == null) {
      throw new IllegalArgumentException(String.format("Cannot add index [%s] to table [%s] as the table cannot be found", newIndex.getName(), tableName));
    }

    List<String> indexes = new ArrayList<>();

    for (Index index : original.indexes()) {
      if (index.getName().equals(newIndex.getName())) {
        throw new IllegalArgumentException(String.format("Cannot add index [%s] to table [%s] as the index already exists", newIndex.getName(), tableName));
      }

      indexes.add(index.getName());
    }
    indexes.add(newIndex.getName());

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, Arrays.asList(new Index[] {newIndex})));
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (!schema.tableExists(tableName)) {
      return false;
    }

    Table table = schema.getTable(tableName);
    SchemaHomology homology = new SchemaHomology();
    for (Index index : table.indexes()) {
      if (homology.indexesMatch(index, newIndex)) {
        return true;
      }
    }
    return false;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    Table original = schema.getTable(tableName);
    List<String> indexeNames = new ArrayList<>();
    boolean foundAndRemovedIndex = false;
    for (Index index : original.indexes()) {
      if (index.getName().equalsIgnoreCase(newIndex.getName())) {
        foundAndRemovedIndex = true;
      } else {
        indexeNames.add(index.getName());
      }
    }

    // Remove the index we are filtering
    if (!foundAndRemovedIndex) {
      throw new IllegalStateException("Error reversing AddIndex database change. Index [" + newIndex.getName() + "] not found in table [" + tableName + "] so it could not be reversed out");
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexeNames, null));
  }


  /**
   * @return the name of the table to add the column to.
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @return The new index.
   */
  public Index getNewIndex() {
    return newIndex;
  }

}
