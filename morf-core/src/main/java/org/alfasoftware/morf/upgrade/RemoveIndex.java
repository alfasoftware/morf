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
 * {@link SchemaChange} which consists of removing an existing index from a
 * table within a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class RemoveIndex implements SchemaChange {

  /**
   * Name of table to add the index to.
   */
  private final String tableName;

  /**
   * Index to be removed.
   */
  private final Index indexToBeRemoved;


  /**
   * Construct an {@link RemoveIndex} schema change which can apply the change to
   * metadata.
   *
   * @param tableName name of table to add index to.
   * @param index index to be removed.
   */
  public RemoveIndex(String tableName, Index index) {
    this.tableName = tableName;
    this.indexToBeRemoved = index;
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
    boolean foundIndex = false;

    List<String> indexes = new ArrayList<>();
    for (Index index : original.indexes()) {
      // So long as this is not the index we're supposed to be removing
      if (index.getName().equalsIgnoreCase(indexToBeRemoved.getName())) {
        foundIndex = true;
        continue;
      }

      // Add the column to the schema
      indexes.add(index.getName());
    }

    if (!foundIndex) {
      throw new IllegalArgumentException("Cannot remove index [" + indexToBeRemoved.getName() + "] as it does not exist on table [" + tableName + "]");
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, null));
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (!schema.tableExists(tableName))
      return false;

    Table table = schema.getTable(tableName);
    SchemaHomology homology = new SchemaHomology();
    for (Index index : table.indexes()) {
      if (homology.indexesMatch(index, indexToBeRemoved)) {
        return false;
      }
    }
    return true;
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    Table original = schema.getTable(tableName);
    List<String> indexes = new ArrayList<>();
    for (Index index : original.indexes()) {
      indexes.add(index.getName());
    }
    indexes.add(indexToBeRemoved.getName());

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, Arrays.asList(new Index[] {indexToBeRemoved})));
  }


  /**
   * @return The name of the table to remove the index from.
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @return The index to be removed.
   */
  public Index getIndexToBeRemoved() {
    return indexToBeRemoved;
  }
}
