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
 * {@link SchemaChange} which consists of changing a existing index within a
 * schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class ChangeIndex implements SchemaChange {

  /** The table to change **/
  private final String tableName;

  /** The start definition for the index **/
  private final Index fromIndex;

  /** The end definition for the index **/
  private final Index toIndex;


  /**
   * @param tableName the name of the table to change
   * @param fromIndex the {@link Index} to change from
   * @param toIndex the {@link Index} to change to
   */
  public ChangeIndex(String tableName, Index fromIndex, Index toIndex) {
    this.tableName = tableName;
    this.fromIndex = fromIndex;
    this.toIndex = toIndex;
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
    return applyChange(schema, fromIndex, toIndex);
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
      if (homology.indexesMatch(index, toIndex)) {
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
    return applyChange(schema, toIndex, fromIndex);
  }


  /**
   * Changes an index from the start point to the end point.
   *
   * @param schema {@link Schema} to apply the change against resulting in new
   *          metadata.
   * @param indexStartPoint the start definition for the index
   * @param indexEndPoint the end definition for the index
   * @return MetaData with {@link SchemaChange} applied.
   */
  private Schema applyChange(Schema schema, Index indexStartPoint, Index indexEndPoint) {
    // Check the state
    if (indexStartPoint == null) {
      throw new IllegalStateException("Cannot change a null index to have a new definition");
    }

    if (indexEndPoint == null) {
      throw new IllegalStateException(String.format("Cannot change index [%s] to be null", indexStartPoint.getName()));
    }

    // Now setup the new table definition
    Table original = schema.getTable(tableName);

    boolean foundMatch = false;

    // Copy the index names into a list of strings for column sort order
    List<String> indexes = new ArrayList<>();
    for (Index index : original.indexes()) {
      String currentIndexName = index.getName();

      // If we're looking at the index being changed...
      if (currentIndexName.equalsIgnoreCase(indexStartPoint.getName())) {
        // Substitute in the new index
        currentIndexName = indexEndPoint.getName();
        foundMatch = true;
      }

      for (String existing : indexes) {
        if (existing.equalsIgnoreCase(currentIndexName)) {
          throw new IllegalArgumentException(String.format("Cannot change index name from [%s] to [%s] on table [%s] as index with that name already exists", indexStartPoint.getName(), indexEndPoint.getName(), tableName));
        }
      }

      indexes.add(currentIndexName);
    }

    if (!foundMatch) {
      throw new IllegalArgumentException(String.format("Cannot change index [%s] as it does not exist on table [%s]", indexStartPoint.getName(), tableName));
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, Arrays.asList(new Index[] {indexEndPoint})));
  }


  /**
   * Gets the name of the table to change.
   *
   * @return the name of the table to change
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * Gets the index prior to the change
   *
   * @return the index prior to the change
   */
  public Index getFromIndex() {
    return fromIndex;
  }


  /**
   * Gets the index after the change
   *
   * @return the index after the change
   */
  public Index getToIndex() {
    return toIndex;
  }


  @Override
  public String toString() {
    return "ChangeIndex [tableName=" + tableName + ", fromIndex=" + fromIndex + ", toIndex=" + toIndex + "]";
  }
}
