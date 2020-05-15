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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;

/**
 * {@link SchemaChange} which consists of renaming an existing index within a
 * schema.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class RenameIndex implements SchemaChange {

  private final String tableName;
  private final String fromIndexName;
  private final String toIndexName;


  /**
   * Rename the index specified.
   *
   * @param tableName the name of the index to change
   * @param fromIndexName the name of the index which will be renamed
   * @param toIndexName the new name for the specified index
   */
  public RenameIndex(String tableName, String fromIndexName, String toIndexName) {
    this.tableName = tableName;
    this.fromIndexName = fromIndexName;
    this.toIndexName = toIndexName;
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
    return applyChange(schema, fromIndexName, toIndexName);
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema,
   *      ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (!schema.tableExists(tableName)) {
      return false;
    }

    Table table = schema.getTable(tableName);
    for (Index index : table.indexes()) {
      if (index.getName().equalsIgnoreCase(toIndexName)) {
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
    return applyChange(schema, toIndexName, fromIndexName);
  }


  /**
   * Renames an index from the name specified to the new name.
   *
   * @param schema {@link Schema} to apply the change against resulting in new
   *          metadata.
   * @param indexStartName the starting name for the index
   * @param indexEndName the end name for the index
   * @return MetaData with {@link SchemaChange} applied.
   */
  private Schema applyChange(Schema schema, String indexStartName, String indexEndName) {
    // Check the state
    if (StringUtils.isBlank(indexStartName)) {
      throw new IllegalArgumentException("Cannot rename an index without the name of the index to rename");
    }

    if (StringUtils.isBlank(indexEndName)) {
      throw new IllegalArgumentException(String.format("Cannot rename index [%s] to be blank", indexStartName));
    }

    // Now setup the new table definition
    Table original = schema.getTable(tableName);

    boolean foundMatch = false;

    // Copy the index names into a list of strings
    List<String> indexes = new ArrayList<String>();
    Index newIndex = null;
    for (Index index : original.indexes()) {
      String currentIndexName = index.getName();

      // If we're looking at the index being renamed...
      if (currentIndexName.equalsIgnoreCase(indexStartName)) {
        // Substitute in the new index name
        currentIndexName = indexEndName;
        if (index.isUnique()) {
          newIndex = index(indexEndName).columns(index.columnNames()).unique();
        } else {
          newIndex = index(indexEndName).columns(index.columnNames());
        }

        foundMatch = true;
      }

      for (String existing : indexes) {
        if (existing.equalsIgnoreCase(currentIndexName)) {
          throw new IllegalArgumentException(String.format(
            "Cannot rename index from [%s] to [%s] on table [%s] as index with that name already exists", indexStartName,
            indexEndName, tableName));
        }
      }

      indexes.add(currentIndexName);
    }

    if (!foundMatch) {
      throw new IllegalArgumentException(String.format("Cannot rename index [%s] as it does not exist on table [%s]",
        indexStartName, tableName));
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, Arrays.asList(new Index[] { newIndex })));
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
   * Gets the name of the index prior to the change
   *
   * @return the name of the index prior to the change
   */
  public String getFromIndexName() {
    return fromIndexName;
  }


  /**
   * Gets the name of the index after the change
   *
   * @return the name of the index after the change
   */
  public String getToIndexName() {
    return toIndexName;
  }
}
