/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deferred;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.SchemaChange;
import org.alfasoftware.morf.upgrade.SchemaChangeVisitor;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link SchemaChange} which queues a new index for background creation via
 * the deferred index execution mechanism. The index is added to the in-memory
 * schema immediately (so schema validation remains consistent), but the actual
 * {@code CREATE INDEX} DDL is deferred and executed by
 * {@code DeferredIndexExecutor} after the upgrade completes.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredAddIndex implements SchemaChange {

  /**
   * Name of table to add the index to.
   */
  private final String tableName;

  /**
   * New index to be created in the background.
   */
  private final Index newIndex;

  /**
   * UUID string of the upgrade step that queued this operation.
   */
  private final String upgradeUUID;

  /**
   * DAO for queued-operation checks; may be {@code null} when constructed
   * normally (created lazily from {@link ConnectionResources} in
   * {@link #isApplied}).
   */
  private final DeferredIndexOperationDAO dao;


  /**
   * Construct a {@link DeferredAddIndex} schema change.
   *
   * @param tableName   name of table to add the index to.
   * @param index       the index to be created in the background.
   * @param upgradeUUID UUID string of the upgrade step that queued this operation.
   */
  public DeferredAddIndex(String tableName, Index index, String upgradeUUID) {
    this.tableName = tableName;
    this.newIndex = index;
    this.upgradeUUID = upgradeUUID;
    this.dao = null;
  }


  /**
   * Constructor for testing — allows injection of a pre-built DAO.
   *
   * @param tableName   name of table to add the index to.
   * @param index       the index to be created in the background.
   * @param upgradeUUID UUID string of the upgrade step that queued this operation.
   * @param dao         DAO to use instead of creating one from {@link ConnectionResources}.
   */
  @VisibleForTesting
  DeferredAddIndex(String tableName, Index index, String upgradeUUID, DeferredIndexOperationDAO dao) {
    this.tableName = tableName;
    this.newIndex = index;
    this.upgradeUUID = upgradeUUID;
    this.dao = dao;
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
   * Adds the index to the in-memory schema. No DDL is emitted — the actual
   * {@code CREATE INDEX} is handled by the background executor.
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    Table original = schema.getTable(tableName);
    if (original == null) {
      throw new IllegalArgumentException(
        String.format("Cannot defer add index [%s] to table [%s] as the table cannot be found", newIndex.getName(), tableName));
    }

    List<String> indexes = new ArrayList<>();
    for (Index index : original.indexes()) {
      if (index.getName().equalsIgnoreCase(newIndex.getName())) {
        throw new IllegalArgumentException(
          String.format("Cannot defer add index [%s] to table [%s] as the index already exists", newIndex.getName(), tableName));
      }
      indexes.add(index.getName());
    }
    indexes.add(newIndex.getName());

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexes, Arrays.asList(new Index[] {newIndex})));
  }


  /**
   * Returns {@code true} if either:
   * <ol>
   *   <li>the index already exists in the database schema (build has completed), or</li>
   *   <li>a deferred operation for this table and index name is present in the
   *       queue (the upgrade step has been processed but the build is still
   *       pending or in progress).</li>
   * </ol>
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (schema.tableExists(tableName)) {
      Table table = schema.getTable(tableName);
      SchemaHomology homology = new SchemaHomology();
      for (Index index : table.indexes()) {
        if (homology.indexesMatch(index, newIndex)) {
          return true;
        }
      }
    }

    DeferredIndexOperationDAO effectiveDao = dao != null ? dao : new DeferredIndexOperationDAOImpl(database);
    return effectiveDao.existsByTableNameAndIndexName(tableName, newIndex.getName());
  }


  /**
   * Removes the index from the in-memory schema (inverse of {@link #apply}).
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    Table original = schema.getTable(tableName);
    List<String> indexNames = new ArrayList<>();
    boolean found = false;
    for (Index index : original.indexes()) {
      if (index.getName().equalsIgnoreCase(newIndex.getName())) {
        found = true;
      } else {
        indexNames.add(index.getName());
      }
    }

    if (!found) {
      throw new IllegalStateException(
        "Error reversing DeferredAddIndex. Index [" + newIndex.getName() + "] not found in table [" + tableName + "]");
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, null, null, indexNames, null));
  }


  /**
   * @return the UUID string of the upgrade step that queued this deferred index operation.
   */
  public String getUpgradeUUID() {
    return upgradeUUID;
  }


  /**
   * @return the name of the table the index will be added to.
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * @return the index to be created in the background.
   */
  public Index getNewIndex() {
    return newIndex;
  }


  @Override
  public String toString() {
    return "DeferredAddIndex [tableName=" + tableName + ", newIndex=" + newIndex + ", upgradeUUID=" + upgradeUUID + "]";
  }
}
