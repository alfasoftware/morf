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

package org.alfasoftware.morf.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.alfasoftware.morf.dataset.DataSetAdapter;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Adapts a data set by making sure a target database schema adheres to the schema in the data set being transmitted.
 * This implementation holds a state which is safely shared in a concurrent access scenario.
 * @author Copyright (c) Alfa Financial Software 2025
 */
public class ConcurrentSchemaModificationAdapter extends DataSetAdapter {

  private static final Log log = LogFactory.getLog(ConcurrentSchemaModificationAdapter.class);

  private  Connection connection;

  private final SqlDialect sqlDialect;

  private final DatabaseDataSetConsumer databaseDataSetConsumer;

  //shared between instances
  private static SchemaResourceState schemaState;
  //the lock to guard shared resources
  private static final Lock schemaLock = new ReentrantLock();

  private static class SchemaResourceState {

    /**
     * Provides database meta data once the adapter is open.
     */
    private SchemaResource schemaResource;

    private volatile boolean viewsDropped;

    /**
     * A list of the tables that were in the database when we started and have not been updated.
     *
     * <p>This is used to delete left over tables at the end of the operation. The list
     * is stored upper case, to avoid issues with databases which store case insensitive
     * names as all-caps.</p>
     */
    private final Set<String> remainingTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final Map<String, Table> existingIndexNamesAndTables = new ConcurrentHashMap<>();
  }


  /**
   * @param consumer Target data set consumer which is typically an instance of {@link DatabaseDataSetConsumer}.
   */
  public ConcurrentSchemaModificationAdapter(DatabaseDataSetConsumer consumer) {
    super(consumer);
    this.databaseDataSetConsumer = consumer;
    this.sqlDialect = databaseDataSetConsumer.connectionResources.sqlDialect();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#open()
   */
  @Override
  public void open() {
    schemaLock.lock();
    try {
      if(schemaState == null) { //initialize only once
        schemaState = new SchemaResourceState();
        schemaState.schemaResource = databaseDataSetConsumer.connectionResources.openSchemaResource(databaseDataSetConsumer.getDataSource());

        // get a list of all the tables at the start
        for (Table table : schemaState.schemaResource.tables()) {
          schemaState.remainingTables.add(table.getName().toUpperCase());
          table.indexes().forEach(index -> schemaState.existingIndexNamesAndTables.put(index.getName().toUpperCase(), table));
        }
      }
    }
    finally {
      schemaLock.unlock();
    }
    try {
      this.connection = databaseDataSetConsumer.getDataSource().getConnection();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error opening connection", e);
    }
    //this will open the consumer
    super.open();
  }


  /**
   * Drops all views from the existing schema if it has not already done so. This should be called whenever tables are dropped or modified to guard against an invalid situation.
   */
  private void dropExistingViewsIfNecessary() {
    if (schemaState.viewsDropped) {
      return;
    }
    schemaLock.lock();
    try {
      SqlScriptExecutor sqlExecutor = databaseDataSetConsumer.getSqlExecutor();
      for (View view : schemaState.schemaResource.views()) {
        sqlExecutor.execute(sqlDialect.dropStatements(view), connection);
      }
      schemaState.viewsDropped = true;
    }
    finally {
      schemaLock.unlock();
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {

    // only drop the remaining tables if the data copy completed cleanly
    if (closeState == CloseState.FINALLY_COMPLETE) {
      schemaLock.lock();
      try {
        dropRemainingTables();
        schemaState.schemaResource.close();
        schemaState = null;
      }
      finally {
        schemaLock.unlock();
      }
    }
    else if (closeState == CloseState.COMPLETE) {
      try {
        if (!connection.getAutoCommit()) {
          connection.commit();
        }
      } catch (SQLException e) {
        throw new RuntimeSqlException("Error committing", e);
      } finally {
        try {
          connection.close();
        } catch (Exception ignored) {
        }
      }
    }

    super.close(closeState);
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    schemaState.remainingTables.remove(table.getName().toUpperCase());
    initialiseTableSchema(table);

    super.table(table, records);
  }


  /**
   * Make sure the database is ready to receive the data. Check the table schema matches what we expect, if it doesn't drop and re-create.
   *
   * @param table The source table we're expecting.
   */
  private void initialiseTableSchema(Table table) {
    SqlScriptExecutor sqlExecutor = databaseDataSetConsumer.getSqlExecutor();

    // check whether the table already exists
    if (schemaState.schemaResource.tableExists(table.getName())) {
      // if the table exists, we need to check it's of the right schema
      Table databaseTableMetaData = schemaState.schemaResource.getTable(table.getName());

      if (!new SchemaHomology().tablesMatch(table, databaseTableMetaData)) {
        // there was a difference. Drop and re-deploy
        log.debug("Replacing table [" + table.getName() + "] with different version");
        dropExistingViewsIfNecessary();
        dropExistingIndexesIfNecessary(table);
        sqlExecutor.execute(sqlDialect.dropStatements(databaseTableMetaData), connection);
        sqlExecutor.execute(sqlDialect.tableDeploymentStatements(table), connection);

      } else {
        // Remove the index names that are now part of the modified schema
        table.indexes().forEach(index -> schemaState.existingIndexNamesAndTables.remove(index.getName().toUpperCase()));
      }
    } else {
        log.debug("Deploying missing table [" + table.getName() + "]");
        dropExistingViewsIfNecessary();
        dropExistingIndexesIfNecessary(table);
        sqlExecutor.execute(sqlDialect.tableDeploymentStatements(table), connection);
      }

  }


  /**
   * Drop all the tables that are left over at the end of the import
   */
  private void dropRemainingTables() {
    SqlScriptExecutor sqlExecutor = databaseDataSetConsumer.getSqlExecutor();

    try (Connection conn = databaseDataSetConsumer.getDataSource().getConnection()) {
      for (String tableName : schemaState.remainingTables) {
        log.debug("Dropping table [" + tableName + "] which was not in the transmitted data set");
        dropExistingViewsIfNecessary();
        Table table = schemaState.schemaResource.getTable(tableName);
        sqlExecutor.execute(sqlDialect.dropStatements(table), conn);
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Could not open database connection", e);
    }
  }


  /**
   * Drop all existing indexes of the table that's about to be deployed.
   */
  private void dropExistingIndexesIfNecessary(Table tableToDeploy) {
    tableToDeploy.indexes().forEach(index -> {
      Table existingTableWithSameIndex = schemaState.existingIndexNamesAndTables.remove(index.getName().toUpperCase());
      if (existingTableWithSameIndex != null && !tableToDeploy.getName().equalsIgnoreCase(existingTableWithSameIndex.getName())) {
        // Only drop the index if it belongs to the previous schema under a different tablename.
        databaseDataSetConsumer.getSqlExecutor().execute(sqlDialect.indexDropStatements(existingTableWithSameIndex, index), connection);
      }
    });
  }

}

