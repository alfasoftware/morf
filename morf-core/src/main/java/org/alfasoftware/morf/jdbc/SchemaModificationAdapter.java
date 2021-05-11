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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SchemaModificationAdapter extends DataSetAdapter {
  /** Standard logger */
  private static final Log log = LogFactory.getLog(SchemaModificationAdapter.class);

  /**
   * A list of the tables that were in the database when we started and have not been updated.
   *
   * <p>This is used to delete left over tables at the end of the operation. The list
   * is stored upper case, to avoid issues with databases which store case insensitive
   * names as all-caps.</p>
   */
  private final Set<String> remainingTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final Set<String> existingIndexNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final SqlDialect sqlDialect;

  /**
   * Provides database meta data once the adapter is open.
   */
  private SchemaResource schemaResource;

  private Connection connection;

  private final DatabaseDataSetConsumer databaseDataSetConsumer;

  private boolean viewsDropped;


  /**
   * @param consumer Target data set consumer which is typically an instance of {@link DatabaseDataSetConsumer}.
   */
  public SchemaModificationAdapter(DatabaseDataSetConsumer consumer) {
    super(consumer);
    this.databaseDataSetConsumer = consumer;
    this.sqlDialect = databaseDataSetConsumer.connectionResources.sqlDialect();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#open()
   */
  @Override
  public void open() {
    super.open();
    schemaResource = databaseDataSetConsumer.connectionResources.openSchemaResource(databaseDataSetConsumer.getDataSource());
    try {
      connection = databaseDataSetConsumer.getDataSource().getConnection();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error closing connection", e);
    }
    // get a list of all the tables at the start
    for (Table table : schemaResource.tables()) {
      remainingTables.add(table.getName().toUpperCase());
      table.indexes().forEach(index -> existingIndexNames.add(index.getName().toUpperCase()));
    }
  }


  /**
   * Drops all views from the existing schema if it has not already done so. This should be called whenever tables are dropped or modified to guard against an invalid situation.
   */
  private synchronized void dropExistingViewsIfNecessary() {
    if (viewsDropped) {
      return;
    }
    SqlScriptExecutor sqlExecutor = databaseDataSetConsumer.getSqlExecutor();
    for (View view : schemaResource.views()) {
      sqlExecutor.execute(sqlDialect.dropStatements(view), connection);
    }
    viewsDropped = true;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    // only drop the remaining tables if the data copy completed cleanly
    if (closeState == CloseState.COMPLETE) {
      dropRemainingTables();
    }
    schemaResource.close();
    try {
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error closing connection", e);
    }

    super.close(closeState);
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetAdapter#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    remainingTables.remove(table.getName().toUpperCase());
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
    if (schemaResource.tableExists(table.getName())) {
      // if the table exists, we need to check it's of the right schema
      Table databaseTableMetaData = schemaResource.getTable(table.getName());
      databaseTableMetaData.indexes().forEach(index -> existingIndexNames.remove(index.getName().toUpperCase()));

      if (!new SchemaHomology().tablesMatch(table, databaseTableMetaData)) {
        // there was a difference. Drop and re-deploy
        log.debug("Replacing table [" + table.getName() + "] with different version");
        dropExistingViewsIfNecessary();
        sqlExecutor.execute(sqlDialect.dropStatements(databaseTableMetaData), connection);
        sqlExecutor.execute(sqlDialect.tableDeploymentStatements(table), connection);
      }

    } else {
      log.debug("Deploying missing table [" + table.getName() + "]");
      dropExistingViewsIfNecessary();
      table.indexes().forEach(index -> {
        if (existingIndexNames.remove(index.getName().toUpperCase())) {
          sqlExecutor.execute(sqlDialect.indexDropStatements(table, index), connection);
        }
      });
      sqlExecutor.execute(sqlDialect.tableDeploymentStatements(table), connection);
    }
  }


  /**
   * Drop all the tables that are left over at the end of the import
   */
  private void dropRemainingTables() {
    SqlScriptExecutor sqlExecutor = databaseDataSetConsumer.getSqlExecutor();

    for (String tableName : remainingTables) {
      log.debug("Dropping table [" + tableName + "] which was not in the transmitted data set");
      dropExistingViewsIfNecessary();
      Table table = schemaResource.getTable(tableName);
      sqlExecutor.execute(sqlDialect.dropStatements(table), connection);
    }
  }

}
