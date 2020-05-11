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

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.dataset.TableLoader;
import org.alfasoftware.morf.metadata.Table;

import com.google.inject.Inject;


/**
 * Implementation of {@link DataSetConsumer} that pipes records into a JDBC
 * database connection wihout truncating tables and merging duplicate records.
 *
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class MergingDatabaseDataSetConsumer extends DatabaseDataSetConsumer implements DataSetConsumer {


  /**
   * Creates an instance of the database data set consumer.
   *
   * @param connectionResources Provides database connection interfaces.
   * @param sqlScriptExecutorProvider a provider of {@link SqlScriptExecutor}s.
   */
  public MergingDatabaseDataSetConsumer(ConnectionResources connectionResources, SqlScriptExecutorProvider sqlScriptExecutorProvider) {
    this(connectionResources, sqlScriptExecutorProvider, connectionResources.getDataSource());
  }


  /**
   * Creates an instance of the database data set consumer.
   *
   * <p>Can also be injected from Guice.</p>
   *
   * @param connectionResources Provides database connection interfaces.
   * @param sqlScriptExecutorProvider a provider of {@link SqlScriptExecutor}s.
   * @param dataSource DataSource providing database connections.
   */
  @Inject
  public MergingDatabaseDataSetConsumer(ConnectionResources connectionResources, SqlScriptExecutorProvider sqlScriptExecutorProvider, DataSource dataSource) {
    super(connectionResources, sqlScriptExecutorProvider, dataSource);
  }


  /**
   * @see DataSetConsumer#table(Table, Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    TableLoader.builder()
      .withConnection(connection)
      .withSqlScriptExecutor(sqlExecutor)
      .withDialect(sqlDialect)
      .explicitCommit(true)
      .ignoreDuplicateKeys(true)
      .insertingWithPresetAutonums()
      .forTable(table)
      .load(records);
  }
}