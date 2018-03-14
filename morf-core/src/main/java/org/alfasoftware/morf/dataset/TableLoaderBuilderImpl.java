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

import java.sql.Connection;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Table;

import com.google.inject.Provider;
import com.google.inject.util.Providers;

/**
 * Internal implementation of {@link TableLoaderBuilder}.
 */
class TableLoaderBuilderImpl implements TableLoaderBuilder {
  private Connection connection;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private SqlScriptExecutor sqlScriptExecutor;
  private boolean explicitCommit;
  private boolean truncateBeforeLoad;
  private boolean insertingWithPresetAutonums;
  private boolean insertingUnderAutonumLimit;
  private Provider<SqlDialect> sqlDialect;
  private int batchSize = 1000;

  TableLoaderBuilderImpl() {
    super();
    // This will need to be provided in withSqlScriptExecutor
    sqlScriptExecutorProvider = null;
  }

  @Override
  public TableLoaderBuilder withDialect(SqlDialect sqlDialect) {
    this.sqlDialect = Providers.of(sqlDialect);
    return this;
  }

  @Override
  public TableLoaderBuilder withConnection(Connection connection) {
    this.connection = connection;
    return this;
  }

  @Override
  public TableLoaderBuilder withSqlScriptExecutor(SqlScriptExecutor sqlScriptExecutor) {
    this.sqlScriptExecutor = sqlScriptExecutor;
    return this;
  }

  @Override
  public TableLoaderBuilder explicitCommit() {
    this.explicitCommit = true;
    return this;
  }

  @Override
  public TableLoaderBuilder explicitCommit(boolean explicitCommit) {
    this.explicitCommit = explicitCommit;
    return this;
  }

  @Override
  public TableLoaderBuilder truncateBeforeLoad() {
    this.truncateBeforeLoad = true;
    return this;
  }

  @Override
  public TableLoaderBuilder insertingWithPresetAutonums() {
    this.insertingWithPresetAutonums = true;
    return this;
  }

  @Override
  public TableLoaderBuilder insertingUnderAutonumLimit() {
    this.insertingUnderAutonumLimit = true;
    return this;
  }

  @Override
  public TableLoaderBuilder withBatchSize(int recordsPerBatch) {
    this.batchSize = recordsPerBatch;
    return this;
  }

  @Override
  public TableLoader forTable(Table table) {
    SqlScriptExecutor executor = sqlScriptExecutor;
    if (executor == null) {
      if (sqlScriptExecutorProvider == null) {
        throw new IllegalArgumentException("No SqlScriptExecutor provided");
      }
      executor = sqlScriptExecutorProvider.get();
    }
    return new TableLoader(
      connection,
      executor,
      sqlDialect.get(),
      explicitCommit,
      table,
      insertingWithPresetAutonums,
      insertingUnderAutonumLimit,
      truncateBeforeLoad,
      batchSize);
  }
}