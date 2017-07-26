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

import org.alfasoftware.morf.dataset.TableLoaderBuilder.TableLoaderBuilderImpl;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Table;
import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.util.Providers;

/**
 * Builds a {@link TableLoader}.
 *
 * <p>Obtain one by either:</p>
 * <ul>
 * <li>Injecting a provider of {@link TableLoaderBuilder}</li>
 * <li>Calling {@link TableLoader#builder()}, then passing an {@link SqlDialect} and a {@link SqlScriptExecutor}.</li>
 * </ul>
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
@ImplementedBy(TableLoaderBuilderImpl.class)
public interface TableLoaderBuilder {

  /**
   * Set the connection to use. This must always be specified.
   * @param connection The connection to use.
   * @return This builder for chaining
   */
  TableLoaderBuilder withConnection(final Connection connection);

  /**
   * Set the {@link SqlDialect} in use. This need only be specified if the builder was not injected.
   * @param sqlDialect  The {@link SqlDialect} to use.
   * @return This builder for chaining
   */
  TableLoaderBuilder withDialect(SqlDialect sqlDialect);

  /**
   * Set the {@link SqlScriptExecutor}. This need only be specified if the builder was not injected.
   * @param executor The {@link SqlScriptExecutor} to use.
   * @return This builder for chaining
   */
  TableLoaderBuilder withSqlScriptExecutor(final SqlScriptExecutor executor);

  /**
   * Should an explicit commit be made after the load?
   *
   * <p>Defaults to false if not called.</p>
   *
   * @return This builder for chaining
   */
  TableLoaderBuilder explicitCommit();

  /**
   * Should an explicit commit be made after the load?
   *
   * <p>Defaults to false if not specified.</p>
   *
   * @param explicitCommit Determines whether an explicit commit should be made or not.
   * @return This builder for chaining
   */
  TableLoaderBuilder explicitCommit(boolean explicitCommit);

  /**
   * Should the table be truncated before the load?
   *
   * <p>Defaults to false if not called.</p>
   *
   * @return This builder for chaining
   */
  TableLoaderBuilder truncateBeforeLoad();

  /**
   * We are inserting rows which have any autonumber columns values already set. (The DB is not allowed to set them for us)
   *
   * <p>This gives the DB dialect a hint that allows it to optimise behaviour.</p>
   *
   * <p>Defaults to false if not called.</p>
   *
   * @return This builder for chaining
   */
  TableLoaderBuilder insertingWithPresetAutonums();

  /**
   * We are definitely going to insert all rows underneath the current next autonum value, we don't want the current next autonum value to change.
   *
   * <p>This gives the DB dialect a hint that allows it to optimise behaviour.</p>
   *
   * Defaults to false if not specified.
   *
   * @return This builder for chaining
   */
  TableLoaderBuilder insertingUnderAutonumLimit();

  /**
   * Build the table loader for the specified table.
   *
   * @param table The table.
   * @return A table loader.
   */
  TableLoader forTable(Table table);

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

    TableLoaderBuilderImpl() {
      super();
      // This will need to be provided in withSqlScriptExecutor
      sqlScriptExecutorProvider = null;
    }

    @Inject
    TableLoaderBuilderImpl(
        SqlScriptExecutorProvider sqlScriptExecutorProvider,
        Provider<SqlDialect> sqlDialect) {
      super();
      this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
      this.sqlDialect = sqlDialect;
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
        truncateBeforeLoad);
    }
  }
}
