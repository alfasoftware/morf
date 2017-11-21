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
package org.alfasoftware.morf.guicesupport;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.TableLoader;
import org.alfasoftware.morf.dataset.TableLoaderBuilder;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.upgrade.TableContribution;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice bindings for Morf module.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MorfModule extends AbstractModule {
  /**
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), UpgradeScriptAddition.class);

    Multibinder<TableContribution> tableMultibinder = Multibinder.newSetBinder(binder(), TableContribution.class);
    tableMultibinder.addBinding().to(DatabaseUpgradeTableContribution.class);
  }


  /**
   * Provides a {@link SqlScriptExecutorProvider}.
   *
   * @param param The optional bindings.
   * @return The {@link SqlScriptExecutorProvider}.
   */
  @Provides
  public SqlScriptExecutorProvider sqlScriptExecutorProvider(SqlScriptExecutorProviderParam param) {
    Preconditions.checkNotNull(param.sqlDialect, "Dialect is null");
    Preconditions.checkNotNull(param.dataSource, "Data source is null");
    return new SqlScriptExecutorProvider(param.dataSource, param.sqlDialect);
  }

  private static final class SqlScriptExecutorProviderParam {
    @Inject(optional = true) SqlDialect sqlDialect;
    @Inject(optional = true) DataSource dataSource;
  }


  /**
   * Provides a {@link TableLoaderBuilder}.
   *
   * @param param The optional bindings.
   * @param sqlScriptExecutorProvider The SQL script executor.
   * @return The {@link TableLoaderBuilder}.
   */
  @Provides()
  public TableLoaderBuilder tableLoaderBuilder(TableLoaderBuilderParam param, SqlScriptExecutorProvider sqlScriptExecutorProvider) {
    Preconditions.checkNotNull(param.sqlDialect, "Dialect is null");
    return TableLoader.builder()
        .withDialect(param.sqlDialect)
        .withSqlScriptExecutor(sqlScriptExecutorProvider.get());
  }

  private static final class TableLoaderBuilderParam {
    @Inject(optional = true) SqlDialect sqlDialect;
  }
}