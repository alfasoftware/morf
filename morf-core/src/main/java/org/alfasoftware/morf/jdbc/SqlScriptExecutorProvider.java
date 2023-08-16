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

import org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.util.Providers;

/**
 * Provides SQLScriptExecutors.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class SqlScriptExecutorProvider implements Provider<SqlScriptExecutor> {

  private final DataSource dataSource;
  private final Provider<SqlDialect> sqlDialect;
  private ConnectionResources connectionResources;

  /**
   * Constructor for Guice.
   *
   * @param dataSource The {@link DataSource} to instantiate the
   *          {@link SqlScriptExecutorProvider} for
   * @param sqlDialect The dialect to use
   * @param connectionResourcesHolder inner class containing an optional injected {@link ConnectionResources} to instantiate the {@link SqlScriptExecutor} with.
   */
  @Inject
  public SqlScriptExecutorProvider(final DataSource dataSource, Provider<SqlDialect> sqlDialect, ConnectionResourcesHolder connectionResourcesHolder) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = sqlDialect;
    this.connectionResources = connectionResourcesHolder.value;
  }

  /**
   * @param dataSource The {@link DataSource} to instantiate the
   *          {@link SqlScriptExecutorProvider} for
   * @param sqlDialect The dialect to use
   */
  public SqlScriptExecutorProvider(final DataSource dataSource, Provider<SqlDialect> sqlDialect) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = sqlDialect;
  }


  /**
   * @param dataSource The database connection source to use
   * @param sqlDialect The dialect to use for the dataSource
   */
  public SqlScriptExecutorProvider(final DataSource dataSource, SqlDialect sqlDialect) {
    super();
    this.dataSource = dataSource;
    this.sqlDialect = Providers.<SqlDialect>of(sqlDialect);
  }


  /**
   * @param connectionResources The connection to use.
   */
  public SqlScriptExecutorProvider(ConnectionResources connectionResources) {
    super();
    this.dataSource = connectionResources.getDataSource();
    this.sqlDialect = Providers.<SqlDialect>of(connectionResources.sqlDialect());
    this.connectionResources = connectionResources;
  }


  /**
   * Gets an instance of a {@link SqlScriptExecutor}.
   *
   * @return an instance of an {@link SqlScriptExecutor}.
   */
  @Override
  public SqlScriptExecutor get() {
    return get(null);
  }


  /**
   * Gets an instance of a {@link SqlScriptExecutor} with the provided visitor
   * set.
   *
   * @param visitor the visitor.
   * @return an instance of an {@link SqlScriptExecutor}.
   */
  public SqlScriptExecutor get(SqlScriptVisitor visitor) {
    if (connectionResources != null) {
      return new SqlScriptExecutor(defaultVisitor(visitor), dataSource, sqlDialect.get(), connectionResources);
    } else {
      return new SqlScriptExecutor(defaultVisitor(visitor), dataSource, sqlDialect.get());
    }
  }


  /**
   * Defaults the {@code visitor} to be a NullVisitor if the visitor is null.
   *
   * @param visitor the visitor to potentially default.
   * @return a not-null visitor.
   */
  protected SqlScriptVisitor defaultVisitor(SqlScriptVisitor visitor) {
    if (visitor != null) {
      return visitor;
    }
    return new NullVisitor();
  }

  /**
   * Null (No-op) visitor.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  private static final class NullVisitor implements SqlScriptVisitor {

    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionStart()
     */
    @Override
    public void executionStart() {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#beforeExecute(java.lang.String)
     */
    @Override
    public void beforeExecute(String sql) {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#afterExecute(java.lang.String,
     *      long)
     */
    @Override
    public void afterExecute(String sql, long numberOfRowsUpdated) {
      // Defaults to no-op
    }


    /**
     * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionEnd()
     */
    @Override
    public void executionEnd() {
      // Defaults to no-op
    }
  }


  public static class Factory {

    /**
     * @param connectionResources The connection resources to use.
     * @return new instance of {@link SqlScriptExecutorProvider}
     */
    public SqlScriptExecutorProvider create(final ConnectionResources connectionResources) {
      return new SqlScriptExecutorProvider(connectionResources);
    }


    /**
     * @param dataSource The database connection source to use
     * @param sqlDialect The dialect to use for the dataSource
     * @return new instance of {@link SqlScriptExecutorProvider}
     */
    public SqlScriptExecutorProvider create(final DataSource dataSource, final SqlDialect sqlDialect) {
      return new SqlScriptExecutorProvider(dataSource, sqlDialect);
    }
  }

  /**
   * Holder class that allows an optional {@link ConnectionResources} to be injected via Guice.
   */
  static class ConnectionResourcesHolder {
    @Inject(optional=true) ConnectionResources value;
  }
}