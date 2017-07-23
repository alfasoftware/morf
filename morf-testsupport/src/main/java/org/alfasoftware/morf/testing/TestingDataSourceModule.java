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

package org.alfasoftware.morf.testing;

import javax.sql.DataSource;

import com.google.inject.Binder;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.ConnectionResourcesBean;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

/**
 * Provides the basic bindings for accessing databases in tests.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestingDataSourceModule extends AbstractModule {

  private static final Log log = LogFactory.getLog(TestingDataSourceModule.class);

  /** Statically cached data source so we don't create a new one for every test. */
  private static volatile DataSource dataSource;

  /**
   * @see com.google.inject.Module#configure(Binder)
   */
  @Override
  public void configure() {
    bind(ConnectionResources.class).toInstance(new ConnectionResourcesBean(Resources.getResource("morf.properties")));
  }


  /**
   * Provide the same {@link DataSource} for every subsequent call in static scope.
   * Avoids excessive synchronisation where possible.
   */
  @Provides
  DataSource provideDataSource(ConnectionResources connectionResources) {
    if (dataSource != null) return dataSource;
    synchronized (this) {
      if (dataSource != null) return dataSource;
      dataSource = connectionResources.getDataSource();
      if (AutoCloseable.class.isInstance(dataSource)) {
        closeDataSourceOnShutdown();
      }
      return dataSource;
    }
  }


  private void closeDataSourceOnShutdown() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        log.info("Closing connection pool on VM shutdown");
        try {
          ((AutoCloseable) dataSource).close();
        } catch (Exception e) {
          throw new RuntimeException("Error closing data source", e);
        }
      }
    });
  }


  @Provides
  SqlDialect provideSqlDialect(ConnectionResources connectionResources) {
    return connectionResources.sqlDialect();
  }
}