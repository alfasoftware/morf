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

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.dataset.TableLoader;
import org.alfasoftware.morf.metadata.Table;
import com.google.inject.Inject;


/**
 * Implementation of {@link DataSetConsumer} that pipes records into a JDBC
 * database connection.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class DatabaseDataSetConsumer implements DataSetConsumer {
  /** Standard logger */
  private static final Log log = LogFactory.getLog(DatabaseDataSetConsumer.class);

  /**
   * Database connection resource for the database we are writing to.
   */
  protected final ConnectionResources connectionResources;

  /**
   * Database connection to which the data is written.
   */
  private Connection connection;

  /**
   * The SQL dialect
   */
  private SqlDialect sqlDialect;

  /**
   * Used to run SQL statements.
   */
  private final SqlScriptExecutor sqlExecutor;

  /**
   * DataSource providing database connections.
   */
  private final DataSource dataSource;

  private boolean wasAutoCommit;



  /**
   * Creates an instance of the database data set consumer.
   *
   * @param connectionResources Provides database connection interfaces.
   * @param sqlScriptExecutorProvider a provider of {@link SqlScriptExecutor}s.
   */
  public DatabaseDataSetConsumer(ConnectionResources connectionResources, SqlScriptExecutorProvider sqlScriptExecutorProvider) {
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
  public DatabaseDataSetConsumer(ConnectionResources connectionResources, SqlScriptExecutorProvider sqlScriptExecutorProvider, DataSource dataSource) {
    super();
    this.connectionResources = connectionResources;
    this.dataSource = dataSource;
    this.sqlExecutor = sqlScriptExecutorProvider.get();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
   */
  @Override
  public void open() {
    log.debug("Opening database connection");
    try {
      connection = dataSource.getConnection();
      wasAutoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      sqlDialect = connectionResources.sqlDialect();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error opening connection", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    try {
      try {
        if (CloseState.COMPLETE.equals(closeState)) {
          log.debug("Closing and committing");
          connection.commit();
        } else {
          log.debug("Rolling back");
          connection.rollback();
        }
      } finally {
        connection.setAutoCommit(wasAutoCommit);
        connection.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Error committing and closing", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    TableLoader.builder()
      .withConnection(connection)
      .withSqlScriptExecutor(sqlExecutor)
      .withDialect(sqlDialect)
	  .explicitCommit("H2".equals(connectionResources.getDatabaseType()))
      .truncateBeforeLoad()
      .insertingWithPresetAutonums()
      .forTable(table)
      .load(records);
  }


  /**
   * @return the dataSource
   */
  DataSource getDataSource() {
    return dataSource;
  }


  /**
   * @return the sqlExecutor
   */
  SqlScriptExecutor getSqlExecutor() {
    return sqlExecutor;
  }

}
