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
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.alfasoftware.morf.metadata.SchemaResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * Generic, base implementation of {@link ConnectionResources}.  Allows the actual configuration
 * data to be derived from anywhere and then ensures that the correct dialect, database type,
 * schema and data sources are created on request.  Extend this when you already have the
 * connection details available but need a {@link DataSource} created.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public abstract class AbstractConnectionResources implements ConnectionResources {

  private static final Log log = LogFactory.getLog(AbstractConnectionResources.class);


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#sqlDialect()
   */
  @Override
  public final SqlDialect sqlDialect() {
    return findDatabaseType().sqlDialect(getSchemaName());
  }


  private DatabaseType findDatabaseType() {
    return DatabaseType.Registry.findByIdentifier(getDatabaseType());
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getDataSource()
   */
  @Override
  public DataSource getDataSource() {
    return new ConnectionDetailsDataSource();
  }


  /**
   * @return {@link XADataSource} created for this {@link ConnectionResources}
   */
  public final XADataSource getXADataSource() {
    Preconditions.checkNotNull(getDatabaseType(), "Cannot create XADataSource without defined DatabaseType");
    return findDatabaseType().getXADataSource(getJdbcUrl(), getUserName(), getPassword());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#openSchemaResource()
   */
  @Override
  public final SchemaResource openSchemaResource() {
    return openSchemaResource(getDataSource());
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#openSchemaResource(DataSource)
   */
  @Override
  public final SchemaResource openSchemaResource(DataSource dataSource) {
    return SchemaResourceImpl.create(dataSource, this);
  }


  /**
   * @return the databaseType
   */
  @Override
  public abstract String getDatabaseType();


  /**
   * @param databaseType the databaseType to set
   */
  public abstract void setDatabaseType(String databaseType);


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getStatementPoolingMaxStatements()
   */
  @Override
  public abstract int getStatementPoolingMaxStatements();


  /**
   * Sets the maximum number of statements to cache in a pool.
   *
   * @param statementPoolingMaxStatements the number of statements to cache.
   */
  public abstract void setStatementPoolingMaxStatements(int statementPoolingMaxStatements);


  /**
   * @return a formatted jdbc url string.
   */
  public final String getJdbcUrl() {
    return getDatabaseType() == null
        ? null
        : findDatabaseType().formatJdbcUrl(
            JdbcUrlElements.forDatabaseType(getDatabaseType())
              .withHost(getHostName())
              .withPort(getPort())
              .withDatabaseName(getDatabaseName())
              .withInstanceName(getInstanceName())
              .withSchemaName(getSchemaName())
              .build()
          );
  }


  /**
   * @return the hostName
   */
  public abstract String getHostName();


  /**
   * @param hostName the hostName to set
   */
  public abstract void setHostName(String hostName);


  /**
   * @return the port (or zero to signify default port)
   */
  public abstract int getPort();


  /**
   * @param port the port (or zero to signify default port)
   */
  public abstract void setPort(int port);


  /**
   * Several RDBMS allow you to run several instances of the database
   * server software on the same machine all behind the same network connection
   * details. This identifies the instance of the database server on the specified host.
   *
   * <ul>
   * <li>On Oracle, this is the SID and it is mandatory.</li>
   * <li>On SQL Server, this is the instance name and can be blank for the default instance.</li>
   * </ul>
   *
   * @return the name of the instance to connect to.
   */
  public abstract String getInstanceName();


  /**
   * Several RDBMS allow you to run several instances of the database
   * server software on the same machine all behind the same network connection
   * details. This identifies the instance of the database server on the specified host.
   *
   * <ul>
   * <li>On Oracle, this is the SID and it is mandatory.</li>
   * <li>On SQL Server, this is the instance name and can be blank for the default instance.</li>
   * </ul>
   *
   * @param instanceName the name of the instance to connect to.
   */
  public abstract void setInstanceName(String instanceName);


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getDatabaseName()
   */
  @Override
  public abstract String getDatabaseName();


  /**
   * For RDBMS where there might be several databases within an instance of
   * the database server (SQL Server).
   *
   * <ul>
   * <li>On Oracle this is not required - the next level within an instance (SID) is a schema.</li>
   * <li>On SQL Server this is the "database".</li>
   * <li>On MySQL, this is the "database".</li>
   * <li>On DB2/400, this is the data library</li>
   * </ul>
   *
   * @param databaseName the name of the database to connect to.
   */
  public abstract void setDatabaseName(String databaseName);


  /**
   * @return the schemaName
   */
  @Override
  public abstract String getSchemaName();


  /**
   * @param schemaName the schemaName to set
   */
  public abstract void setSchemaName(String schemaName);


  /**
   * @return the user name to log on with.
   */
  public abstract String getUserName();


  /**
   * @param userName the user name to log on with.
   */
  public abstract void setUserName(String userName);


  /**
   * @return the password to log on with.
   */
  public abstract String getPassword();


  /**
   * @param password the password to log on with.
   */
  public abstract void setPassword(String password);


  /**
   * Implementation of data source based on this {@link ConnectionResources}.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private final class ConnectionDetailsDataSource extends DataSourceAdapter {

    /**
     * @see javax.sql.DataSource#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException {
      return getConnection(AbstractConnectionResources.this.getUserName(), AbstractConnectionResources.this.getPassword());
    }

    /**
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      log.info("Opening new database connection to [" + AbstractConnectionResources.this.getJdbcUrl() + "] with username [" + username + "]");

      // Load the JDBC driver for this DatabaseType
      String driverClassName = findDatabaseType().driverClassName();
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        log.warn("Failed to load JDBC driver [" + driverClassName + "] for DatabaseType [" + AbstractConnectionResources.this.getDatabaseType() + "]", e);
      }

      Connection connection;

      try {
        connection = DriverManager.getConnection(AbstractConnectionResources.this.getJdbcUrl(), username, password);
      } catch (SQLException se) {
        log.error(String.format("Unable to connect to URL: %s, with user: %s", AbstractConnectionResources.this.getJdbcUrl(), username));
        throw se;
      }

      // Attempt to switch to read-committed. This is the default on many platforms, but not on MySQL.
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      return connection;
    }
  }
}