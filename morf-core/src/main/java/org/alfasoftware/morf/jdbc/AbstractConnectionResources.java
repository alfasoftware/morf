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


import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executor;

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
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getFetchSizeForBulkSelects()
   */
  @Override
  public abstract Integer getFetchSizeForBulkSelects();


  /**
   * Sets the JDBC Fetch Size to use when performing bulk select operations, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelects()}.
   *
   * @param fetchSizeForBulkSelects the JDBC fetch size to use.
   */
  public abstract void setFetchSizeForBulkSelects(Integer fetchSizeForBulkSelects);


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()
   */
  @Override
  public abstract Integer getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming();


  /**
   * Sets the JDBC Fetch Size to use when performing bulk select operations while allowing connection use, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()}.
   *
   * @param fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming the JDBC fetch size to use.
   */
  public abstract void setFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming(Integer fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming);


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
      log.info("Opening new database connection to [" + AbstractConnectionResources.this.getJdbcUrl() + "] with username [" + username + "] for schema [" + AbstractConnectionResources.this.getSchemaName() + "]");
      loadJdbcDriver();
      Connection connection = openConnection(username, password);
      return log.isDebugEnabled() ? new LoggingConnection(connection) : connection;
    }


    private void loadJdbcDriver() {
      String driverClassName = findDatabaseType().driverClassName();
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        log.warn("Failed to load JDBC driver [" + driverClassName + "] for DatabaseType [" + AbstractConnectionResources.this.getDatabaseType() + "]", e);
      }
    }


    private Connection openConnection(String username, String password) throws SQLException {
      Connection connection;
      try {
        connection = DriverManager.getConnection(AbstractConnectionResources.this.getJdbcUrl(), username, password);
      } catch (SQLException se) {
        log.error(String.format("Unable to connect to URL: %s, with user: %s", AbstractConnectionResources.this.getJdbcUrl(), username));
        throw se;
      }
      try {
        // Attempt to switch to read-committed. This is the default on many platforms, but not on MySQL.
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return connection;
      } catch (Exception e) {
        connection.close();
        throw e;
      }
    }
  }


  private static final class LoggingConnection implements Connection {

    final int id = new Random().nextInt(0xFFFF);

    final Connection connection;

    public LoggingConnection(Connection connection) {
      log.debug("Opening database connection " + id);
      this.connection = connection;
    }

    @Override
    public void close() throws SQLException {
      log.debug("Closing database connection " + id);
      connection.close();
    }

    @Override
    public void commit() throws SQLException {
      connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
      connection.rollback();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
      return connection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
      return connection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
      connection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      connection.releaseSavepoint(savepoint);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
      connection.abort(executor);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
      connection.setSchema(schema);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
      connection.setAutoCommit(autoCommit);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      connection.setReadOnly(readOnly);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
      connection.setHoldability(holdability);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      connection.setTransactionIsolation(level);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
      connection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
      connection.setClientInfo(properties);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
      connection.setCatalog(catalog);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      return connection.nativeSQL(sql);
    }

    @Override
    public Statement createStatement() throws SQLException {
      return connection.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return connection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      return connection.prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return connection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return connection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return connection.prepareStatement(sql, columnNames);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      return connection.prepareCall(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
      return connection.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      return connection.isValid(timeout);
    }

    @Override
    public boolean isClosed() throws SQLException {
      return connection.isClosed();
    }

    @Override
    public String getSchema() throws SQLException {
      return connection.getSchema();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      return connection.getAutoCommit();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      return connection.getMetaData();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      return connection.getTransactionIsolation();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      return connection.getNetworkTimeout();
    }

    @Override
    public int getHoldability() throws SQLException {
      return connection.getHoldability();
    }

    @Override
    public String getCatalog() throws SQLException {
      return connection.getCatalog();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      return connection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
      return connection.getClientInfo();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return connection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      connection.clearWarnings();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
      return connection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      connection.setTypeMap(map);
    }

    @Override
    public Clob createClob() throws SQLException {
      return connection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
      return connection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
      return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
      return connection.createSQLXML();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return connection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return connection.createStruct(typeName, attributes);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return connection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return connection.isWrapperFor(iface);
    }
  }
}