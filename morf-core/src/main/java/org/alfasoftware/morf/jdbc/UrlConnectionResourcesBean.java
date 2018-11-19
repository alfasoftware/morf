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
import javax.xml.crypto.Data;

import org.alfasoftware.morf.metadata.SchemaResource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * A {@link ConnectionResources} implementation which uses only the JDBC URL,
 * where possible.
 * 
 * <p>Create using
 * {@link DatabaseType.Registry#urlToConnectionResources(String)}.</p>
 */
public class UrlConnectionResourcesBean implements ConnectionResources {

  private static final Log log = LogFactory.getLog(UrlConnectionResourcesBean.class);
  
  private final String url;
  private final String databaseType;
  private final String databaseName;
  
  private String schemaName;
  private int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
  private int statementPoolingMaxStatements;

  /**
   * Constructor intended for use by implementations of {@link DatabaseType}.
   * 
   * @param url The JDBC URL.
   * @param databaseType The database type identifier.
   * @param databaseName The name of the database. Used for tagging metadata. Can be anything.
   */
  public UrlConnectionResourcesBean(String url, String databaseType, String databaseName) {
    this.url = url;
    this.databaseType = databaseType;
    this.databaseName = databaseName;
  }

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
    return findDatabaseType().getXADataSource(getJdbcUrl(), null, null);
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
   * @return a formatted jdbc url string.
   */
  public String getJdbcUrl() {
    return url;
  }
  

  @Override
  public String getDatabaseType() {
    return databaseType;
  }


  @Override
  public String getSchemaName() {
    return schemaName;
  }
  
  
  /**
   * @param schemaName the schemaName to set
   */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }


  @Override
  public String getDatabaseName() {
    return databaseName;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.ConnectionResources#getStatementPoolingMaxStatements()
   */
  @Override
  public int getStatementPoolingMaxStatements() {
    return statementPoolingMaxStatements;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractConnectionResources#setStatementPoolingMaxStatements(int)
   */
  public void setStatementPoolingMaxStatements(int statementPoolingMaxStatements) {
    this.statementPoolingMaxStatements = statementPoolingMaxStatements;
  }

  
  /**
   * Gets the transaction isolation level to use (e.g. {@link Connection#TRANSACTION_READ_COMMITTED}).
   * 
   * <p>Defaults to {@link Connection#TRANSACTION_READ_COMMITTED} if not specified.</p>
   * 
   * @return The isolation level.
   */
  public int getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }
  
  
  /**
   * Sets the transaction isolation level to use (e.g. {@link Connection#TRANSACTION_READ_COMMITTED}).
   * 
   * <p>Defaults to {@link Connection#TRANSACTION_READ_COMMITTED} if not specified.</p>
   * 
   * @param transactionIsolationLevel The isolation level.
   */
  public void setTransactionIsolationLevel(int transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
  }


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
      return getConnection(null, null);
    }

    /**
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      log.info("Opening new database connection to [" + UrlConnectionResourcesBean.this.getJdbcUrl() + "] with username [" + username + "]");
      loadJdbcDriver();
      return openConnection(username, password);
    }


    private void loadJdbcDriver() {
      String driverClassName = findDatabaseType().driverClassName();
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        log.warn("Failed to load JDBC driver [" + driverClassName + "] for DatabaseType [" + UrlConnectionResourcesBean.this.getDatabaseType() + "]", e);
      }
    }


    private Connection openConnection(String username, String password) throws SQLException {
      Connection connection;
      try {
        connection = username == null
            ? DriverManager.getConnection(UrlConnectionResourcesBean.this.getJdbcUrl())
            : DriverManager.getConnection(UrlConnectionResourcesBean.this.getJdbcUrl(), username, password);
      } catch (SQLException se) {
        log.error(String.format("Unable to connect to URL: %s, with user: %s", UrlConnectionResourcesBean.this.getJdbcUrl(), username));
        throw se;
      }
      try {
        connection.setTransactionIsolation(transactionIsolationLevel);
        return connection;
      } catch (Exception e) {
        connection.close();
        throw e;
      }
    }
  }
}