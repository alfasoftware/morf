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

package org.alfasoftware.morf.jdbc.sqlserver;

import java.sql.Connection;
import java.util.Stack;

import javax.sql.XADataSource;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Optional;


/**
 * Support for Sql Server database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class SqlServer extends AbstractDatabaseType {

  private static final Log log = LogFactory.getLog(SqlServer.class);

  public static final String IDENTIFIER = "SQL_SERVER";


  /**
   * Constructor.
   */
  public SqlServer() {
    super("com.microsoft.sqlserver.jdbc.SQLServerDriver", IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#formatJdbcUrl(org.alfasoftware.morf.jdbc.JdbcUrlElements)
   */
  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    return "jdbc:sqlserver://" + jdbcUrlElements.getHostName() + (StringUtils.isNotBlank(jdbcUrlElements.getInstanceName()) ? "\\" + jdbcUrlElements.getInstanceName() : "")
        + (jdbcUrlElements.getPort() == 0 ? "" : ":" + jdbcUrlElements.getPort()) + (StringUtils.isNotBlank(jdbcUrlElements.getDatabaseName()) ? ";database=" + jdbcUrlElements.getDatabaseName() : "");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#openSchema(java.sql.Connection,
   *      java.lang.String, java.lang.String)
   */
  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new SqlServerMetaDataProvider(connection, schemaName);
  }


  /**
   * Returns a SQL Server XA data source. Note that this method may fail at
   * run-time if {@code SQLServerXADataSource} is not available on the classpath.
   *
   * @throws IllegalStateException If the data source cannot be created.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    try {
      log.info("Initialising SQL Server XA data source...");
      XADataSource dataSource = (XADataSource) Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXADataSource").newInstance();
      dataSource.getClass().getMethod("setURL", String.class).invoke(dataSource, jdbcUrl);
      dataSource.getClass().getMethod("setUser", String.class).invoke(dataSource, username);
      dataSource.getClass().getMethod("setPassword", String.class).invoke(dataSource, password);
      return dataSource;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create SQL Server XA data source", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#sqlDialect(java.lang.String)
   */
  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new SqlServerDialect(schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#matchesProduct(java.lang.String)
   */
  @Override
  public boolean matchesProduct(String product) {
    return product.toLowerCase().contains("microsoft sql server");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractDatabaseType#extractJdbcUrl(java.lang.String)
   */
  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String jdbcUrl) {
    Stack<String> splitURL = splitJdbcUrl(jdbcUrl);

    String scheme = splitURL.pop();

    if (!scheme.equalsIgnoreCase("sqlserver")) {
      return Optional.absent();
    }

    if (!splitURL.pop().equals("://")) {
      // If the next characters are not "://" then die
      throw new IllegalArgumentException("Expected '//' to follow the scheme name in [" + jdbcUrl + "]");
    }

    JdbcUrlElements.Builder connectionDetails = JdbcUrlElements.forDatabaseType(identifier());

    // The next bit is the host name
    String host = splitURL.pop();
    if (host.contains("\\")) {
      // Which may be split between host and instance
      String[] hostInstancePair = host.split("\\\\");
      connectionDetails.withHost(hostInstancePair[0]);
      connectionDetails.withInstanceName(hostInstancePair[1]);
    } else {
      connectionDetails.withHost(host);
    }

    // If there's a ":" then this will be the port
    if (splitURL.peek().equals(":")) {
      splitURL.pop(); // Remove the delimiter
      connectionDetails.withPort(Integer.parseInt(splitURL.pop()));
    }

    // Now get the path
    String path = extractPath(splitURL);
    String[] nameValuePairsArray = path.split("&");
    String databaseName = "";
    for (int i = 0; i < nameValuePairsArray.length; i++) {
      String entry = nameValuePairsArray[i];

      String name = entry.substring(0, entry.indexOf('='));
      if (name.equalsIgnoreCase("database")) {
        databaseName = entry.substring(entry.indexOf('=') + 1);
        break;
      }
    }

    connectionDetails.withDatabaseName(databaseName);

    return Optional.of(connectionDetails.build());
  }
}