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

package org.alfasoftware.morf.jdbc.mysql;

import java.sql.Connection;
import java.util.Stack;

import javax.sql.XADataSource;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Optional;


/**
 * Support for MySQL database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class MySql extends AbstractDatabaseType {

  private static final Log log = LogFactory.getLog(MySql.class);

  public static final String IDENTIFIER = "MY_SQL";


  /**
   * Constructor.
   */
  public MySql() {
    super("com.mysql.jdbc.Driver", IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#formatJdbcUrl(org.alfasoftware.morf.jdbc.JdbcUrlElements)
   */
  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    return "jdbc:mysql://" + jdbcUrlElements.getHostName() + (jdbcUrlElements.getPort() == 0 ? "" : ":" + jdbcUrlElements.getPort()) + "/" + jdbcUrlElements.getDatabaseName() + "?rewriteBatchedStatements=true";
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#openSchema(Connection, String, String) 
   */
  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new MySqlMetaDataProvider(connection, databaseName);
  }


  /**
   * Returns a MySQL XA data source. Note that this method may fail at
   * run-time if {@code MysqlXADataSource} is not available on the classpath.
   *
   * @throws IllegalStateException If the data source cannot be created.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(java.lang.String,
   *      java.lang.String, java.lang.String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    try {
      log.info("Initialising MySQL XA data source...");
      XADataSource dataSource = (XADataSource) Class.forName("com.mysql.jdbc.jdbc2.optional.MysqlXADataSource").newInstance();
      dataSource.getClass().getMethod("setURL", String.class).invoke(dataSource, jdbcUrl);
      dataSource.getClass().getMethod("setUser", String.class).invoke(dataSource, username);
      dataSource.getClass().getMethod("setPassword", String.class).invoke(dataSource, password);
      //see http://www.atomikos.com/Documentation/KnownProblems#MySQL_XA_bug
      //did not have to set com.atomikos.icatch.serial_jta_transactions=false
      dataSource.getClass().getMethod("setPinGlobalTxToPhysicalConnection", boolean.class).invoke(dataSource, true);
      return dataSource;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create Oracle XA data source", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#sqlDialect(java.lang.String)
   */
  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new MySqlDialect();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#matchesProduct(java.lang.String)
   */
  @Override
  public boolean matchesProduct(String product) {
    return product.equalsIgnoreCase("MySQL");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.AbstractDatabaseType#extractJdbcUrl(java.lang.String)
   */
  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String jdbcUrl) {
    Stack<String> splitURL = splitJdbcUrl(jdbcUrl);

    String scheme = splitURL.pop();

    if (!scheme.equalsIgnoreCase("mysql")) {
      return Optional.absent();
    }

    if (!splitURL.pop().equals("://")) {
      // If the next characters are not "://" then die
      throw new IllegalArgumentException("Expected '//' to follow the scheme name in [" + jdbcUrl + "]");
    }

    JdbcUrlElements.Builder connectionDetails = extractHostAndPort(splitURL);

    // Now get the path
    String path = extractPath(splitURL);

    // Set the database name removing the first character (which will be a /)
    // Remove anything after a ? as well, as these are parameters
    String[] pathParts = path.split("\\?");
    String databaseName = pathParts.length == 0 ? "" : pathParts[0];
    connectionDetails.withDatabaseName(databaseName);

    return Optional.of(connectionDetails.build());
  }
}