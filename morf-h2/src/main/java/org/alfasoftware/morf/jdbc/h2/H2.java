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

package org.alfasoftware.morf.jdbc.h2;

import java.io.File;
import java.sql.Connection;

import javax.sql.XADataSource;

import org.apache.commons.lang.StringUtils;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import com.google.common.base.Optional;

/**
 * Support for H2 database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class H2 extends AbstractDatabaseType {

  public static final String IDENTIFIER = "H2";


  /**
   * Constructor.
   */
  public H2() {
    super("org.h2.Driver", IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#formatJdbcUrl(JdbcUrlElements)
   */
  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    // http://www.h2database.com/html/features.html#database_url

    StringBuilder builder = new StringBuilder()
      .append("jdbc:h2:");

    if (StringUtils.isNotBlank(jdbcUrlElements.getHostName()) && !"localhost".equals(jdbcUrlElements.getHostName()) || jdbcUrlElements.getPort() > 0) {
      builder
        .append("tcp://")
        .append(jdbcUrlElements.getHostName())
        .append(jdbcUrlElements.getPort() == 0 ? "" : ":" + jdbcUrlElements.getPort())
        .append("/mem:") // this means we're going to use a remote in-memory DB which isn't ideal
        .append(jdbcUrlElements.getDatabaseName());
    } else {
      // no host, try the instanceName
      if (StringUtils.isBlank(jdbcUrlElements.getInstanceName())) {
        builder
          .append("mem:")
          .append(jdbcUrlElements.getDatabaseName());
      } else {
        // Allow the instanceName to have a trailing slash, or not.
        builder
          .append("file:")
          .append(jdbcUrlElements.getInstanceName())
          .append(jdbcUrlElements.getInstanceName().endsWith(File.separator) ? "" : File.separator)
          .append(jdbcUrlElements.getDatabaseName());
      }
    }

    // The DB_CLOSE_DELAY=-1 prevents the database being lost when the last connection is closed.
    // The MVCC=TRUE allows higher concurrency - delete, insert and update operations will only issue a shared lock on the table.
    // The DEFAULT_LOCK_TIMEOUT=60000 sets the default lock timeout to 60
    //    seconds. When the value is not set, it takes default
    //    org.h2.engine.Constants.INITIAL_LOCK_TIMEOUT=2000 value
    builder.append(";DB_CLOSE_DELAY=-1;MVCC=TRUE;DEFAULT_LOCK_TIMEOUT=60000");

    return builder.toString();
  }

  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#openSchema(Connection, String, String)
   */
  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new H2MetaDataProvider(connection);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    throw new UnsupportedOperationException("H2 does not fully support XA connections. "
        + "It may cause many different problems while running integration tests with H2. "
        + "Please switch off Atomikos or change database engine. See WEB-31172 for details");
    // JdbcDataSource xaDataSource = new JdbcDataSource();
    // xaDataSource.setURL(jdbcUrl);
    // xaDataSource.setUser(username);
    // xaDataSource.setPassword(password);
    // return xaDataSource;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#sqlDialect(java.lang.String)
   */
  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new H2Dialect();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#matchesProduct(java.lang.String)
   */
  @Override
  public boolean matchesProduct(String product) {
    return product.equalsIgnoreCase("H2");
  }


  /**
   * We don't need to support extracting connection details from H2.  It's only
   * used for in-memory databases currently.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#extractJdbcUrl(java.lang.String)
   */
  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String url) {
    return Optional.absent();
  }
}