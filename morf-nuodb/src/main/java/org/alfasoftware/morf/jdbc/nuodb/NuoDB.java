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

package org.alfasoftware.morf.jdbc.nuodb;

import java.sql.Connection;

import javax.sql.XADataSource;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;

import com.google.common.base.Optional;

/**
 * Support for NuoDB database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public final class NuoDB extends AbstractDatabaseType {

  public static final String IDENTIFIER = "NUODB";


  /**
   * Constructor.
   */
  public NuoDB() {
    super("com.nuodb.jdbc.Driver", IDENTIFIER);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#formatJdbcUrl(org.alfasoftware.morf.jdbc.JdbcUrlElements)
   */
  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    return "jdbc:com.nuodb://" + jdbcUrlElements.getHostName() + ":" + jdbcUrlElements.getPort() + "/" + jdbcUrlElements.getDatabaseName() + "?isolation=read_committed&schema=" + jdbcUrlElements.getSchemaName();
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#openSchema(java.sql.Connection, java.lang.String, java.lang.String)
   */
  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new NuoDBMetaDataProvider(connection, schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    throw new UnsupportedOperationException("XA not supported in NuoDB at present");
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#sqlDialect(java.lang.String)
   */
  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new NuoDBDialect(schemaName);
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#matchesProduct(java.lang.String)
   */
  @Override
  public boolean matchesProduct(String product) {
    return product.equalsIgnoreCase("NUODB");
  }


  /**
   * FIXME this should really be supported to allow use of the data transfer UI.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#extractJdbcUrl(java.lang.String)
   */
  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String url) {
    return Optional.absent();
  }
}