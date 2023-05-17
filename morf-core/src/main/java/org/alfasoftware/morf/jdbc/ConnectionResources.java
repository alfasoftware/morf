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

import java.sql.ResultSet;

import javax.sql.DataSource;

import org.alfasoftware.morf.metadata.SchemaResource;

/**
 * Provides access to key database access resources.  Implement directly in applications
 * where the {@link DataSource} and connection details are already available.  For new
 * applications, it is advised that you either extend {@link AbstractConnectionResources}
 * or create an instance of {@link ConnectionResourcesBean}.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public interface ConnectionResources {


  /**
   * Open a schema resource for the database described by this object.
   *
   * <p><strong>Resources returned by this method must be explicitly closed.</strong></p>
   *
   * @return A meta data provider..
   */
  public SchemaResource openSchemaResource();


  /**
   * Open a schema resource for the database described by this object.
   *
   * <p><strong>Resources returned by this method must be explicitly closed.</strong></p>
   *
   * @param dataSource DataSource to use for the schema.
   * @return A meta data provider..
   */
  public SchemaResource openSchemaResource(DataSource dataSource);


  /**
   * Access an SQL statement generator for the database described by this object.
   * This is a shortcut to calling {@link DatabaseType#sqlDialect(String)}  with
   * {@link #getSchemaName()} as the parameter.
   *
   * @return An sql generator that will adhere to syntax rules for the specified database.
   */
  public SqlDialect sqlDialect();


  /**
   * Calling getDataSource().getConnection should be equivalent to calling openConnection().
   *
   * @return A jdbc data source.
   */
  public DataSource getDataSource();


  /**
   * @return The database type. This is required for functionality that is inevitably database specific.
   */
  public String getDatabaseType();


  /**
   * @return The schema name. Not required on all databases.
   */
  public String getSchemaName();


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
   * @return the name of the database to connect to.
   */
  public String getDatabaseName();


  /**
   * The number of PreparedStatements will be cached for a single pooled
   * Connection. If zero, statement caching will not be enabled.
   *
   * @return the number of statements to cache per connection.
   */
  public int getStatementPoolingMaxStatements();

  /**
   *
   * The JDBC Fetch Size to use when performing bulk select operations, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelects()}.
   * The default behaviour for this method is interpreted as "not set" rather than 0.
   *  @return The number of rows to try and fetch at a time when
   *          performing bulk select operations.
   */
  public default Integer getFetchSizeForBulkSelects(){
    return null;
  };

  /**
   * Sets the JDBC Fetch Size to use when performing bulk select operations, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelects()}.
   * The default behaviour for this method is interpreted as not setting the value.
   * @param fetchSizeForBulkSelects the JDBC fetch size to use.
   */
  public default void setFetchSizeForBulkSelects(Integer fetchSizeForBulkSelects){
  }

  /**
   *
   * The JDBC Fetch Size to use when performing bulk select operations while allowing connection use, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()}.
   * The default behaviour for this method is interpreted as "not set" rather than 0.
   *  @return The number of rows to try and fetch at a time (default) when
   *         performing bulk select operations and needing to use the connection while
   *         the {@link ResultSet} is open.
   */
  public default Integer getFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming(){
    return null;
  };


  /**
   * Sets the JDBC Fetch Size to use when performing bulk select operations while allowing connection use, intended to replace the default in {@link SqlDialect#fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming()}.
   * The default behaviour for this method is interpreted as not setting the value.
   * @param fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming the JDBC fetch size to use.
   */
  public default void setFetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming(Integer fetchSizeForBulkSelectsAllowingConnectionUseDuringStreaming){
  }
}
