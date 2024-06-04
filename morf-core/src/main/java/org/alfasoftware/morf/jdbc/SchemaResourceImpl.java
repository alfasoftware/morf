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
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.dataset.SchemaAdapter;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;

/**
 * Implementation of {@link SchemaResource} Tracking the closing of DB Connections.
 */
public final class SchemaResourceImpl extends SchemaAdapter implements SchemaResource {
  private final Connection connection;
  private final boolean wasAutoCommit;


  /**
   * Create an implementation of {@link SchemaResource}.
   *
   * @param dataSource The data source to supply a Connection.
   * @param connectionResources The {@link ConnectionResources} details.
   * @return A {@link SchemaResource}.
   */
  public static SchemaResource create(DataSource dataSource, ConnectionResources connectionResources) {
    try {
      // this connection is closed by SchemaResourceImpl
      Connection connection = dataSource.getConnection();
      Schema schema = DatabaseType.Registry.findByIdentifier(connectionResources.getDatabaseType()).openSchema(connection, connectionResources.getDatabaseName(), connectionResources.getSchemaName());
      return new SchemaResourceImpl(connection, schema);
    } catch (SQLException e) {
      throw new RuntimeSqlException("Getting connection", e);
    }
  }


  /**
   * Internal constructor.
   */
  private SchemaResourceImpl(Connection connection, Schema schema) throws SQLException {
    super(schema);
    this.connection = connection;

    // we need to set this connection to not auto-commit
    wasAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
  }


  /**
   * @see org.alfasoftware.morf.metadata.SchemaResource#close()
   */
  @Override
  public void close() {
    try {
      // restore auto-commit state
      connection.commit();
      connection.setAutoCommit(wasAutoCommit);
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeSqlException("Closing", e);
    }
  }


  @Override
  public Optional<DatabaseMetaDataProvider> getDatabaseMetaDataProvider() {
    if (delegate instanceof DatabaseMetaDataProvider) {
      DatabaseMetaDataProvider metaDataProvider = (DatabaseMetaDataProvider)delegate;
      return Optional.of(metaDataProvider);
    }

    return SchemaResource.super.getDatabaseMetaDataProvider();
  }


  /**
   * Introduced to allow access to meta-data when using auto-healing with Oracle. See MORF-98.
   * @return the table collection supplier.
   */
  @Override
  public Optional<TableCollectionSupplier> getTableCollectionSupplier() {
    return Optional.of(delegate);
  }
}