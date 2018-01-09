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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;


import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Allows identification of a database from its metadata.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class DatabaseTypeIdentifier {
  private static final Log log = LogFactory.getLog(DatabaseTypeIdentifier.class);

  private final DataSource dataSource;


  /**
   * Construct an identification helper.
   *
   * @param dataSource Data source to query metadata for.
   */
  public DatabaseTypeIdentifier(DataSource dataSource) {
    super();
    this.dataSource = dataSource;
  }


  /**
   * Try to identify the database type from connection metadata.
   *
   * @return Database type, or null if none.
   */
  public Optional<DatabaseType> identifyFromMetaData() {
    try {
      Connection connection = dataSource.getConnection();
      try {
        DatabaseMetaData metaData = connection.getMetaData();
        String product = metaData.getDatabaseProductName();
        String versionString = metaData.getDatabaseProductVersion().replaceAll("\n", "\\\\n");
        int    versionMajor  = metaData.getDatabaseMajorVersion();
        int    versionMinor  = metaData.getDatabaseMinorVersion();
        log.info(String.format("Database product = [%s], version = [%s], v%d.%d", product, versionString, versionMajor, versionMinor));

        return DatabaseType.Registry.findByProductName(product);

      } finally {
        connection.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL exception", e);
    }
  }
}
