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

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Utility methods for database type identifier tests.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class DatabaseTypeIdentifierTestUtils {

  /**
   * Create a mock data source reporting the correct {@link DatabaseMetaData}.
   *
   * @param product {@link DatabaseMetaData#getDatabaseProductName()}
   * @param versionString {@link DatabaseMetaData#getDatabaseProductVersion()}
   * @param versionMajor {@link DatabaseMetaData#getDatabaseMajorVersion()}
   * @param versionMinor {@link DatabaseMetaData#getDatabaseMinorVersion()}
   * @return A data source which will report the above (and nothing else).
   * @throws SQLException as part of mocked signature.
   */
  public static DataSource mockDataSourceFor(String product, String versionString, int versionMajor, int versionMinor) throws SQLException {
    DataSource dataSource = mock(DataSource.class, RETURNS_DEEP_STUBS);
    when(dataSource.getConnection().getMetaData().getDatabaseProductName()).thenReturn(product);
    when(dataSource.getConnection().getMetaData().getDatabaseProductVersion()).thenReturn(versionString);
    when(dataSource.getConnection().getMetaData().getDatabaseMajorVersion()).thenReturn(versionMajor);
    when(dataSource.getConnection().getMetaData().getDatabaseMinorVersion()).thenReturn(versionMinor);

    return dataSource;
  }
}