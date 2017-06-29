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

import static org.alfasoftware.morf.jdbc.DatabaseTypeIdentifierTestUtils.mockDataSourceFor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.DatabaseTypeIdentifier;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import com.google.common.base.Optional;

public class TestMySqlDatabaseType {

  private DatabaseType databaseType;


  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(MySql.IDENTIFIER);
  }


  /**
   * Tests MySQL JDBC URL formatting.
   */
  @Test
  public void testMySqlUrlFormatting() {
    assertEquals(
      "MySQL database url",
      "jdbc:mysql://localhost/data?rewriteBatchedStatements=true",
      databaseType.formatJdbcUrl(
        JdbcUrlElements.forDatabaseType(MySql.IDENTIFIER)
          .withHost("localhost")
          .withDatabaseName("data")
          .build()
      )
    );
  }

  /**
   * Test identification of a database platform.
   *
   * @throws SQLException as part of tested contract.
   */
  @Test
  public void testIdentifyFromMetaData() throws SQLException {
    // -- Unknown and resource management...
    //
    DataSource dataSource = mockDataSourceFor("FictiousDB", "9.9.9", 9, 9);
    assertEquals(Optional.absent(), new DatabaseTypeIdentifier(dataSource).identifyFromMetaData());
    verify(dataSource.getConnection()).close();

    // -- Support platforms...
    //
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("MySQL", "5.5.29", 5, 5)).identifyFromMetaData().get());
  }


  /**
   * Tests MySQL formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromMySQL() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:mysql://localhost:3306/alfa").get();

    assertEquals("Should have the correct type", MySql.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "localhost", result.getHostName());
    assertEquals("Should have the correct port", 3306, result.getPort());
    assertEquals("Should have the correct database name", "alfa", result.getDatabaseName());
  }


  /**
   * Tests MySQL formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromMySQLNoPort() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:mysql://localhost/alfa").get();

    assertEquals("Should have the correct type", MySql.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "localhost", result.getHostName());
    assertEquals("Should have the correct database name", "alfa", result.getDatabaseName());
  }


  /**
   * Checks the bidirectionality of our URL split and combine behaviour.
   */
  public void testUrlRoundTrips() {
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(MySql.IDENTIFIER).withHost("hostname").withPort(3306).withDatabaseName("databasename").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(MySql.IDENTIFIER).withHost("hostname").withDatabaseName("databasename").build());
  }


  /**
   * Tests a URL <- split -> URL round-trip.
   */
  private void comparerUrlRoundtrips(JdbcUrlElements jdbcUrlElements) {
    String jdbcURL = databaseType.formatJdbcUrl(jdbcUrlElements);
    JdbcUrlElements cd = databaseType.extractJdbcUrl(jdbcURL).get();
    assertEquals(jdbcUrlElements, cd);
  }
}