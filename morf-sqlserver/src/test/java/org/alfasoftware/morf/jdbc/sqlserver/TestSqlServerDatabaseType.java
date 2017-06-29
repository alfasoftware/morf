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

public class TestSqlServerDatabaseType {

  private DatabaseType databaseType;


  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(SqlServer.IDENTIFIER);
  }


  /**
   * Tests SQL Server JDBC URL formatting.
   */
  @Test
  public void testSqlServerUrlFormatting() {
    assertEquals(
      "SQL Server database url",
      "jdbc:sqlserver://localhost;database=data",
      databaseType.formatJdbcUrl(
        JdbcUrlElements.forDatabaseType(SqlServer.IDENTIFIER)
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
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("Microsoft SQL Server", "10.50.2500", 10, 50)).identifyFromMetaData().get());
  }


  /**
   * Tests SQL Server formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromSQLServer() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:sqlserver://somesqlserver.co.uk:2309;database=testing").get();

    assertEquals("Should have the correct type", SqlServer.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "somesqlserver.co.uk", result.getHostName());
    assertEquals("Should have the correct port", 2309, result.getPort());
    assertEquals("Should have the correct database name", "testing", result.getDatabaseName());

    result = databaseType.extractJdbcUrl("jdbc:sqlserver://somesqlserver.co.uk\\blah:2309;database=testing").get();

    assertEquals("Should have the correct type", SqlServer.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "somesqlserver.co.uk", result.getHostName());
    assertEquals("Should have the correct instance name", "blah", result.getInstanceName());
    assertEquals("Should have the correct port", 2309, result.getPort());
    assertEquals("Should have the correct database name", "testing", result.getDatabaseName());
  }


  /**
   * Tests SQL Server formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromSQLServerNoPort() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:sqlserver://somesqlserver.co.uk;database=testing").get();

    assertEquals("Should have the correct type", SqlServer.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "somesqlserver.co.uk", result.getHostName());
    assertEquals("Should have the correct database name", "testing", result.getDatabaseName());

    result = databaseType.extractJdbcUrl("jdbc:sqlserver://somesqlserver.co.uk\\blah;database=testing").get();

    assertEquals("Should have the correct type", SqlServer.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "somesqlserver.co.uk", result.getHostName());
    assertEquals("Should have the correct instance name", "blah", result.getInstanceName());
    assertEquals("Should have the correct database name", "testing", result.getDatabaseName());
  }


  /**
   * Checks the bidirectionality of our URL split and combine behaviour.
   */
  public void testUrlRoundTrips() {
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(SqlServer.IDENTIFIER).withHost("hostname").withPort(1234)                                 .withDatabaseName("databaseName").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(SqlServer.IDENTIFIER).withHost("hostname").withPort(1234).withInstanceName("instanceName").withDatabaseName("databaseName").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(SqlServer.IDENTIFIER).withHost("hostname")               .withInstanceName("instanceName").withDatabaseName("databaseName").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(SqlServer.IDENTIFIER).withHost("hostname")                                                .withDatabaseName("databaseName").build());
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