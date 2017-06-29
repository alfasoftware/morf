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

package org.alfasoftware.morf.jdbc.oracle;

import static org.alfasoftware.morf.jdbc.DatabaseTypeIdentifierTestUtils.mockDataSourceFor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.DatabaseTypeIdentifier;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;

public class TestOracleDatabaseType {

  private DatabaseType databaseType;


  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(Oracle.IDENTIFIER);
  }


  @Test(expected = IllegalStateException.class)
  public void openingSchemaOfOracleWithoutSchemaNameShouldThrowISE() {
    databaseType.openSchema(null, null, null);
  }


  @Test(expected = IllegalStateException.class)
  public void openingSchemaOfOracleWithEmptySchemaNameShouldThrowISE() {
    databaseType.openSchema(null, null, "");
  }

  /**
   * Tests Oracle JDBC URL formatting.
   */
  @Test
  public void testOracleUrlFormatting() {
    assertEquals(
      "Oracle database url",
      "jdbc:oracle:thin:@localhost/null",
      databaseType.formatJdbcUrl(
        JdbcUrlElements.forDatabaseType(Oracle.IDENTIFIER)
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
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("Oracle", "Oracle Database 10g Enterprise Edition Release 10.2.0.3.0 - Production\nWith the Partitioning, OLAP and Data Mining options", 10, 2)).identifyFromMetaData().get());
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("Oracle", "Oracle Database 11g Enterprise Edition Release 11.2.0.1.0 - 64bit Production\nWith the Partitioning, OLAP, Data Mining and Real Application Testing options", 11, 2)).identifyFromMetaData().get());
  }


  /**
   * Tests Oracle formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromOracle() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:oracle:thin:@anoraclehost.co.uk:1251/instance").get();

    assertEquals("Should have the correct type", Oracle.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "anoraclehost.co.uk", result.getHostName());
    assertEquals("Should have the correct port", 1251, result.getPort());
    assertEquals("Should have the correct instance name", "instance", result.getInstanceName());
  }


  /**
   * Tests Oracle formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromOracleNoPort() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:oracle:thin:@anoraclehost.co.uk/instance").get();

    assertEquals("Should have the correct type", Oracle.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "anoraclehost.co.uk", result.getHostName());
    assertEquals("Should have the correct instance name", "instance", result.getInstanceName());
  }


  /**
   * Checks the bidirectionality of our URL split and combine behaviour.
   */
  public void testUrlRoundTrips() {
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(Oracle.IDENTIFIER).withHost("hostname").withPort(1521).withInstanceName("instanceName").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(Oracle.IDENTIFIER).withHost("hostname").withInstanceName("instanceName").build());
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