package org.alfasoftware.morf.jdbc.redshift;

import static org.alfasoftware.morf.jdbc.DatabaseTypeIdentifierTestUtils.mockDataSourceFor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.DatabaseTypeIdentifier;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.JdbcUrlElements.Builder;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for DatabaseType.PGSQL
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class TestRedshiftDatabaseType {

  private DatabaseType databaseType;

  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(Redshift.IDENTIFIER);
  }


  /**
   * Test the JDBC URL construction.
   */
  @Test
  public void testFormatJdbcUrl() {
    assertEquals("jdbc:redshift://foo.com:123/alfa", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withPort(123).withDatabaseName("alfa").build()));
    assertEquals("jdbc:redshift://foo.com/alfa", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withDatabaseName("alfa").build()));
    assertEquals("jdbc:redshift://localhost/data", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("localhost").withDatabaseName("data").build()));
  }


  /**
   * Tests Redshift formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromMySQL() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:redshift://localhost:3306/alfa").get();

    assertEquals("Should have the correct type", Redshift.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "localhost", result.getHostName());
    assertEquals("Should have the correct port", 3306, result.getPort());
    assertEquals("Should have the correct database name", "alfa", result.getDatabaseName());
  }


  /**
   * Tests Redshift formatted JDBC URLs.
   */
  @Test
  public void testBuildConnectionDetailsFromMySQLNoPort() {
    JdbcUrlElements result = databaseType.extractJdbcUrl("jdbc:redshift://localhost/alfa").get();

    assertEquals("Should have the correct type", Redshift.IDENTIFIER, result.getDatabaseType());
    assertEquals("Should have the correct host", "localhost", result.getHostName());
    assertEquals("Should have the correct database name", "alfa", result.getDatabaseName());
  }


  /**
   * Checks the bidirectionality of our URL split and combine behaviour.
   */
  @Test
  public void testUrlRoundTrips() {
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(Redshift.IDENTIFIER).withHost("hostname").withPort(3306).withDatabaseName("databasename").build());
    comparerUrlRoundtrips(JdbcUrlElements.forDatabaseType(Redshift.IDENTIFIER).withHost("hostname").withDatabaseName("databasename").build());
  }


  /**
   * Tests a URL <- split -> URL round-trip.
   */
  private void comparerUrlRoundtrips(JdbcUrlElements jdbcUrlElements) {
    String jdbcURL = databaseType.formatJdbcUrl(jdbcUrlElements);
    JdbcUrlElements cd = databaseType.extractJdbcUrl(jdbcURL).get();
    assertEquals(jdbcUrlElements, cd);
  }


  /**
   * Test identification of a database platform.
   */
  @Test
  public void testIdentifyFromMetaData() throws SQLException {
    // -- Unknown and resource management...
    //
    DataSource dataSource = mockDataSourceFor("FictiousDB", "9.9.9", 9, 9);
    assertEquals(Optional.empty(), new DatabaseTypeIdentifier(dataSource).identifyFromMetaData());
    verify(dataSource.getConnection()).close();

    // -- Support platforms...
    //
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("Redshift", "1.3.167", 1, 3)).identifyFromMetaData().get());
  }


  private Builder jdbcUrlElementBuilder() {
    return JdbcUrlElements.forDatabaseType(Redshift.IDENTIFIER);
  }
}
