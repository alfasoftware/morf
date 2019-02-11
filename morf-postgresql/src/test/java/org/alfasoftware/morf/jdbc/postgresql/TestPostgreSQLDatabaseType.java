package org.alfasoftware.morf.jdbc.postgresql;

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
public class TestPostgreSQLDatabaseType {

  private DatabaseType databaseType;

  @Before
  public void setup() {
    databaseType = DatabaseType.Registry.findByIdentifier(PostgreSQL.IDENTIFIER);
  }


  /**
   * Test the JDBC URL construction.
   */
  @Test
  public void testFormatJdbcUrl() {
    assertEquals("jdbc:postgresql://foo.com:123/alfa", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withPort(123).withDatabaseName("alfa").build()));
    assertEquals("jdbc:postgresql://foo.com/alfa", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("foo.com").withDatabaseName("alfa").build()));
    assertEquals("jdbc:postgresql://localhost/data", databaseType.formatJdbcUrl(jdbcUrlElementBuilder().withHost("localhost").withDatabaseName("data").build()));
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
    assertEquals(databaseType, new DatabaseTypeIdentifier(mockDataSourceFor("PostgreSQL", "1.3.167", 1, 3)).identifyFromMetaData().get());
  }


  private Builder jdbcUrlElementBuilder() {
    return JdbcUrlElements.forDatabaseType(PostgreSQL.IDENTIFIER);
  }
}
