package org.alfasoftware.morf.jdbc.postgresql;

import java.sql.Connection;
import java.util.Optional;

import javax.sql.XADataSource;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Support for PostgreSQL database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public final class PostgreSQL extends AbstractDatabaseType {

  private static final Log log = LogFactory.getLog(PostgreSQL.class);

  public static final String IDENTIFIER = "PGSQL";

  public PostgreSQL() {
    super("org.postgresql.Driver", IDENTIFIER);
  }


  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    return "jdbc:postgresql://"
        + jdbcUrlElements.getHostName()
        + (jdbcUrlElements.getPort() == 0 ? "" : ":" + jdbcUrlElements.getPort())
        + "/" + jdbcUrlElements.getDatabaseName()
        ;
  }


  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new PostgreSQLMetaDataProvider(connection, schemaName);
  }


  /**
   * Returns a PostgreSQL XA data source.
   *
   * @throws IllegalStateException If the data source cannot be created.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(String, String, String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    try {
      log.info("Initialising PostgreSQL XA data source...");
      XADataSource dataSource = (XADataSource) Class.forName("org.postgresql.xa.PGXADataSource").newInstance();
      dataSource.getClass().getMethod("setURL", String.class).invoke(dataSource, jdbcUrl);
      dataSource.getClass().getMethod("setUser", String.class).invoke(dataSource, username);
      dataSource.getClass().getMethod("setPassword", String.class).invoke(dataSource, password);
      return dataSource;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create PostgreSQL XA data source", e);
    }
  }


  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new PostgreSQLDialect(schemaName);
  }


  @Override
  public boolean matchesProduct(String product) {
    return product.equalsIgnoreCase("PostgreSQL");
  }


  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String url) {
    throw new UnsupportedOperationException("Not yet...");
  }
}
