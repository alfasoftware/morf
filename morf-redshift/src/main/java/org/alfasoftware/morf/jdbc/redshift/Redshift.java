package org.alfasoftware.morf.jdbc.redshift;

import java.sql.Connection;
import java.util.Optional;
import java.util.Stack;

import javax.sql.XADataSource;

import org.alfasoftware.morf.jdbc.AbstractDatabaseType;
import org.alfasoftware.morf.jdbc.JdbcUrlElements;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Support for Redshift database hosts.
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public final class Redshift extends AbstractDatabaseType {

  private static final Log log = LogFactory.getLog(Redshift.class);

  public static final String IDENTIFIER = "REDSHIFT";

  public Redshift() {
    super("com.amazon.redshift.Driver", IDENTIFIER);
  }


  @Override
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements) {
    return "jdbc:redshift://"
        + jdbcUrlElements.getHostName()
        + (jdbcUrlElements.getPort() == 0 ? "" : ":" + jdbcUrlElements.getPort())
        + "/" + jdbcUrlElements.getDatabaseName()
        ;
  }


  @Override
  public Schema openSchema(Connection connection, String databaseName, String schemaName) {
    return new RedshiftMetaDataProvider(connection, schemaName);
  }


  /**
   * Returns a Redshift XA data source.
   *
   * @throws IllegalStateException If the data source cannot be created.
   *
   * @see org.alfasoftware.morf.jdbc.DatabaseType#getXADataSource(String, String, String)
   */
  @Override
  public XADataSource getXADataSource(String jdbcUrl, String username, String password) {
    throw new UnsupportedOperationException("Morf-Redshift does not implement XA connections. ");
  }


  @Override
  public SqlDialect sqlDialect(String schemaName) {
    return new RedshiftDialect(schemaName);
  }


  @Override
  public boolean matchesProduct(String product) {
    return product.equalsIgnoreCase("Redshift");
  }


  @Override
  public Optional<JdbcUrlElements> extractJdbcUrl(String url) {
    Stack<String> splitURL = splitJdbcUrl(url);

    String scheme = splitURL.pop();

    if (!scheme.equalsIgnoreCase("redshift")) {
      return Optional.empty();
    }

    if (!splitURL.pop().equals("://")) {
      // If the next characters are not "://" then die
      throw new IllegalArgumentException("Expected '//' to follow the scheme name in [" + url + "]");
    }

    JdbcUrlElements.Builder connectionDetails = extractHostAndPort(splitURL);

    // Now get the path
    String path = extractPath(splitURL);

    connectionDetails.withDatabaseName(path);

    return Optional.of(connectionDetails.build());
  }
}
