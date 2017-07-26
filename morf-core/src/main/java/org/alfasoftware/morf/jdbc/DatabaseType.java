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
import java.util.List;
import java.util.ServiceLoader;

import javax.sql.XADataSource;

import org.alfasoftware.morf.metadata.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Encapsulates a supported database type.
 *
 * <p>Implementations must be registered using {@link ServiceLoader}.</p>
 *
 * @author Copyright (c) Alfa 2017
 */
public interface DatabaseType {

  /**
   * @return the class name of the driver associated with this database type.
   */
  public String driverClassName();


  /**
   * Generates a JDBC url for the database type. Not all of the parameters are
   * necessarily required for all database types. This is the inverse of
   * {@link #extractJdbcUrl(String)}, so:
   *
   * <pre>{@code
   * formatJdbcUrl(extractJdbcUrl(x).get()).equals(x)
   * }</pre>
   *
   * @param jdbcUrlElements The elements of the URL.
   * @return A fully formed JDBC URL for the database connection.
   */
  public String formatJdbcUrl(JdbcUrlElements jdbcUrlElements);


  /**
   * If the JDBC URL matches the database type, extracts the elements of the URL.
   * Otherwise returns absent to indicate that the URL is not understood by this
   * database type.
   *
   * <p>This is the inverse of {@link #formatJdbcUrl(JdbcUrlElements)}, so:</p>
   *
   * <pre>{@code
   * extractJdbcUrl(formatJdbcUrl(x)).get().equals(x)
   * }</pre>
   *
   * @param url The JDBC URL.
   * @return The {@link JdbcUrlElements}, if the URL matches the database type,
   * otherwise absent.
   */
  public Optional<JdbcUrlElements> extractJdbcUrl(String url);


  /**
   * @param connection Connection from which meta data should be provided.
   * @param databaseName For vendors that support multiple named databases.
   * @param schemaName For vendors that support multiple schemas within a database.
   * @return A meta data provider for the given database.
   */
  public Schema openSchema(Connection connection, String databaseName, String schemaName);


  /**
   * @param jdbcUrl The JDBC URL
   * @param  username The user name
   * @param password  The password
   * @return {@link XADataSource} implementation specific for this {@link DatabaseType}
   */
  public XADataSource getXADataSource(String jdbcUrl, String username, String password);


  /**
   * @return true if tracing can be enabled on the database type
   */
  public boolean canTrace();


  /**
   * Determine the {@link SqlDialect} for this database type.
   *
   * @param schemaName Optional schema name, if applicable,.
   * @return The applicable dialect.
   */
  public SqlDialect sqlDialect(String schemaName);


  /**
   * @return A unique string identifier for the database type. Originally, {@link DatabaseType}
   * was an enum.  This maps to the original enum name to enable existing configuration
   * to still work.
   */
  public String identifier();


  /**
   * Returns true if the JDBC product description corresponds to this database type.
   *
   * @param product The JDBC product description
   * @return true if the JDBC product description corresponds to this database type.
   * @see java.sql.DatabaseMetaData#getDatabaseProductName()
   */
  public boolean matchesProduct(String product);


  /**
   * Static registry of all supported database types, derived from {@link ServiceLoader}.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public static final class Registry {

    private static final Log log = LogFactory.getLog(DatabaseType.Registry.class);

    private static final ImmutableMap<String, DatabaseType> registeredTypes;

    /*
     * Inspects the classpath for implementations of {@link DatabaseType} and registers them automatically.
     */
    static {
      log.info("Loading database types...");
      registeredTypes = Maps.uniqueIndex(ServiceLoader.load(DatabaseType.class), new Function<DatabaseType, String>() {
        @Override
        public String apply(DatabaseType input) {
          log.info(" - Registering [" + input.identifier() + "] as [" + input.getClass().getCanonicalName() + "]");
          return input.identifier();
        }
      });
    }


    /**
     * Returns the registered database type by its identifier.
     *
     * <p>It can be assumed that performance of this method will be <code>O(1)</code> so is
     * suitable for repeated calling in performance code.  There should be
     * few reasons for caching the response.</p>
     *
     * @param identifier The database type identifier (see {@link DatabaseType#identifier()}).
     * @return The {@link DatabaseType}.
     * @throws IllegalArgumentException If no such identifier is found.
     */
    public static DatabaseType findByIdentifier(String identifier) {
      DatabaseType result = registeredTypes.get(identifier);
      if (result == null) throw new IllegalArgumentException("Identifier [" + identifier + "] not known");
      return result;
    }


    /**
     * Returns the first available database type matching the JDBC product
     * name.
     *
     * <p>Returns absent if no matching database type is found. If there are
     * multiple matches for the product name, {@link IllegalStateException}
     * will be thrown.</p>
     *
     * <p>No performance guarantees are made, but it will be <em>at best</em>
     * <code>O(n),</code>where <code>n</code> is the number of registered database
     * types.</p>
     *
     * @param product The JDBC database product name.
     * @return The {@link DatabaseType}.
     * @throws IllegalStateException If more than one matching database type is found.
     */
    public static Optional<DatabaseType> findByProductName(final String product) {
      List<DatabaseType> result = FluentIterable.from(registeredTypes.values()).filter(new Predicate<DatabaseType>() {
        @Override
        public boolean apply(DatabaseType input) {
          return input.matchesProduct(product);
        }
      }).toList();
      if (result.isEmpty()) return Optional.absent();
      if (result.size() > 1) throw new IllegalArgumentException("Database product name [" + product + "] matches "
          + "more than one registered database type " + result);
      return Optional.of(result.get(0));
    }


    /**
     * Extracts the database connection details from a JDBC URL.
     *
     * <p>Finds the first available {@link DatabaseType} with a matching protocol,
     * then uses that to parse out the connection details.</p>
     *
     * <p>If there are multiple matches for the protocol, {@link IllegalStateException}
     * will be thrown.</p>
     *
     * <p>No performance guarantees are made, but it will be <em>at best</em>
     * <code>O(n),</code>where <code>n</code> is the number of registered
     * database types.</p>
     *
     * @param url The JDBC URL.
     * @return The connection details.
     * @throws IllegalArgumentException If no database type matching the URL
     *           protocol is found or the matching database type fails to parse
     *           the URL.
     */
    public static JdbcUrlElements parseJdbcUrl(String url) {
      JdbcUrlElements result = null;
      for (DatabaseType databaseType : registeredTypes.values()) {
        Optional<JdbcUrlElements> connectionDetails = databaseType.extractJdbcUrl(url);
        if (connectionDetails.isPresent()) {
          if (result != null) throw new IllegalArgumentException("[" + url + "] matches more than one registered database type");
          result = connectionDetails.get();
        }
      }
      if (result == null) throw new IllegalArgumentException("[" + url + "] is not a valid JDBC URL");
      return result;
    }
  }

  // Deprecated references to specific database types.

  /** @deprecated use AlfaDatabaseType.MY_SQL, and carefully consider why you are detecting a specific database type at all. */
  @Deprecated
  public static final String MY_SQL = "MY_SQL";

  /** @deprecated use AlfaDatabaseType.ORACLE, and carefully consider why you are detecting a specific database type at all. */
  @Deprecated
  public static final String ORACLE = "ORACLE";

  /** @deprecated use AlfaDatabaseType.SQL_SERVER, and carefully consider why you are detecting a specific database type at all. */
  @Deprecated
  public static final String SQL_SERVER = "SQL_SERVER";

  /** @deprecated use AlfaDatabaseType.H2, and carefully consider why you are detecting a specific database type at all. */
  @Deprecated
  public static final String H2 = "H2";

  /** @deprecated use AlfaDatabaseType.NUODB, and carefully consider why you are detecting a specific database type at all. */
  @Deprecated
  public static final String NUODB = "NUODB";
}