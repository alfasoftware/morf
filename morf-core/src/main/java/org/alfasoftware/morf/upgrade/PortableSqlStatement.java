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

package org.alfasoftware.morf.upgrade;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.sql.Statement;
import org.omg.CORBA.portable.Streamable;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

/**
 * A {@link Statement} that allows free format SQL to be run over a database.
 *
 * <p>This should be considered a <strong>last resort</strong> in the rare cases
 * where the SQL required is not supported by the general purpose SQL DSL.  It forces
 * the author to write as many different statements as there are supported platforms.</p>
 *
 * <p>One more common use case is where there are statements that must only be run
 * on one or more specific platforms.  For instance, some complex data migrations often
 * benefit from gathering statistics mid-way on platforms which do not automatically
 * gather statistics, such as Oracle.  Using {@link PortableSqlStatement}, the author
 * may just write the Oracle statement and leave the versions for other platforms
 * blank.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class PortableSqlStatement implements Statement {

  /**
   * Map containing database specific SQL statements keyed by {@link DatabaseType#identifier()}.
   */
  private final Map<String, String> statements = new HashMap<>();

  /**
   * The {@link TableNameResolver}
   */
  private TableNameResolver nameResolver;


  /**
   * Adds a database specific SQL statement.
   *
   * @param databaseTypeIdentifier The database type identifier ({@link DatabaseType#identifier()}).
   * @param sql The SQL statement.
   * @return this {@link PortableSqlStatement}.
   */
  public PortableSqlStatement add(String databaseTypeIdentifier, String sql) {
    statements.put(databaseTypeIdentifier, sql);
    return this;
  }

  /**
   * Adds an SQL statement to be run for a given db.
   *
   * @param databaseTypeIdentifier The database type identifier ({@link DatabaseType#identifier()}).
   * @param stream The {@link Streamable} containing the SQL.
   * @return This {@link PortableSqlStatement}.
   */
  public PortableSqlStatement add(String databaseTypeIdentifier, InputStream stream) {
    InputStreamReader streamReader = null;
    try {
      streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
      statements.put(databaseTypeIdentifier, CharStreams.toString(streamReader));
    } catch (IOException e) {
      throw new RuntimeException(
        "Error loading SQL upgrade script from server.", e);
    } finally {
      Closeables.closeQuietly(stream);
      Closeables.closeQuietly(streamReader);
    }
    return this;
  }

  /**
   * Adds an unsupported marker for a given database.
   *
   * @param databaseTypeIdentifier The db type identifier ({@link DatabaseType#identifier()}).
   * @param supported The {@link DataUpgradeSupported} value.
   * @return This {@link PortableSqlStatement}.
   */
  public PortableSqlStatement add(String databaseTypeIdentifier, DataUpgradeSupported supported) {
    statements.put(databaseTypeIdentifier, supported.toString());
    return this;
  }


  /**
   * @return the statements
   */
  public Map<String, String> getStatements() {
    return statements;
  }


  /**
   * Deep copy.
   *
   * @return null.
   */
  @Override
  public Statement deepCopy() {
    return null;
  }


  /**
   * Sets the {@link TableNameResolver}.
   *
   * @param nameResolver The {@link TableNameResolver}.
   */
  public void inplaceUpdateTransitionalTableNames(TableNameResolver nameResolver) {
    this.nameResolver = nameResolver;
  }


  /**
   * Generates the statement.
   *
   * @param databaseTypeIdentifier The database type identifier ({@link DatabaseType#identifier()}).
   * @param tablePrefix The prefix for table names (e.g. the database schema for Oracle) - Not required
   *                    for database types which default an appropriate value from the connection details (e.g. MySql).
   * @return String The SQL statement.
   */
  public String getStatement(String databaseTypeIdentifier, String tablePrefix) {
    String statement = generateStatement(statements.get(databaseTypeIdentifier), tablePrefix);
    if (statement.equals("")) {
      throw new RuntimeException("Database type " + databaseTypeIdentifier
        + " has no configuration settings for this data upgrade.");
    }
    if (statement.equals(DataUpgradeSupported.UNSUPPORTED.toString())) {
      throw new RuntimeException("Database type " + databaseTypeIdentifier
        + " unsupported for this data upgrade.");
    }
    return statement;
  }


  /**
   * Generates the SQL statement.
   *
   * @param sourceSQL The source SQL.
   * @param tablePrefix The prefix for table names (e.g. the database schema for Oracle) - Not required
   *                    for database types which default an appropriate value from the connection details (e.g. MySql).
   * @return The SQL statement with table names substituted.
   */
  private String generateStatement(String sourceSQL, String tablePrefix) {
    if (sourceSQL == null || sourceSQL.equals("")) {
      return "";
    }

    // Add the correct transitional table names
    StringBuffer buffer = new StringBuffer(sourceSQL);
    int currentIndex = 0;
    while (currentIndex < buffer.length()) {
      int startPosition = 0;
      int endPosition = 0;
      startPosition = buffer.indexOf("{", currentIndex);
      endPosition = buffer.indexOf("}", startPosition);
      if (startPosition == -1) {
        break;
      }
      String tableName = buffer.substring(startPosition + 1, endPosition);
      String replacementTableName = tablePrefix + nameResolver.activeNameFor(tableName);
      buffer.replace(startPosition, endPosition + 1, replacementTableName);
      currentIndex = endPosition;
    }
    return buffer.toString();
  }


  /**
   * Enum defining whether a database type is supported by this SQL statement.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  public enum DataUpgradeSupported {

    /**
     * The SQL script is unsupported for the specified database type.
     */
    UNSUPPORTED("unsupported");

    /**
     * The code.
     */
    private final String code;

    /**
     * Sets the code.
     *
     * @param code The code
     */
    private DataUpgradeSupported(String code) {
      this.code = code;
    }

    /**
     * Returns value of enum.
     *
     * @param s The code
     * @return The DatabaseUpradeSupported
     */
    public static DataUpgradeSupported getValueOf(String s) {
      for (DataUpgradeSupported type : DataUpgradeSupported.values()) {
        if (type.code.equals(s)) {
          return type;
        }
      }
      throw new IllegalArgumentException(String.format(
        "Unknown DataUpgradeSupported type [%s]", s));
    }
  }
}
