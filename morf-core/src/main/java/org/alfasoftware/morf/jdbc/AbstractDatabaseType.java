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

import java.util.Collections;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convenience base class for implementations of {@link DatabaseType}.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public abstract class AbstractDatabaseType implements DatabaseType {

  /**
   * The class name of the driver associated with this type.
   */
  private final String driverClassName;

  /**
   * The unique string identifier for the database type.
   */
  private final String identifier;


  /**
   * Protected constructor.
   *
   * @param driverClassName The driver class that must be used to connect to this database type.
   * @param identifier The unique string identifier for the database type.
   */
  protected AbstractDatabaseType(String driverClassName, String identifier) {
    this.driverClassName = driverClassName;
    this.identifier = identifier;
  }


  /**
   * @return the class name of the driver associated with this database type.
   */
  @Override
  public final String driverClassName() {
    return driverClassName;
  }


  /**
   * @return true if tracing can be enabled on the database type
   */
  @Override
  public boolean canTrace() {
    return false;
  }


  /**
   * @see org.alfasoftware.morf.jdbc.DatabaseType#identifier()
   */
  @Override
  public final String identifier() {
    return identifier;
  }


  /**
   * Splits up a JDBC URL into its components and returns the result
   * as a stack, allowing it to be parsed from left to right.
   *
   * @param jdbcUrl The JDBC URL.
   * @return The split result as a stack.
   */
  protected final Stack<String> splitJdbcUrl(String jdbcUrl) {
    Stack<String> splitURL = split(jdbcUrl.trim(), "[/;:@]+");

    if (!splitURL.pop().equalsIgnoreCase("jdbc") || !splitURL.pop().equals(":")) {
      throw new IllegalArgumentException("[" + jdbcUrl + "] is not a valid JDBC URL");
    }
    return splitURL;
  }


  /**
   * Creates a new {@link JdbcUrlElements.Builder}, prepopulating it with
   * the host and port from a stack returned by {@link #splitJdbcUrl(String)}.
   *
   * @param splitURL Stack containing the host name and optionally a colon and the port number
   * @return Builder for chaining.
   */
  protected final JdbcUrlElements.Builder extractHostAndPort(Stack<String> splitURL) {
    JdbcUrlElements.Builder builder = JdbcUrlElements.forDatabaseType(identifier());

    // The next bit is the host name
    builder.withHost(splitURL.pop());

    // If there's a ":" then this will be the port
    if (splitURL.peek().equals(":")) {
      splitURL.pop(); // Remove the delimiter
      builder.withPort(Integer.parseInt(splitURL.pop()));
    }

    return builder;
  }


  /**
   * Extracts the path part of the URL from the stack returned by
   * {@link #splitJdbcUrl(String)}.  Assumes that the stack has already
   * had the preceding components (e.g. protocol, host and port) popped.
   *
   * @param splitURL A stack containing the remaining portions of a JDBC URL.
   * @return The path.
   */
  protected final String extractPath(Stack<String> splitURL) {
    StringBuilder path = new StringBuilder();

    if (!splitURL.isEmpty()) {
      splitURL.pop();  // Remove the delimiter
      while (!splitURL.isEmpty()) {
        path = path.append(splitURL.pop());
      }
    }
    return path.toString();
  }


  /**
   * Splits a string using the pattern specified, keeping the delimiters in between.
   *
   * @param text the text to split.
   * @param delimiterPattern the regex pattern to use as the delimiter.
   * @return a list of strings made up of the parts and their delimiters.
   */
  private Stack<String> split(String text, String delimiterPattern) {

    if (text == null) {
      throw new IllegalArgumentException("You must supply some text to split");
    }

    if (delimiterPattern == null) {
      throw new IllegalArgumentException("You must supply a pattern to match on");
    }

    Pattern pattern = Pattern.compile(delimiterPattern);

    int lastMatch = 0;
    Stack<String> splitted = new Stack<>();

    Matcher m = pattern.matcher(text);

    // Iterate trough each match
    while (m.find()) {
      // Text since last match
      splitted.add(text.substring(lastMatch, m.start()));

      // The delimiter itself
      splitted.add(m.group());

      lastMatch = m.end();
    }

    // Trailing text
    splitted.add(text.substring(lastMatch));

    Collections.reverse(splitted);

    return splitted;
  }
}