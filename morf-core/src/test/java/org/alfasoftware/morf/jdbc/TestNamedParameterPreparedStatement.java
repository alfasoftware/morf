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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement.ParseResult;
import org.junit.Test;

/**
 * Tests {@link NamedParameterPreparedStatement}.
 *
 * @author Copyright (c) Alfa Financial Software 2014
 */
public class TestNamedParameterPreparedStatement {


  /**
   * Ensures that we can handle multi-use of the same parameter in the same statement,
   * a variety of mixed ordering of these parameters, and we ignore parameters in
   * parentheses.
   */
  @Test
  public void testParseWithParameterspParenthesesAndSingleQuotes() {
    // Given a SQL query with many parameters (one in parentheses and another in single quotes)
    String sql = "SELECT :fee,:fi, :fo(:fum), ':eek' FROM :fum WHERE :fum AND :fo AND 1::TEXT = 1 :: NUMERIC AND : spaced AND :1";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the parameter in the single quotes as is
    // but replace the parameter in the parentheses with a question mark
    assertEquals("Parsed SQL",
        "SELECT ?,?, ?(?), ':eek' FROM ? WHERE ? AND ? AND 1:? = 1 :: NUMERIC AND : spaced AND :1",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    assertTrue(parseResult.getIndexesForParameter("fee").size() == 1);
    assertTrue(parseResult.getIndexesForParameter("fee").contains(1));
    assertTrue(parseResult.getIndexesForParameter("fi").size() == 1);
    assertTrue(parseResult.getIndexesForParameter("fi").contains(2));

    List<Integer> foIndexes = parseResult.getIndexesForParameter("fo");
    assertTrue(foIndexes.size() == 2);
    assertTrue(foIndexes.contains(3));
    assertTrue(foIndexes.contains(7));

    List<Integer> fumIndexes = parseResult.getIndexesForParameter("fum");
    assertTrue(fumIndexes.size() == 3);
    assertTrue(fumIndexes.contains(4));
    assertTrue(fumIndexes.contains(5));
    assertTrue(fumIndexes.contains(6));

    List<Integer> textIndexes = parseResult.getIndexesForParameter("TEXT");
    assertTrue(textIndexes.size() == 1);
    assertTrue(textIndexes.contains(8));

    // And it should correctly identify that the 'eek' or NUMERIC or spaced is not treated as parameters
    assertTrue(parseResult.getIndexesForParameter("eek").isEmpty());
    assertTrue(parseResult.getIndexesForParameter("NUMERIC").isEmpty());
    assertTrue(parseResult.getIndexesForParameter("spaced").isEmpty());
    assertTrue(parseResult.getIndexesForParameter("1").isEmpty());
  }


  /**
   * Ensures that the `indexMap` is properly populated with parameter information when the SQL query contains comments.
   * The SQL query provided contains parameters, a comment and some text.
   * The parsed SQL should have '?' characters in place of parameters and should ignore comments.
   */
  @Test
  public void testParseWithParameterInsideComment() {
    // Given a SQL query with a comment with a parameter inside
    String sql = "SELECT :fee,:fi, :fo(:fum), ':eek' FROM :fum\n-- Comment :bang\nWHERE :fum AND :fo";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the comment as is
    assertEquals("Parsed SQL",
        "SELECT ?,?, ?(?), ':eek' FROM ?\n-- Comment :bang\nWHERE ? AND ?",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    assertTrue(parseResult.getIndexesForParameter("fee").contains(1));
    assertTrue(parseResult.getIndexesForParameter("fi").contains(2));

    List<Integer> foIndexes = parseResult.getIndexesForParameter("fo");
    assertTrue(foIndexes.contains(3));
    assertTrue(foIndexes.contains(7));

    List<Integer> fumIndexes = parseResult.getIndexesForParameter("fum");
    assertTrue(fumIndexes.contains(4));
    assertTrue(fumIndexes.contains(5));
    assertTrue(fumIndexes.contains(6));

    // And it should correctly identify that 'bang' and 'eek' parameters are not found
    assertTrue(parseResult.getIndexesForParameter("bang").isEmpty());
    assertTrue(parseResult.getIndexesForParameter("eek").isEmpty());
  }


  /**
   * Test parsing a SQL query with a comment inside single quotes.
   * This test verifies that the SQL parser correctly handles a comment that is
   * inside single quotes, ensuring that the comment is not treated as part of the
   * SQL parameters.
   */
  @Test
  public void testParseWithCommentInsideSingleQuotes() {
    // Given a SQL query with a comment inside single quotes
    String sql = "SELECT id, name, '-- not really a comment', value FROM products WHERE name = :name AND value > :minValue";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the comment inside single quotes as is
    assertEquals("Parsed SQL",
        "SELECT id, name, '-- not really a comment', value FROM products WHERE name = ? AND value > ?",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    List<Integer> nameIndexes = parseResult.getIndexesForParameter("name");
    assertEquals("Parameter count", 1, nameIndexes.size());
    assertEquals("Parameter index", 1, nameIndexes.get(0).intValue());

    List<Integer> minValueIndexes = parseResult.getIndexesForParameter("minValue");
    assertEquals("Parameter count", 1, minValueIndexes.size());
    assertEquals("Parameter index", 2, minValueIndexes.get(0).intValue());
  }


  /**
   * Test parsing a SQL query with a comment inside double quotes.
   * This test verifies that the SQL parser correctly handles a comment that is
   * inside double quotes, ensuring that the comment is not treated as part of the
   * SQL parameters.
   */
  @Test
  public void testParseWithCommentInsideDoubleQuotes() {
    // Given a SQL query with a comment inside double quotes
    String sql = "SELECT id, name, \"-- not really a comment\", value FROM products WHERE name = :name AND value > :minValue";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the comment inside double quotes as is
    assertEquals("Parsed SQL",
        "SELECT id, name, \"-- not really a comment\", value FROM products WHERE name = ? AND value > ?",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    List<Integer> nameIndexes = parseResult.getIndexesForParameter("name");
    assertEquals("Parameter count", 1, nameIndexes.size());
    assertEquals("Parameter index", 1, nameIndexes.get(0).intValue());

    List<Integer> minValueIndexes = parseResult.getIndexesForParameter("minValue");
    assertEquals("Parameter count", 1, minValueIndexes.size());
    assertEquals("Parameter index", 2, minValueIndexes.get(0).intValue());
  }


  /**
   * Test parsing a SQL query with quote characters inside a comment.
   * This test checks that the SQL parser correctly handles single and double
   * quote characters inside a comment and does not interpret them as the start
   * of string literals.
   */
  @Test
  public void testParseWithQuoteCharactersInsideComment() {
    // Given a SQL query with a comment containing single and double quote characters
    String sql = "SELECT * FROM products\n-- comment with double quote: \" and single quote: '\nWHERE = :parameter";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the comment as is
    assertEquals("Parsed SQL",
        "SELECT * FROM products\n-- comment with double quote: \" and single quote: '\nWHERE = ?",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameter
    List<Integer> parameterIndexes = parseResult.getIndexesForParameter("parameter");
    assertEquals("Parameter count", 1, parameterIndexes.size());
    assertEquals("Parameter index", 1, parameterIndexes.get(0).intValue());
  }


  /**
   * Test parsing a SQL query with a comment at the end of a line.
   * This test ensures that a comment at the end of a line is correctly preserved
   * in the parsed SQL and is not treated as part of the SQL parameters.
   */
  @Test
  public void testParseWithCommentAtEndOfLine() {
    // Given a SQL query with a comment at the end of a line
    String sql = "SELECT id, name, value FROM products WHERE name = :name AND value > :minValue -- comment at the end of the line";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the comment at the end of the line as is
    assertEquals("Parsed SQL",
        "SELECT id, name, value FROM products WHERE name = ? AND value > ? -- comment at the end of the line",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    List<Integer> nameIndexes = parseResult.getIndexesForParameter("name");
    assertEquals("Parameter count", 1, nameIndexes.size());
    assertEquals("Parameter index", 1, nameIndexes.get(0).intValue());

    List<Integer> minValueIndexes = parseResult.getIndexesForParameter("minValue");
    assertEquals("Parameter count", 1, minValueIndexes.size());
    assertEquals("Parameter index", 2, minValueIndexes.get(0).intValue());
  }


  /**
   * Test parsing a SQL query with a single hyphen at the end of a line/query.
   * This test verifies that a single hyphen at the end of a line or query does
   * not cause any issues in the parsed SQL and is correctly preserved.
   */
  @Test
  public void testParseWithSingleHyphenAtEndOfQuery() {
    // Given a SQL query with a single hyphen at the end
    String sql = "SELECT id, name, value FROM products WHERE name = :name AND value > :minValue -";

    // When we parse the SQL query
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(sql, mock(SqlDialect.class));

    // Then the parsed SQL should retain the single hyphen at the end as is
    assertEquals("Parsed SQL",
        "SELECT id, name, value FROM products WHERE name = ? AND value > ? -",
        parseResult.getParsedSql()
    );

    // And it should correctly identify the parameters
    List<Integer> nameIndexes = parseResult.getIndexesForParameter("name");
    assertEquals("Parameter count", 1, nameIndexes.size());
    assertEquals("Parameter index", 1, nameIndexes.get(0).intValue());

    List<Integer> minValueIndexes = parseResult.getIndexesForParameter("minValue");
    assertEquals("Parameter count", 1, minValueIndexes.size());
    assertEquals("Parameter index", 2, minValueIndexes.get(0).intValue());
  }


  /**
   * Tests that maximum rows are passed through.
   *
   * @throws SQLException
   */
  public void testMaxRows() throws SQLException {
    Connection connection = mock(Connection.class);
    PreparedStatement underlying = mock(PreparedStatement.class);
    when(connection.prepareStatement("SELECT * FROM somewhere")).thenReturn(underlying);
    NamedParameterPreparedStatement.parseSql("SELECT * FROM somewhere", mock(SqlDialect.class)).createFor(connection).setMaxRows(123);
    verify(underlying).setMaxRows(123);
  }
}
