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
 * @author Copyright (c) Alfa Financial Software 2023
 */
public class TestNamedParameterPreparedStatement {

  /**
   * Ensures that we can handle multi-use of the same parameter in the same statement,
   * a variety of mixed ordering of these parameters, and we ignore parameters in
   * parentheses.
   */
  @Test
  public void testParse() {
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(
        "SELECT :fee,:fi, :fo(:fum), ':eek' FROM :fum WHERE :fum AND :fo",
        mock(SqlDialect.class)
    );

    assertEquals("Parsed SQL",
        "SELECT ?,?, ?(?), ':eek' FROM ? WHERE ? AND ?",
        parseResult.getParsedSql()
    );
    assertTrue(parseResult.getIndexesForParameter("fee").contains(1));
    assertTrue(parseResult.getIndexesForParameter("fi").contains(2));

    List<Integer> foIndexes = parseResult.getIndexesForParameter("fo");
    assertTrue(foIndexes.contains(3));
    assertTrue(foIndexes.contains(7));

    List<Integer> fumIndexes = parseResult.getIndexesForParameter("fum");
    assertTrue(fumIndexes.contains(4));
    assertTrue(fumIndexes.contains(5));
    assertTrue(fumIndexes.contains(6));

    assertTrue(parseResult.getIndexesForParameter("eek").isEmpty());
  }


  /**
   * Ensures that the `indexMap` is properly populated with parameter information when the SQL query contains comments.
   * The SQL query provided contains parameters, a comment and some text.
   * The parsed SQL should have '?' characters in place of parameters and should ignore comments.
   */
  @Test
  public void testParseWithComments() {
    ParseResult parseResult = NamedParameterPreparedStatement.parseSql(
        "SELECT :fee,:fi, :fo(:fum), ':eek' FROM :fum\n-- Comment :bang\nWHERE :fum AND :fo",
        mock(SqlDialect.class)
    );

    assertEquals("Parsed SQL",
        "SELECT ?,?, ?(?), ':eek' FROM ?\n-- Comment :bang\nWHERE ? AND ?",
        parseResult.getParsedSql()
    );
    assertTrue(parseResult.getIndexesForParameter("fee").contains(1));
    assertTrue(parseResult.getIndexesForParameter("fi").contains(2));

    List<Integer> foIndexes = parseResult.getIndexesForParameter("fo");
    assertTrue(foIndexes.contains(3));
    assertTrue(foIndexes.contains(7));

    List<Integer> fumIndexes = parseResult.getIndexesForParameter("fum");
    assertTrue(fumIndexes.contains(4));
    assertTrue(fumIndexes.contains(5));
    assertTrue(fumIndexes.contains(6));

    assertTrue(parseResult.getIndexesForParameter("bang").isEmpty());
    assertTrue(parseResult.getIndexesForParameter("eek").isEmpty());
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
