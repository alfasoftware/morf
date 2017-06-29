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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

import org.alfasoftware.morf.jdbc.NamedParameterPreparedStatement.ParseResult;

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
  public void testParse() {
    ParseResult parseResult = NamedParameterPreparedStatement.parse(
      "SELECT :fee,:fi, :fo(:fum), ':eek' FROM :fum WHERE :fum AND :fo"
    );

    assertThat("Parsed SQL", parseResult.getParsedSql(),                equalTo("SELECT ?,?, ?(?), ':eek' FROM ? WHERE ? AND ?"));
    assertThat("fee",        parseResult.getIndexesForParameter("fee"), contains(1));
    assertThat("fi",         parseResult.getIndexesForParameter("fi"),  contains(2));
    assertThat("fo",         parseResult.getIndexesForParameter("fo"),  contains(3, 7));
    assertThat("fum",        parseResult.getIndexesForParameter("fum"), contains(4, 5, 6));
    assertThat("eek",        parseResult.getIndexesForParameter("eek"), hasSize(0));
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
    NamedParameterPreparedStatement.parse("SELECT * FROM somewhere").createFor(connection).setMaxRows(123);
    verify(underlying).setMaxRows(123);
  }
}
