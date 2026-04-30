/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.jdbc.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.junit.Before;
import org.junit.Test;

/**
 * Focused unit tests for the dialect methods used by the deferred-index
 * background-build flow on Oracle: {@code isIndexValid}. Oracle does not
 * override {@code setLockTimeoutSql} / {@code resetLockTimeoutSql} — its
 * NOWAIT-equivalent default already fail-fasts.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestOracleDeferredIndexSupport {

  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
  private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

  private final OracleDialect dialect = new OracleDialect("APPSCHEMA");


  @Before
  public void setUp() throws SQLException {
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
  }


  /** {@code USER_INDEXES.STATUS = 'VALID'} → {@code Optional.of(true)}. */
  @Test
  public void testIsIndexValidWhenStatusValidReturnsTrue() throws SQLException {
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn("VALID");

    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    assertEquals(Optional.of(Boolean.TRUE), result);
  }


  /** {@code USER_INDEXES.STATUS = 'UNUSABLE'} → {@code Optional.of(false)}. */
  @Test
  public void testIsIndexValidWhenStatusUnusableReturnsFalse() throws SQLException {
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn("UNUSABLE");

    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    assertEquals(Optional.of(Boolean.FALSE), result);
  }


  /** No matching row in {@code USER_INDEXES} → {@code Optional.empty()}. */
  @Test
  public void testIsIndexValidWhenNoRowReturnsEmpty() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    assertEquals(Optional.empty(), result);
  }


  /**
   * Oracle stores unquoted identifiers folded to upper case. The dialect
   * must upper-case the supplied index name before binding it as a parameter
   * so the {@code WHERE INDEX_NAME = ?} clause matches.
   */
  @Test
  public void testIsIndexValidUppercasesIndexName() throws SQLException {
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn("VALID");

    dialect.isIndexValid(connection, "Product", "MIXEDcase_Idx");

    verify(statement).setString(1, "MIXEDCASE_IDX");
  }


  /** Any {@link SQLException} propagates as {@link RuntimeSqlException} carrying the index name. */
  @Test
  public void testIsIndexValidWrapsSqlExceptionAsRuntimeSqlException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("ORA-12541"));

    RuntimeSqlException ex = assertThrows(RuntimeSqlException.class,
        () -> dialect.isIndexValid(connection, "Product", "Product_Idx"));
    assertTrue(ex.getMessage().contains("Product_Idx"));
  }


  /** Oracle declines to gate lock-timeouts — its default behaviour already fail-fasts. */
  @Test
  public void testSetLockTimeoutSqlReturnsEmpty() {
    assertEquals(Optional.empty(), dialect.setLockTimeoutSql(Duration.ofSeconds(10)));
  }


  /** No reset needed when there's no SET. */
  @Test
  public void testResetLockTimeoutSqlReturnsEmpty() {
    assertEquals(Optional.empty(), dialect.resetLockTimeoutSql());
  }
}
