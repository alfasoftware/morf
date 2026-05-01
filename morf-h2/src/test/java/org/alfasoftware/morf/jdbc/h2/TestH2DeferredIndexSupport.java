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

package org.alfasoftware.morf.jdbc.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
 * Focused unit tests for H2 (v1)'s deferred-index dialect surface:
 * {@code isIndexValid}. H2 has no in-catalog INVALID state — atomic CREATE
 * — so existence-in-{@code INFORMATION_SCHEMA} is the full domain of the
 * answer.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestH2DeferredIndexSupport {

  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
  private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);

  private final H2Dialect dialect = new H2Dialect("PUBLIC");


  @Before
  public void setUp() throws SQLException {
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
  }


  /** Index present in {@code INFORMATION_SCHEMA.INDEXES} → {@code Optional.of(true)}. */
  @Test
  public void testIsIndexValidWhenIndexPresentReturnsTrue() throws SQLException {
    when(resultSet.next()).thenReturn(true);

    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    assertEquals(Optional.of(Boolean.TRUE), result);
  }


  /** Index absent → {@code Optional.empty()} (H2 has no INVALID state to surface). */
  @Test
  public void testIsIndexValidWhenIndexAbsentReturnsEmpty() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    assertEquals(Optional.empty(), result);
  }


  /**
   * H2 folds unquoted identifiers to upper case in {@code INFORMATION_SCHEMA}.
   * The dialect must upper-case the supplied index name before binding it
   * so the {@code UPPER(INDEX_NAME) = ?} clause matches.
   */
  @Test
  public void testIsIndexValidUppercasesIndexName() throws SQLException {
    when(resultSet.next()).thenReturn(true);

    dialect.isIndexValid(connection, "Product", "MIXEDcase_Idx");

    verify(statement).setString(1, "MIXEDCASE_IDX");
  }


  /** Any {@link SQLException} propagates as {@link RuntimeSqlException} carrying the index name. */
  @Test
  public void testIsIndexValidWrapsSqlExceptionAsRuntimeSqlException() throws SQLException {
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("conn closed"));

    RuntimeSqlException ex = assertThrows(RuntimeSqlException.class,
        () -> dialect.isIndexValid(connection, "Product", "Product_Idx"));
    assertTrue(ex.getMessage().contains("Product_Idx"));
  }


  /** H2 declines to gate lock-timeouts — its 1s default is short enough; atomic CREATE means no contention. */
  @Test
  public void testSetLockTimeoutSqlReturnsEmpty() {
    assertEquals(Optional.empty(), dialect.setLockTimeoutSql(Duration.ofSeconds(10)));
  }


  /** No reset needed when there's no SET. */
  @Test
  public void testResetLockTimeoutSqlReturnsEmpty() {
    assertEquals(Optional.empty(), dialect.resetLockTimeoutSql());
  }


  /** H2 does not require autocommit for the build path -- atomic CREATE, DDL implicitly committed. */
  @Test
  public void testDeferredIndexBuildDoesNotRequireAutoCommit() {
    assertFalse(dialect.deferredIndexBuildRequiresAutoCommit());
  }
}
