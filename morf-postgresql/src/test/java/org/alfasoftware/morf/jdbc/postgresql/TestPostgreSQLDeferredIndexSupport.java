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

package org.alfasoftware.morf.jdbc.postgresql;

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
import org.mockito.ArgumentCaptor;

/**
 * Focused unit tests for the dialect methods used by the deferred-index
 * background-build flow on PostgreSQL: {@code isIndexValid},
 * {@code setLockTimeoutSql}, and {@code resetLockTimeoutSql}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestPostgreSQLDeferredIndexSupport {

  private final PostgreSQLDialect dialect = new PostgreSQLDialect("schemaA");
  private final Connection connection = mock(Connection.class, RETURNS_SMART_NULLS);
  private final PreparedStatement statement = mock(PreparedStatement.class, RETURNS_SMART_NULLS);
  private final ResultSet resultSet = mock(ResultSet.class, RETURNS_SMART_NULLS);


  @Before
  public void setUp() throws SQLException {
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
  }


  // ---- isIndexValid -------------------------------------------------------

  /** A row with {@code indisvalid=true} surfaces as {@code Optional.of(true)}. */
  @Test
  public void testIsIndexValidWhenIndisvalidTrueReturnsTrue() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);

    // when
    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    // then
    assertEquals(Optional.of(Boolean.TRUE), result);
  }


  /** A row with {@code indisvalid=false} surfaces as {@code Optional.of(false)}. */
  @Test
  public void testIsIndexValidWhenIndisvalidFalseReturnsFalse() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(false);

    // when
    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    // then
    assertEquals(Optional.of(Boolean.FALSE), result);
  }


  /** No matching row in {@code pg_index} → {@code Optional.empty()}. */
  @Test
  public void testIsIndexValidWhenNoRowReturnsEmpty() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(false);

    // when
    Optional<Boolean> result = dialect.isIndexValid(connection, "Product", "Product_Idx");

    // then
    assertEquals(Optional.empty(), result);
  }


  /**
   * When a schema name is configured, the query joins {@code pg_namespace}
   * and binds the schema name as the first parameter — protects against a
   * same-named index in another schema being matched by accident.
   */
  @Test
  public void testIsIndexValidQueryFiltersOnSchemaWhenConfigured() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);

    // when
    new PostgreSQLDialect("MySchema").isIndexValid(connection, "Product", "Product_Idx");

    // then
    verify(connection).prepareStatement(sql.capture());
    assertTrue("Query should include pg_namespace join when schema is configured: " + sql.getValue(),
        sql.getValue().contains("pg_namespace"));
    assertTrue("Query should bind nspname: " + sql.getValue(),
        sql.getValue().contains("n.nspname"));
    verify(statement).setString(1, "MySchema");
    verify(statement).setString(2, "Product_Idx");
  }


  /**
   * When no schema name is configured, the {@code pg_namespace} join is
   * omitted — preserves backward compatibility with deployments that don't
   * set a schema explicitly.
   */
  @Test
  public void testIsIndexValidQueryOmitsSchemaWhenNotConfigured() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);

    // when
    new PostgreSQLDialect(null).isIndexValid(connection, "Product", "Product_Idx");

    // then
    verify(connection).prepareStatement(sql.capture());
    assertFalse("Query should not include pg_namespace when schema is unconfigured: " + sql.getValue(),
        sql.getValue().contains("pg_namespace"));
    verify(statement).setString(1, "Product_Idx");
  }


  /** A blank schema name behaves the same as null — no namespace join. */
  @Test
  public void testIsIndexValidQueryOmitsSchemaWhenBlank() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);

    // when
    new PostgreSQLDialect("").isIndexValid(connection, "Product", "Product_Idx");

    // then
    verify(connection).prepareStatement(sql.capture());
    assertFalse(sql.getValue().contains("pg_namespace"));
  }


  /**
   * A whitespace-only schema name fails {@code SchemaValidatorUtil.validateSchemaName}
   * at construction (the validator rejects anything outside {@code [A-Za-z0-9_]*}),
   * so the {@code StringUtils.isNotBlank} check inside {@code isIndexValid} can never
   * see a whitespace-only string. Documents the upstream guard rather than the dead
   * defensive branch.
   */
  @Test
  public void testWhitespaceSchemaNameRejectedAtConstruction() {
    // when / then
    assertThrows(IllegalArgumentException.class, () -> new PostgreSQLDialect("   "));
  }


  /** Index name lookup is case-insensitive (PG can fold quoted vs unquoted). */
  @Test
  public void testIsIndexValidUsesCaseInsensitiveCompare() throws SQLException {
    // given
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);

    // when
    dialect.isIndexValid(connection, "Product", "MIXEDcase_Idx");

    // then
    verify(connection).prepareStatement(sql.capture());
    assertTrue("Query should lowercase both sides for case-insensitive compare: " + sql.getValue(),
        sql.getValue().contains("lower(c.relname) = lower(?)"));
  }


  /** Any {@link SQLException} propagates as {@link RuntimeSqlException} carrying the index name. */
  @Test
  public void testIsIndexValidWrapsSqlExceptionAsRuntimeSqlException() throws SQLException {
    // given
    when(connection.prepareStatement(anyString())).thenThrow(new SQLException("conn closed"));

    // when / then
    RuntimeSqlException ex = assertThrows(RuntimeSqlException.class,
        () -> dialect.isIndexValid(connection, "Product", "Product_Idx"));
    assertTrue(ex.getMessage().contains("Product_Idx"));
  }


  // ---- setLockTimeoutSql / resetLockTimeoutSql ----------------------------

  /** {@code SET lock_timeout} uses the supplied duration in milliseconds. */
  @Test
  public void testSetLockTimeoutSqlReturnsExpectedFormat() {
    // when
    Optional<String> sql = dialect.setLockTimeoutSql(Duration.ofSeconds(10));

    // then
    assertEquals(Optional.of("SET lock_timeout = 10000"), sql);
  }


  /** Sub-second durations round-trip through {@code toMillis()}. */
  @Test
  public void testSetLockTimeoutSqlSubSecond() {
    // when
    Optional<String> sql = dialect.setLockTimeoutSql(Duration.ofMillis(500));

    // then
    assertEquals(Optional.of("SET lock_timeout = 500"), sql);
  }


  /** {@code RESET lock_timeout} restores the session default. */
  @Test
  public void testResetLockTimeoutSqlReturnsExpectedValue() {
    // when
    Optional<String> sql = dialect.resetLockTimeoutSql();

    // then
    assertEquals(Optional.of("RESET lock_timeout"), sql);
  }


  /** PostgreSQL requires autocommit for the build path because {@code CREATE INDEX CONCURRENTLY} can't run inside a transaction block. */
  @Test
  public void testDeferredIndexBuildRequiresAutoCommit() {
    // when / then
    assertTrue(dialect.deferredIndexBuildRequiresAutoCommit());
  }
}
