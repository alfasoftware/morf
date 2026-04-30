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

package org.alfasoftware.morf.upgrade.deployedindexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/**
 * Unit tests for {@link DeferredIndexBuildTaskImpl} — one test per branch
 * of the reconciliation algorithm.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexBuildTaskImpl {

  private static final String TABLE = "Product";
  private static final String INDEX = "Product_Idx1";
  private static final String CREATE_SQL = "CREATE INDEX Product_Idx1 ON Product (col1)";
  private static final String DROP_SQL = "DROP INDEX Product_Idx1";
  private static final String LOCK_TIMEOUT_SQL = "SET lock_timeout = 10000";
  private static final String LOCK_TIMEOUT_RESET_SQL = "RESET lock_timeout";

  private ConnectionResources connectionResources;
  private SqlDialect dialect;
  private DataSource dataSource;
  private Connection connection;
  private Statement statement;
  private DeployedIndexesDAO dao;

  private DeferredIndexBuildTaskImpl task;


  @Before
  public void setUp() throws SQLException {
    connectionResources = mock(ConnectionResources.class);
    dialect = mock(SqlDialect.class);
    dataSource = mock(DataSource.class);
    connection = mock(Connection.class);
    statement = mock(Statement.class);
    dao = mock(DeployedIndexesDAO.class);

    when(connectionResources.sqlDialect()).thenReturn(dialect);
    when(connectionResources.getDataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getAutoCommit()).thenReturn(false);
    when(connection.createStatement()).thenReturn(statement);
    // Default for dialects whose Optional return types Mockito wouldn't auto-empty.
    when(dialect.setLockTimeoutSql(any(Duration.class))).thenReturn(Optional.empty());
    when(dialect.resetLockTimeoutSql()).thenReturn(Optional.empty());

    task = new DeferredIndexBuildTaskImpl(rowWith(DeployedIndexStatus.PENDING, 0), connectionResources, dao);
  }


  // ---- Trivial branches --------------------------------------------------

  /** No tracking row found — task no-ops; no DAO writes, no SQL run. */
  @Test
  public void testRowMissingNoOp() throws SQLException {
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.empty());

    task.run();

    verify(dao, never()).markStarted(any(), any(), anyLong(), anyInt());
    verify(dao, never()).markCompleted(any(), any(), anyLong());
    verify(dao, never()).markFailed(any(), any(), any());
    verify(statement, never()).execute(any());
  }


  /** Row already COMPLETED (race) — task no-ops. */
  @Test
  public void testRowCompletedNoOp() throws SQLException {
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.COMPLETED, 0)));

    task.run();

    verify(dao, never()).markStarted(any(), any(), anyLong(), anyInt());
    verify(dao, never()).markCompleted(any(), any(), anyLong());
    verify(dao, never()).markFailed(any(), any(), any());
    verify(statement, never()).execute(any());
  }


  // ---- VALID branch -------------------------------------------------------

  /** Physical index already valid — markCompleted; no SQL run. */
  @Test
  public void testValidMarksCompleted() throws SQLException {
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.IN_PROGRESS, 1)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.TRUE));

    task.run();

    verify(dao).markCompleted(eq(TABLE), eq(INDEX), anyLong());
    verify(dao, never()).markStarted(any(), any(), anyLong(), anyInt());
    verify(dao, never()).markFailed(any(), any(), any());
    verify(statement, never()).execute(any());
  }


  // ---- ABSENT branch ------------------------------------------------------

  /** Physical index absent — markStarted (attempts++), CREATE, markCompleted. */
  @Test
  public void testAbsentHappyPath() throws SQLException {
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.PENDING, 2)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.empty());
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));

    task.run();

    InOrder order = inOrder(dao, statement);
    order.verify(dao).markStarted(eq(TABLE), eq(INDEX), anyLong(), eq(3));
    order.verify(statement).execute(CREATE_SQL);
    order.verify(dao).markCompleted(eq(TABLE), eq(INDEX), anyLong());
    verify(dao, never()).markFailed(any(), any(), any());
  }


  /** Physical index absent + CREATE fails — markStarted then markFailed with the SQL message. */
  @Test
  public void testAbsentCreateFailsMarksFailed() throws SQLException {
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.FAILED, 4)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.empty());
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));
    doThrow(new SQLException("unique constraint violated")).when(statement).execute(CREATE_SQL);

    task.run();

    verify(dao).markStarted(eq(TABLE), eq(INDEX), anyLong(), eq(5));
    verify(dao).markFailed(eq(TABLE), eq(INDEX), eq("unique constraint violated"));
    verify(dao, never()).markCompleted(any(), any(), anyLong());
  }


  // ---- INVALID branch -----------------------------------------------------

  /**
   * Physical index INVALID + dialect supplies lock_timeout — the lock SQL,
   * the DROP, and the CREATE all run on the same connection in order; the
   * lock_timeout is reset in the finally block to avoid leaking into the
   * connection pool.
   *
   * <p>Each phase opens its own {@link Statement} (executeOne/executeAll
   * use try-with-resources). Stubbing distinct mock instances per
   * createStatement() call lets the test verify each {@code execute} against
   * the right phase, so a future refactor that splits work across different
   * connections would be caught.</p>
   */
  @Test
  public void testInvalidHappyPathPostgresLockTimeout() throws SQLException {
    Statement stmtSet = mock(Statement.class);
    Statement stmtDrop = mock(Statement.class);
    Statement stmtCreate = mock(Statement.class);
    Statement stmtReset = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stmtSet, stmtDrop, stmtCreate, stmtReset);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.IN_PROGRESS, 0)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.FALSE));
    when(dialect.setLockTimeoutSql(eq(DeferredIndexBuildTaskImpl.LOCK_TIMEOUT))).thenReturn(Optional.of(LOCK_TIMEOUT_SQL));
    when(dialect.resetLockTimeoutSql()).thenReturn(Optional.of(LOCK_TIMEOUT_RESET_SQL));
    when(dialect.indexDropStatements(any(), any())).thenReturn(List.of(DROP_SQL));
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));

    task.run();

    verify(stmtSet).execute(LOCK_TIMEOUT_SQL);
    verify(stmtDrop).execute(DROP_SQL);
    verify(stmtCreate).execute(CREATE_SQL);
    verify(stmtReset).execute(LOCK_TIMEOUT_RESET_SQL);
    InOrder order = inOrder(dao, stmtSet, stmtDrop, stmtCreate, stmtReset);
    order.verify(dao).markStarted(eq(TABLE), eq(INDEX), anyLong(), eq(1));
    order.verify(stmtSet).execute(LOCK_TIMEOUT_SQL);
    order.verify(stmtDrop).execute(DROP_SQL);
    order.verify(stmtCreate).execute(CREATE_SQL);
    order.verify(dao).markCompleted(eq(TABLE), eq(INDEX), anyLong());
    order.verify(stmtReset).execute(LOCK_TIMEOUT_RESET_SQL);
    verify(dao, never()).markFailed(any(), any(), any());
  }


  /** Dialect does not supply lock_timeout (Oracle/H2) — the SET is skipped; DROP + CREATE proceed; no reset. */
  @Test
  public void testInvalidNoLockTimeoutSkipsSet() throws SQLException {
    Statement stmtDrop = mock(Statement.class);
    Statement stmtCreate = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stmtDrop, stmtCreate);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.PENDING, 0)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.FALSE));
    when(dialect.setLockTimeoutSql(any(Duration.class))).thenReturn(Optional.empty());
    when(dialect.indexDropStatements(any(), any())).thenReturn(List.of(DROP_SQL));
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));

    task.run();

    verify(stmtDrop).execute(DROP_SQL);
    verify(stmtCreate).execute(CREATE_SQL);
    verify(dao).markCompleted(eq(TABLE), eq(INDEX), anyLong());
  }


  /** INVALID + DROP fails (e.g. lock timeout) — markFailed with the "could not drop" prefix; CREATE not attempted; lock_timeout still reset. */
  @Test
  public void testInvalidDropFailsMarksFailedWithPrefixAndDoesNotCreate() throws SQLException {
    Statement stmtSet = mock(Statement.class);
    Statement stmtDrop = mock(Statement.class);
    Statement stmtReset = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stmtSet, stmtDrop, stmtReset);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.FAILED, 7)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.FALSE));
    when(dialect.setLockTimeoutSql(any(Duration.class))).thenReturn(Optional.of(LOCK_TIMEOUT_SQL));
    when(dialect.resetLockTimeoutSql()).thenReturn(Optional.of(LOCK_TIMEOUT_RESET_SQL));
    when(dialect.indexDropStatements(any(), any())).thenReturn(List.of(DROP_SQL));
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));
    doThrow(new SQLException("canceling statement due to lock timeout")).when(stmtDrop).execute(DROP_SQL);

    task.run();

    verify(dao).markStarted(eq(TABLE), eq(INDEX), anyLong(), eq(8));
    ArgumentCaptor<String> errMsg = ArgumentCaptor.forClass(String.class);
    verify(dao).markFailed(eq(TABLE), eq(INDEX), errMsg.capture());
    assertTrue("expected 'could not drop' prefix; got: " + errMsg.getValue(),
        errMsg.getValue().startsWith("could not drop invalid leftover: "));
    verify(stmtSet).execute(LOCK_TIMEOUT_SQL);
    verify(stmtReset).execute(LOCK_TIMEOUT_RESET_SQL);
    // CREATE is never attempted — verify on the statement-pool level via createStatement count.
    verify(connection, times(3)).createStatement();
    verify(dao, never()).markCompleted(any(), any(), anyLong());
  }


  /** INVALID + DROP succeeds + CREATE fails — markFailed with the raw SQL message (no prefix). */
  @Test
  public void testInvalidCreateAfterDropFailsMarksFailedWithRawMessage() throws SQLException {
    Statement stmtDrop = mock(Statement.class);
    Statement stmtCreate = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stmtDrop, stmtCreate);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.IN_PROGRESS, 1)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.FALSE));
    when(dialect.setLockTimeoutSql(any(Duration.class))).thenReturn(Optional.empty());
    when(dialect.indexDropStatements(any(), any())).thenReturn(List.of(DROP_SQL));
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));
    doThrow(new SQLException("disk full")).when(stmtCreate).execute(CREATE_SQL);

    task.run();

    verify(dao).markFailed(eq(TABLE), eq(INDEX), eq("disk full"));
    verify(stmtDrop).execute(DROP_SQL);
    verify(stmtCreate).execute(CREATE_SQL);
  }


  /**
   * INVALID + lock_timeout SET fails — failure is swallowed (best-effort fail-fast),
   * DROP and CREATE proceed; reset is NOT issued because we never successfully set
   * the lock_timeout in the first place.
   */
  @Test
  public void testInvalidLockTimeoutSetFailsStillProceeds() throws SQLException {
    Statement stmtSet = mock(Statement.class);
    Statement stmtDrop = mock(Statement.class);
    Statement stmtCreate = mock(Statement.class);
    when(connection.createStatement()).thenReturn(stmtSet, stmtDrop, stmtCreate);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.PENDING, 0)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.FALSE));
    when(dialect.setLockTimeoutSql(any(Duration.class))).thenReturn(Optional.of(LOCK_TIMEOUT_SQL));
    when(dialect.resetLockTimeoutSql()).thenReturn(Optional.of(LOCK_TIMEOUT_RESET_SQL));
    when(dialect.indexDropStatements(any(), any())).thenReturn(List.of(DROP_SQL));
    when(dialect.deferredIndexDeploymentStatements(any(), any())).thenReturn(List.of(CREATE_SQL));
    doThrow(new SQLException("permission denied")).when(stmtSet).execute(LOCK_TIMEOUT_SQL);

    task.run();

    verify(stmtDrop).execute(DROP_SQL);
    verify(stmtCreate).execute(CREATE_SQL);
    verify(dao).markCompleted(eq(TABLE), eq(INDEX), anyLong());
    // No 4th createStatement (no reset path engaged).
    verify(connection, times(3)).createStatement();
  }


  // ---- Connection lifecycle ----------------------------------------------

  /**
   * When the dialect declares it requires autocommit (PG, because
   * {@code CREATE INDEX CONCURRENTLY} can't run in a transaction block),
   * the build task flips autocommit on for the work and restores the prior
   * value on close.
   */
  @Test
  public void testAutoCommitSetTrueAndRestoredWhenDialectRequires() throws SQLException {
    when(dialect.deferredIndexBuildRequiresAutoCommit()).thenReturn(true);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.PENDING, 0)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.TRUE));
    when(connection.getAutoCommit()).thenReturn(false);

    task.run();

    InOrder order = inOrder(connection);
    order.verify(connection).getAutoCommit();
    order.verify(connection).setAutoCommit(true);
    order.verify(connection).setAutoCommit(false);  // restored
  }


  /**
   * When the dialect does NOT require autocommit (Oracle, H2 — DDL is
   * implicitly committed regardless), the build task leaves the connection's
   * autocommit state alone — neither read nor written.
   */
  @Test
  public void testAutoCommitNotTouchedWhenDialectDoesNotRequire() throws SQLException {
    when(dialect.deferredIndexBuildRequiresAutoCommit()).thenReturn(false);
    when(dao.findByTableAndIndex(TABLE, INDEX)).thenReturn(Optional.of(rowWith(DeployedIndexStatus.PENDING, 0)));
    when(dialect.isIndexValid(connection, TABLE, INDEX)).thenReturn(Optional.of(Boolean.TRUE));

    task.run();

    verify(connection, never()).getAutoCommit();
    verify(connection, never()).setAutoCommit(anyBoolean());
  }


  /** Unexpected SQLException from getConnection propagates as RuntimeSqlException — not caught + persisted. */
  @Test
  public void testUnexpectedSqlExceptionPropagatesAsRuntimeSqlException() throws SQLException {
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    RuntimeSqlException thrown = assertThrows(RuntimeSqlException.class, task::run);
    assertTrue(thrown.getMessage().contains(TABLE + "." + INDEX));
    verify(dao, never()).markFailed(any(), any(), any());
  }


  /**
   * Unexpected DAO failure during the row re-fetch propagates as a
   * {@link RuntimeException}; the task does not catch it or persist it as
   * FAILED — the next pass retries from a fresh connection.
   */
  @Test
  public void testDaoFindByTableAndIndexThrowsPropagates() {
    when(dao.findByTableAndIndex(TABLE, INDEX))
        .thenThrow(new RuntimeSqlException("tracking-table connection broken", new SQLException("conn closed")));

    RuntimeException thrown = assertThrows(RuntimeException.class, task::run);
    assertTrue("expected the DAO failure to propagate; got: " + thrown.getMessage(),
        thrown.getMessage().contains("tracking-table connection broken"));
    verify(dao, never()).markStarted(any(), any(), anyLong(), anyInt());
    verify(dao, never()).markCompleted(any(), any(), anyLong());
    verify(dao, never()).markFailed(any(), any(), any());
  }


  // ---- Trivial getters ---------------------------------------------------

  /** Identity getters reflect the constructor arguments. */
  @Test
  public void testIdentityGetters() {
    assertEquals(TABLE, task.getTableName());
    assertEquals(INDEX, task.getIndexName());
  }


  /** Snapshot getters expose status, attemptsCount, and errorMessage from the row captured at construction. */
  @Test
  public void testSnapshotGettersExposeRowStateAtConstructionTime() {
    DeployedIndex row = rowWith(DeployedIndexStatus.FAILED, 3);
    row.setErrorMessage("disk full");
    DeferredIndexBuildTaskImpl t = new DeferredIndexBuildTaskImpl(row, connectionResources, dao);

    assertEquals(DeployedIndexStatus.FAILED, t.getStatus());
    assertEquals(3, t.getAttemptsCount());
    assertEquals(Optional.of("disk full"), t.getErrorMessage());
  }


  /** When the row has never failed, errorMessage is empty (not "" or null-leak). */
  @Test
  public void testSnapshotGettersErrorMessageEmptyWhenNeverFailed() {
    DeployedIndex row = rowWith(DeployedIndexStatus.PENDING, 0);
    row.setErrorMessage(null);
    DeferredIndexBuildTaskImpl t = new DeferredIndexBuildTaskImpl(row, connectionResources, dao);

    assertEquals(Optional.empty(), t.getErrorMessage());
  }


  // ---- Helpers -----------------------------------------------------------

  private static DeployedIndex rowWith(DeployedIndexStatus status, int attempts) {
    DeployedIndex row = new DeployedIndex();
    row.setTableName(TABLE);
    row.setIndexName(INDEX);
    row.setIndexUnique(false);
    row.setIndexColumns(List.of("col1"));
    row.setStatus(status);
    row.setAttemptsCount(attempts);
    return row;
  }
}
