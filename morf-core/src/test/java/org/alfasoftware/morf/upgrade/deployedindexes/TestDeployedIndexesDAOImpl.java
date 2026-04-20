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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link DeployedIndexesDAOImpl}. Mocks the factory,
 * dialect, and executor to verify pure delegation + SQL execution.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeployedIndexesDAOImpl {

  private DeployedIndexesStatementFactory factory;
  private SqlScriptExecutorProvider executorProvider;
  private SqlScriptExecutor executor;
  private SqlDialect dialect;
  private ConnectionResources connectionResources;
  private DeployedIndexesDAOImpl dao;


  @Before
  public void setUp() {
    factory = mock(DeployedIndexesStatementFactory.class);
    executorProvider = mock(SqlScriptExecutorProvider.class);
    executor = mock(SqlScriptExecutor.class);
    dialect = mock(SqlDialect.class);
    connectionResources = mock(ConnectionResources.class);
    when(connectionResources.sqlDialect()).thenReturn(dialect);
    when(executorProvider.get()).thenReturn(executor);
    dao = new DeployedIndexesDAOImpl(executorProvider, connectionResources, factory);
  }


  // ---- read queries ------------------------------------------------------

  /** findAll uses the factory's statement and runs the mapper. */
  @Test
  public void testFindAllConvertsAndExecutes() {
    // given
    org.alfasoftware.morf.sql.SelectStatement stmt = mock(org.alfasoftware.morf.sql.SelectStatement.class);
    when(factory.statementToFindAll()).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("SELECT ...");

    // when
    dao.findAll();

    // then
    verify(factory).statementToFindAll();
    verify(dialect).convertStatementToSQL(stmt);
    verify(executor).executeQuery(any(String.class), any());
  }


  /** findByTable passes tableName through to factory. */
  @Test
  public void testFindByTableDelegates() {
    // given
    org.alfasoftware.morf.sql.SelectStatement stmt = mock(org.alfasoftware.morf.sql.SelectStatement.class);
    when(factory.statementToFindByTable("T1")).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("SELECT ...");

    // when
    dao.findByTable("T1");

    // then
    verify(factory).statementToFindByTable("T1");
  }


  /** findNonTerminalOperations delegates. */
  @Test
  public void testFindNonTerminalOperationsDelegates() {
    // given
    org.alfasoftware.morf.sql.SelectStatement stmt = mock(org.alfasoftware.morf.sql.SelectStatement.class);
    when(factory.statementToFindNonTerminalOperations()).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("SELECT ...");

    // when
    dao.findNonTerminalOperations();

    // then
    verify(factory).statementToFindNonTerminalOperations();
  }


  /** countAllByStatus initialises zero-counts and aggregates the rows. */
  @Test
  public void testCountAllByStatusInitialisesZeros() {
    // given -- executor returns no rows
    org.alfasoftware.morf.sql.SelectStatement stmt = mock(org.alfasoftware.morf.sql.SelectStatement.class);
    when(factory.statementToSelectStatusColumn()).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("SELECT status FROM ...");
    when(executor.executeQuery(any(String.class), any())).thenAnswer(inv -> {
      // invoke the processor with a mock empty ResultSet
      @SuppressWarnings("unchecked")
      org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor<Object> proc =
          (org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor<Object>) inv.getArgument(1);
      ResultSet rs = mock(ResultSet.class);
      when(rs.next()).thenReturn(false);
      return proc.process(rs);
    });

    // when
    Map<DeployedIndexStatus, Integer> result = dao.countAllByStatus();

    // then -- every status present with count 0
    for (DeployedIndexStatus s : DeployedIndexStatus.values()) {
      assertEquals((Integer) 0, result.get(s));
    }
  }


  // ---- status updates ----------------------------------------------------

  /** markStarted builds the statement via the factory and executes it. */
  @Test
  public void testMarkStartedDelegates() {
    // given
    org.alfasoftware.morf.sql.UpdateStatement stmt = mock(org.alfasoftware.morf.sql.UpdateStatement.class);
    when(factory.statementToMarkStarted("T", "I", 123L)).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("UPDATE ...");

    // when
    dao.markStarted("T", "I", 123L);

    // then
    verify(factory).statementToMarkStarted("T", "I", 123L);
    verify(executor).execute(anyCollection());
  }


  /** markCompleted builds the statement via the factory and executes it. */
  @Test
  public void testMarkCompletedDelegates() {
    // given
    org.alfasoftware.morf.sql.UpdateStatement stmt = mock(org.alfasoftware.morf.sql.UpdateStatement.class);
    when(factory.statementToMarkCompleted("T", "I", 456L)).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("UPDATE ...");

    // when
    dao.markCompleted("T", "I", 456L);

    // then
    verify(factory).statementToMarkCompleted("T", "I", 456L);
  }


  /** markFailed issues two statements: status update and retry-count bump. */
  @Test
  public void testMarkFailedIssuesStatusAndRetryUpdates() {
    // given
    org.alfasoftware.morf.sql.UpdateStatement statusStmt = mock(org.alfasoftware.morf.sql.UpdateStatement.class);
    org.alfasoftware.morf.sql.UpdateStatement retryStmt = mock(org.alfasoftware.morf.sql.UpdateStatement.class);
    when(factory.statementToMarkFailed("T", "I", "err")).thenReturn(statusStmt);
    when(factory.statementToBumpRetryCount("T", "I")).thenReturn(retryStmt);
    when(dialect.convertStatementToSQL(statusStmt)).thenReturn("UPDATE status");
    when(dialect.convertStatementToSQL(retryStmt)).thenReturn("UPDATE retry");

    // when
    dao.markFailed("T", "I", "err");

    // then
    verify(factory).statementToMarkFailed("T", "I", "err");
    verify(factory).statementToBumpRetryCount("T", "I");
  }


  /** resetAllInProgressToPending delegates to factory.statementToResetInProgress. */
  @Test
  public void testResetAllInProgressToPendingDelegates() {
    // given
    org.alfasoftware.morf.sql.UpdateStatement stmt = mock(org.alfasoftware.morf.sql.UpdateStatement.class);
    when(factory.statementToResetInProgress()).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("UPDATE ...");

    // when
    dao.resetAllInProgressToPending();

    // then
    verify(factory).statementToResetInProgress();
  }


  // ---- ResultSet mapping -------------------------------------------------

  /** mapEntries copies every column and preserves null startedTime/completedTime via wasNull. */
  @Test
  public void testMapEntriesHandlesNullTimestamps() throws SQLException {
    // given -- a ResultSet with one row, null timestamps
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true, false);
    when(rs.getLong("id")).thenReturn(1L);
    when(rs.getString("tableName")).thenReturn("T");
    when(rs.getString("indexName")).thenReturn("I");
    when(rs.getBoolean("indexUnique")).thenReturn(false);
    when(rs.getString("indexColumns")).thenReturn("c1,c2");
    when(rs.getString("status")).thenReturn("PENDING");
    when(rs.getInt("retryCount")).thenReturn(0);
    when(rs.getLong("createdTime")).thenReturn(42L);
    // startedTime null
    when(rs.getLong("startedTime")).thenReturn(0L);
    // completedTime null
    when(rs.getLong("completedTime")).thenReturn(0L);
    when(rs.wasNull()).thenReturn(true, true);  // startedTime, then completedTime
    when(rs.getString("errorMessage")).thenReturn(null);

    // given -- exercise mapEntries via findAll
    org.alfasoftware.morf.sql.SelectStatement stmt = mock(org.alfasoftware.morf.sql.SelectStatement.class);
    when(factory.statementToFindAll()).thenReturn(stmt);
    when(dialect.convertStatementToSQL(stmt)).thenReturn("SELECT ...");
    when(executor.executeQuery(any(String.class), any())).thenAnswer(inv -> {
      @SuppressWarnings("unchecked")
      org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor<Object> proc =
          (org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor<Object>) inv.getArgument(1);
      return proc.process(rs);
    });

    // when
    List<DeployedIndex> result = dao.findAll();

    // then -- one entry with column data correctly copied
    assertEquals(1, result.size());
    DeployedIndex entry = result.get(0);
    assertEquals(1L, entry.getId());
    assertEquals("T", entry.getTableName());
    assertEquals("I", entry.getIndexName());
    assertEquals(List.of("c1", "c2"), entry.getIndexColumns());
    assertEquals(DeployedIndexStatus.PENDING, entry.getStatus());
    assertEquals(42L, entry.getCreatedTime());
    // null timestamps
    assertEquals(null, entry.getStartedTime());
    assertEquals(null, entry.getCompletedTime());
  }
}
