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

package org.alfasoftware.morf.upgrade.deferred;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link DeferredIndexOperationDAOImpl}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexOperationDAOImpl {

  @Mock private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Mock private SqlScriptExecutor sqlScriptExecutor;
  @Mock private SqlDialect sqlDialect;
  @Mock private ConnectionResources connectionResources;

  private DeferredIndexOperationDAO dao;

  private static final String TABLE     = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_NAME;
  private static final String COL_TABLE = DatabaseUpgradeTableContribution.DEFERRED_INDEX_OPERATION_COLUMN_NAME;


  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(sqlScriptExecutorProvider.get()).thenReturn(sqlScriptExecutor);
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(List.of("SQL"));
    when(sqlDialect.convertStatementToSQL(any(UpdateStatement.class))).thenReturn("UPDATE_SQL");
    when(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).thenReturn("SELECT_SQL");
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);
    dao = new DeferredIndexOperationDAOImpl(sqlScriptExecutorProvider, connectionResources);
  }


  /**
   * Verify findPendingOperations selects from the correct table with
   * a LEFT JOIN to the column table and WHERE status = PENDING clause.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFindPendingOperations() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(List.of());

    dao.findPendingOperations();

    ArgumentCaptor<SelectStatement> captor = ArgumentCaptor.forClass(SelectStatement.class);
    verify(sqlDialect, times(1)).convertStatementToSQL(captor.capture());

    org.alfasoftware.morf.sql.element.TableReference op = tableRef(TABLE);
    org.alfasoftware.morf.sql.element.TableReference col = tableRef(COL_TABLE);

    String expected = select(
        op.field("id"), op.field("upgradeUUID"), op.field("tableName"),
        op.field("indexName"), op.field("indexUnique"),
        op.field("status"), op.field("retryCount"), op.field("createdTime"),
        op.field("startedTime"), op.field("completedTime"), op.field("errorMessage"),
        col.field("columnName"), col.field("columnSequence")
      ).from(op)
       .leftOuterJoin(col, op.field("id").eq(col.field("operationId")))
       .where(op.field("status").eq(DeferredIndexStatus.PENDING.name()))
       .orderBy(op.field("id"), col.field("columnSequence"))
       .toString();

    assertEquals("SELECT statement", expected, captor.getValue().toString());
  }


  /**
   * Verify markStarted produces an UPDATE setting status=IN_PROGRESS and startedTime.
   */
  @Test
  public void testMarkStarted() {
    dao.markStarted(1001L, 20260101120000L);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.IN_PROGRESS.name()).as("status"),
        literal(20260101120000L).as("startedTime")
      )
      .where(field("id").eq(1001L))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
    verify(sqlScriptExecutor).execute("UPDATE_SQL");
  }


  /**
   * Verify markCompleted produces an UPDATE setting status=COMPLETED and completedTime.
   */
  @Test
  public void testMarkCompleted() {
    dao.markCompleted(1001L, 20260101130000L);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.COMPLETED.name()).as("status"),
        literal(20260101130000L).as("completedTime")
      )
      .where(field("id").eq(1001L))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  /**
   * Verify markFailed produces an UPDATE setting status=FAILED, errorMessage,
   * and the updated retryCount.
   */
  @Test
  public void testMarkFailed() {
    dao.markFailed(1001L, "Something went wrong", 2);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.FAILED.name()).as("status"),
        literal("Something went wrong").as("errorMessage"),
        literal(2).as("retryCount")
      )
      .where(field("id").eq(1001L))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  /**
   * Verify resetToPending produces an UPDATE setting status=PENDING.
   */
  @Test
  public void testResetToPending() {
    dao.resetToPending(1001L);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(literal(DeferredIndexStatus.PENDING.name()).as("status"))
      .where(field("id").eq(1001L))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  private DeferredIndexOperation buildOperation(long id, List<String> columns) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setId(id);
    op.setUpgradeUUID("uuid-1");
    op.setTableName("MyTable");
    op.setIndexName("MyIndex");
    op.setIndexUnique(false);
    op.setStatus(DeferredIndexStatus.PENDING);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setColumnNames(columns);
    return op;
  }
}
