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
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.sql.element.Criterion.and;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

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
    dao = new DeferredIndexOperationDAOImpl(sqlScriptExecutorProvider, sqlDialect);
  }


  /**
   * Verify insertOperation produces one INSERT for the main table and one
   * for each column, then executes all statements in a single batch.
   */
  @Test
  public void testInsertOperation() {
    DeferredIndexOperation op = buildOperation("op1", List.of("colA", "colB"));

    dao.insertOperation(op);

    // 1 insert for main row + 2 for columns = 3 convertStatementToSQL calls
    ArgumentCaptor<InsertStatement> captor = ArgumentCaptor.forClass(InsertStatement.class);
    verify(sqlDialect, times(3)).convertStatementToSQL(captor.capture());

    List<InsertStatement> inserts = captor.getAllValues();

    String expectedMain = insert().into(tableRef(TABLE))
      .values(
        literal("op1").as("operationId"),
        literal("uuid-1").as("upgradeUUID"),
        literal("MyTable").as("tableName"),
        literal("MyIndex").as("indexName"),
        literal(DeferredIndexOperationType.ADD.name()).as("operationType"),
        literal(0).as("indexUnique"),
        literal(DeferredIndexStatus.PENDING.name()).as("status"),
        literal(0).as("retryCount"),
        literal(20260101120000L).as("createdTime")
      ).toString();

    assertEquals("Main-table INSERT", expectedMain, inserts.get(0).toString());
    assertEquals("Column-table INSERT 0", tableRef(COL_TABLE).getName(), inserts.get(1).getTable().getName());
    assertEquals("Column-table INSERT 1", tableRef(COL_TABLE).getName(), inserts.get(2).getTable().getName());

    verify(sqlScriptExecutor).execute(anyList());
  }


  /**
   * Verify findPendingOperations selects from the correct table with
   * a WHERE status = PENDING clause.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFindPendingOperations() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(List.of());

    dao.findPendingOperations();

    ArgumentCaptor<SelectStatement> captor = ArgumentCaptor.forClass(SelectStatement.class);
    verify(sqlDialect, times(1)).convertStatementToSQL(captor.capture());

    String expected = select(
        field("operationId"), field("upgradeUUID"), field("tableName"),
        field("indexName"), field("operationType"), field("indexUnique"),
        field("status"), field("retryCount"), field("createdTime"),
        field("startedTime"), field("completedTime"), field("errorMessage")
      ).from(tableRef(TABLE))
       .where(field("status").eq(DeferredIndexStatus.PENDING.name()))
       .toString();

    assertEquals("SELECT statement", expected, captor.getValue().toString());
  }


  /**
   * Verify findStaleInProgressOperations selects with WHERE status=IN_PROGRESS
   * AND startedTime < threshold.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testFindStaleInProgressOperations() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(List.of());

    dao.findStaleInProgressOperations(20260101080000L);

    ArgumentCaptor<SelectStatement> captor = ArgumentCaptor.forClass(SelectStatement.class);
    verify(sqlDialect, times(1)).convertStatementToSQL(captor.capture());

    String expected = select(
        field("operationId"), field("upgradeUUID"), field("tableName"),
        field("indexName"), field("operationType"), field("indexUnique"),
        field("status"), field("retryCount"), field("createdTime"),
        field("startedTime"), field("completedTime"), field("errorMessage")
      ).from(tableRef(TABLE))
       .where(and(
         field("status").eq(DeferredIndexStatus.IN_PROGRESS.name()),
         field("startedTime").lessThan(literal(20260101080000L))
       ))
       .toString();

    assertEquals("SELECT statement", expected, captor.getValue().toString());
  }


  /**
   * Verify existsByUpgradeUUIDAndIndexName selects with WHERE on both fields
   * and returns the result of ResultSet::next.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testExistsByUpgradeUUIDAndIndexNameTrue() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(true);

    boolean result = dao.existsByUpgradeUUIDAndIndexName("uuid-1", "MyIndex");

    assertTrue("Should return true when record exists", result);

    ArgumentCaptor<SelectStatement> captor = ArgumentCaptor.forClass(SelectStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = select(field("operationId"))
      .from(tableRef(TABLE))
      .where(and(
        field("upgradeUUID").eq("uuid-1"),
        field("indexName").eq("MyIndex")
      ))
      .toString();

    assertEquals("SELECT statement", expected, captor.getValue().toString());
  }


  /**
   * Verify existsByUpgradeUUIDAndIndexName returns false when no record exists.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testExistsByUpgradeUUIDAndIndexNameFalse() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenReturn(false);

    assertFalse("Should return false when no record exists",
      dao.existsByUpgradeUUIDAndIndexName("uuid-x", "NoIndex"));
  }


  /**
   * Verify markStarted produces an UPDATE setting status=IN_PROGRESS and startedTime.
   */
  @Test
  public void testMarkStarted() {
    dao.markStarted("op1", 20260101120000L);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.IN_PROGRESS.name()).as("status"),
        literal(20260101120000L).as("startedTime")
      )
      .where(field("operationId").eq("op1"))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
    verify(sqlScriptExecutor).execute("UPDATE_SQL");
  }


  /**
   * Verify markCompleted produces an UPDATE setting status=COMPLETED and completedTime.
   */
  @Test
  public void testMarkCompleted() {
    dao.markCompleted("op1", 20260101130000L);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.COMPLETED.name()).as("status"),
        literal(20260101130000L).as("completedTime")
      )
      .where(field("operationId").eq("op1"))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  /**
   * Verify markFailed produces an UPDATE setting status=FAILED, errorMessage,
   * and the updated retryCount.
   */
  @Test
  public void testMarkFailed() {
    dao.markFailed("op1", "Something went wrong", 2);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(
        literal(DeferredIndexStatus.FAILED.name()).as("status"),
        literal("Something went wrong").as("errorMessage"),
        literal(2).as("retryCount")
      )
      .where(field("operationId").eq("op1"))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  /**
   * Verify resetToPending produces an UPDATE setting status=PENDING.
   */
  @Test
  public void testResetToPending() {
    dao.resetToPending("op1");

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(literal(DeferredIndexStatus.PENDING.name()).as("status"))
      .where(field("operationId").eq("op1"))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  /**
   * Verify updateStatus produces an UPDATE setting status to the supplied value.
   */
  @Test
  public void testUpdateStatus() {
    dao.updateStatus("op1", DeferredIndexStatus.COMPLETED);

    ArgumentCaptor<UpdateStatement> captor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(captor.capture());

    String expected = update(tableRef(TABLE))
      .set(literal(DeferredIndexStatus.COMPLETED.name()).as("status"))
      .where(field("operationId").eq("op1"))
      .toString();

    assertEquals("UPDATE statement", expected, captor.getValue().toString());
  }


  private DeferredIndexOperation buildOperation(String operationId, List<String> columns) {
    DeferredIndexOperation op = new DeferredIndexOperation();
    op.setOperationId(operationId);
    op.setUpgradeUUID("uuid-1");
    op.setTableName("MyTable");
    op.setIndexName("MyIndex");
    op.setOperationType(DeferredIndexOperationType.ADD);
    op.setIndexUnique(false);
    op.setStatus(DeferredIndexStatus.PENDING);
    op.setRetryCount(0);
    op.setCreatedTime(20260101120000L);
    op.setColumnNames(columns);
    return op;
  }
}
