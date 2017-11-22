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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.upgrade.UpgradeStatusTableService.UPGRADE_STATUS;
import static org.alfasoftware.morf.upgrade.UpgradeStatusTableServiceImpl.STATUS_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test {@link UpgradeStatusTableServiceImpl} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class TestUpgradeStatusTableServiceImpl {

  private SqlScriptExecutorProvider         sqlScriptExecutorProvider;
  private SqlScriptExecutor                 sqlScriptExecutor;
  private SqlDialect                        sqlDialect;
  private UpgradeStatusTableService         upgradeStatusTableService;

  private final TableReference upgradeStatusTable = tableRef(UPGRADE_STATUS);


  /**
   * Set up mocks.
   */
  @Before
  public void setUp() {
    sqlScriptExecutorProvider = mock(SqlScriptExecutorProvider.class);
    sqlScriptExecutor = mock(SqlScriptExecutor.class);
    sqlDialect = mock(SqlDialect.class);

    when(sqlScriptExecutorProvider.get()).thenReturn(sqlScriptExecutor);
    upgradeStatusTableService = new UpgradeStatusTableServiceImpl(sqlScriptExecutorProvider, sqlDialect);
  }


  /**
   * Test {@link UpgradeStatusTableServiceImpl#getStatus()} when the table doesn't
   * exist.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testGetStatusWhenTableNotPresent() {
    when(sqlScriptExecutor.executeQuery(anyString(), any(ResultSetProcessor.class))).thenThrow(RuntimeSqlException.class);
    assertEquals("Result", UpgradeStatus.NONE, upgradeStatusTableService.getStatus());
  }


  /**
   * Test {@link UpgradeStatusTableServiceImpl#getStatus()} when the table exists.
   *
   * @throws SQLException if something goes wrong with the mocking.
   */
  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testGetStatusWhenTablePresent() throws SQLException {
    ArgumentCaptor<ResultSetProcessor> captor = ArgumentCaptor.forClass(ResultSetProcessor.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(sqlScriptExecutor.executeQuery(anyString(), captor.capture())).thenAnswer((invocation) -> {
      ResultSetProcessor processor = (ResultSetProcessor) invocation.getArguments()[1];
      assertSame("Processor", captor.getValue(), processor);

      when(resultSet.getString(1)).thenReturn("IN_PROGRESS");
      return processor.process(resultSet);
    });
    assertEquals("Result", UpgradeStatus.IN_PROGRESS, upgradeStatusTableService.getStatus());
    verify(resultSet).next();
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#NONE} to {@link UpgradeStatus#IN_PROGRESS} will create the table
   * and set the status to {@link UpgradeStatus#IN_PROGRESS}.
   */
  @Test
  public void testUpdateFromNoneToInProgress() {
    //Test
    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS);

    //Verify it creates the right table
    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    verify(sqlDialect).tableDeploymentStatements(tableCaptor.capture());
    assertEquals("Table name", tableCaptor.getValue().getName(), UpgradeStatusTableService.UPGRADE_STATUS);

    //Verify it inserts the right value
    String expectedStmt = insert().into(upgradeStatusTable).values(literal(UpgradeStatus.IN_PROGRESS.name()).as(STATUS_COLUMN)).toString();
    ArgumentCaptor<InsertStatement> stmtCaptor = ArgumentCaptor.forClass(InsertStatement.class);

    verify(sqlDialect).convertStatementToSQL(stmtCaptor.capture());

    assertEquals("SQL", expectedStmt, stmtCaptor.getValue().toString());
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#IN_PROGRESS} to {@link UpgradeStatus#DATA_TRANSFER_REQUIRED} will update the
   * status of the table.
   */
  @Test
  public void testUpdateFromInProgressToTransferRequired() {
    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED);

    ArgumentCaptor<UpdateStatement> stmtCaptor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(stmtCaptor.capture());
    String expectedStmt = update(upgradeStatusTable).set(literal(UpgradeStatus.DATA_TRANSFER_REQUIRED.name()).as(STATUS_COLUMN))
         .where(upgradeStatusTable.field(STATUS_COLUMN).eq(UpgradeStatus.IN_PROGRESS.name())).toString();

    assertEquals("SQL", expectedStmt, stmtCaptor.getValue().toString());
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#DATA_TRANSFER_REQUIRED} to {@link UpgradeStatus#DATA_TRANSFER_IN_PROGRESS} will update the
   * status of the table.
   */
  @Test
  public void testUpdateFromTransferRequiredToDataTransferInProgress() {
    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.DATA_TRANSFER_REQUIRED, UpgradeStatus.DATA_TRANSFER_IN_PROGRESS);

    ArgumentCaptor<UpdateStatement> stmtCaptor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(stmtCaptor.capture());

    String expectedStmt = update(upgradeStatusTable).set(literal(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS.name()).as(STATUS_COLUMN))
         .where(upgradeStatusTable.field(STATUS_COLUMN).eq(UpgradeStatus.DATA_TRANSFER_REQUIRED.name())).toString();

    assertEquals("SQL", expectedStmt, stmtCaptor.getValue().toString());
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#DATA_TRANSFER_IN_PROGRESS} to {@link UpgradeStatus#COMPLETED} will update the
   * status of the table.
   */
  @Test
  public void testUpdateFromDataTransferInProgressToCompleted() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS);
    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS, UpgradeStatus.COMPLETED);

    ArgumentCaptor<UpdateStatement> stmtCaptor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(stmtCaptor.capture());

    String expectedStmt = update(upgradeStatusTable).set(literal(UpgradeStatus.COMPLETED.name()).as(STATUS_COLUMN))
         .where(upgradeStatusTable.field(STATUS_COLUMN).eq(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS.name())).toString();

    assertEquals("SQL", expectedStmt, stmtCaptor.getValue().toString());
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#DATA_TRANSFER_REQUIRED} to {@link UpgradeStatus#COMPLETED} will update
   * the status of the table.
   */
  @Test
  public void testUpdateFromDataTransferRequiredToCompleted() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.DATA_TRANSFER_REQUIRED);
    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.DATA_TRANSFER_REQUIRED, UpgradeStatus.COMPLETED);

    ArgumentCaptor<UpdateStatement> stmtCaptor = ArgumentCaptor.forClass(UpdateStatement.class);
    verify(sqlDialect).convertStatementToSQL(stmtCaptor.capture());

    String expectedStmt = update(upgradeStatusTable).set(literal(UpgradeStatus.COMPLETED.name()).as(STATUS_COLUMN))
         .where(upgradeStatusTable.field(STATUS_COLUMN).eq(UpgradeStatus.DATA_TRANSFER_REQUIRED.name())).toString();

    assertEquals("SQL", expectedStmt, stmtCaptor.getValue().toString());
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#DATA_TRANSFER_REQUIRED} to {@link UpgradeStatus#COMPLETED}
   * returns 0 when current status is not DATA_TRANSFER_REQUIRED
   */
  @Test
  public void testUpdateFromDataTransferRequiredToCompletedFail() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS);

    int result = upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.DATA_TRANSFER_REQUIRED, UpgradeStatus.COMPLETED);
    assertEquals("Result", 0 , result);
  }



  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * from {@link UpgradeStatus#DATA_TRANSFER_REQUIRED} to {@link UpgradeStatus#COMPLETED}
   * work when current status is {UpgradeStatus#NONE}.
   */
  @Test
  public void testUpdateFromDataTransferRequiredToCompletedNone() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.NONE);

    int result = upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.DATA_TRANSFER_REQUIRED, UpgradeStatus.COMPLETED);
    assertEquals("Result", 0, result);
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * return 0 if it has thrown an error but the current status is already equals to {@code toStatus}.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testWriteStatusFromStatusWithCurrentStatusEqualsToStatus() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.DATA_TRANSFER_REQUIRED);
    when(sqlScriptExecutor.execute(anyList())).thenThrow(new RuntimeSqlException(new SQLException()));

    int result = upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED);
    assertEquals("Result", 0, result);
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * make a new attempt to execute the script if it has thrown an error
   * but the current status is still equals to {@code fromStatus}.
   */
  @Test
  public void testWriteStatusFromStatusWithCurrentStatusEqualsFromStatus() {
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.IN_PROGRESS);
    when(sqlScriptExecutor.execute(anyListOf(String.class)))
        .thenThrow(new RuntimeSqlException(new SQLException()))
        .thenReturn(0);

    upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED);
    verify(sqlScriptExecutor, times(2)).execute(anyListOf(String.class));
  }


  /**
   * Verify that {@link UpgradeStatusTableService#writeStatusFromStatus(UpgradeStatus, UpgradeStatus)}
   * throw an error if current status is not the expected one.
   */
  @Test
  public void testWriteStatusFromStatusWithCurrentStatusNotEqualsFromStatusOrToStatus() {
    RuntimeSqlException triggeringException = new RuntimeSqlException(new SQLException());
    when(sqlScriptExecutor.executeQuery(any(), any())).thenReturn(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS);
    when(sqlScriptExecutor.execute(anyListOf(String.class)))
        .thenThrow(triggeringException)
        .thenReturn(0);

    try {
      upgradeStatusTableService.writeStatusFromStatus(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED);
      fail("Expected RuntimeSqlException");
    } catch (RuntimeSqlException e) {
      verify(sqlScriptExecutor, times(1)).execute(anyListOf(String.class));
      assertSame("Exception", triggeringException, e);
    }
  }


  /**
   * Verify that {@link UpgradeStatusTableService#tidyUp(DataSource)} delete {@link UpgradeStatusTableService#UPGRADE_STATUS} table
   * @throws SQLException if something goes wrong with the mocking.
   */
  @Test
  public void testTidyUp() throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getConnection()).thenReturn(mock(Connection.class));
    
    upgradeStatusTableService.tidyUp(dataSource);

    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    verify(sqlDialect).dropStatements(tableCaptor.capture());
    assertEquals("Table", tableCaptor.getValue().getName(), UPGRADE_STATUS);
  }
}

