/**
 *
 */
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 *
 */
public class TestInlineTableUpgrader {

  private static final String ID_TABLE_NAME = "idTable";

  private InlineTableUpgrader upgrader;
  private Schema              schema;
  private SqlDialect          sqlDialect;
  private SqlStatementWriter  sqlStatementWriter;

  /**
   * Setup method run before each test.
   */
  @Before
  public void setUp() {
    schema = mock(Schema.class);
    sqlDialect = mock(SqlDialect.class);
    sqlStatementWriter = mock(SqlStatementWriter.class);
    upgrader = new InlineTableUpgrader(schema, sqlDialect, sqlStatementWriter, SqlDialect.IdTable.withDeterministicName(ID_TABLE_NAME));
  }


  /**
   * Test that the temporary ID table is created during the preUpgrade step.
   */
  @Test
  public void testPreUpgrade() {
    final ArgumentCaptor<Table> captor = ArgumentCaptor.forClass(Table.class);
    upgrader.preUpgrade();
    verify(sqlDialect).tableDeploymentStatements(captor.capture());
    assertTrue("Temporary table", captor.getValue().isTemporary());
  }


  /**
   * Verify that the temporary ID table is deleted at the end of the upgrade.
   */
  @Test
  public void testPostUpgrade() {
    final ArgumentCaptor<Table> truncateCaptor = ArgumentCaptor.forClass(Table.class);
    final ArgumentCaptor<Table> dropCaptor = ArgumentCaptor.forClass(Table.class);

    upgrader.postUpgrade();

    verify(sqlDialect).truncateTableStatements(truncateCaptor.capture());
    verify(sqlDialect).dropStatements(dropCaptor.capture());
    assertTrue("Truncate temporary table", truncateCaptor.getValue().isTemporary());
    assertTrue("Drop temporary table", dropCaptor.getValue().isTemporary());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddTable)}.
   */
  @Test
  public void testVisitAddTable() {
    // given
    AddTable addTable = mock(AddTable.class);
    given(addTable.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(addTable);

    // then
    verify(addTable).apply(schema);
    verify(sqlDialect, atLeastOnce()).tableDeploymentStatements(any(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class)); // deploying the specified table and indexes
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveTable)}.
   */
  @Test
  public void testVisitRemoveTable() {
    // given
    RemoveTable removeTable = mock(RemoveTable.class);
    given(removeTable.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(removeTable);

    // then
    verify(removeTable).apply(schema);
    verify(sqlDialect).dropStatements(any(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddIndex)}.
   */
  @Test
  public void testVisitAddIndex() {
    // given
    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(addIndex);

    // then
    verify(addIndex).apply(schema);
    verify(sqlDialect).addIndexStatements(any(Table.class), any(Index.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddColumn)}.
   */
  @Test
  public void testVisitAddColumn() {
    // given
    AddColumn addColumn = mock(AddColumn.class);
    given(addColumn.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(addColumn);

    // then
    verify(addColumn).apply(schema);
    verify(sqlDialect).alterTableAddColumnStatements(any(Table.class), any(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ChangeColumn)}.
   */
  @Test
  public void testVisitChangeColumn() {
    // given
    ChangeColumn changeColumn = mock(ChangeColumn.class);
    given(changeColumn.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(changeColumn);

    // then
    verify(changeColumn).apply(schema);
    verify(sqlDialect).alterTableChangeColumnStatements(any(Table.class), any(Column.class), any(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveColumn)}.
   */
  @Test
  public void testVisitRemoveColumn() {
    // given
    RemoveColumn removeColumn = mock(RemoveColumn.class);
    given(removeColumn.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(removeColumn);

    // then
    verify(removeColumn).apply(schema);
    verify(sqlDialect).alterTableDropColumnStatements(any(Table.class), any(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveIndex)}.
   */
  @Test
  public void testVisitRemoveIndex() {
    // given
    RemoveIndex removeIndex = mock(RemoveIndex.class);
    given(removeIndex.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(removeIndex);

    // then
    verify(removeIndex).apply(schema);
    verify(sqlDialect).indexDropStatements(any(Table.class), any(Index.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ChangeIndex)}.
   */
  @Test
  public void testVisitChangeIndex() {
    // given
    ChangeIndex changeIndex = mock(ChangeIndex.class);
    given(changeIndex.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(changeIndex);

    // then
    verify(changeIndex).apply(schema);
    verify(sqlDialect).indexDropStatements(any(Table.class), any(Index.class));
    verify(sqlDialect).addIndexStatements(any(Table.class), any(Index.class));
    verify(sqlStatementWriter, times(2)).writeSql(anyCollectionOf(String.class)); // index drop and index deployment
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testVisitInsertStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    InsertStatement insertStatement = mock(InsertStatement.class);
    given(executeStatement.getStatement()).willReturn(insertStatement);
    when(sqlDialect.convertStatementToSQL(eq((Statement)insertStatement), any(Schema.class), any(Table.class))).thenCallRealMethod();

    // when
    upgrader.visit(executeStatement);

    // then
    ArgumentCaptor<SqlDialect.IdTable> captor = ArgumentCaptor.forClass(SqlDialect.IdTable.class);
    verify(sqlDialect).convertStatementToSQL(Mockito.eq(insertStatement), Mockito.eq(schema), captor.capture());
    assertEquals("Id Table name differed", ID_TABLE_NAME, captor.getValue().getName());
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testVisitUpdateStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    UpdateStatement updateStatement = mock(UpdateStatement.class);
    given(executeStatement.getStatement()).willReturn(updateStatement);
    when(sqlDialect.convertStatementToSQL(eq((Statement)updateStatement), any(Schema.class), any(Table.class))).thenCallRealMethod();

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(updateStatement);
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testVisitDeleteStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    DeleteStatement deleteStatement = mock(DeleteStatement.class);
    given(executeStatement.getStatement()).willReturn(deleteStatement);
    when(sqlDialect.convertStatementToSQL(eq((Statement)deleteStatement), any(Schema.class), any(Table.class))).thenCallRealMethod();

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(deleteStatement);
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testVisitMergeStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    MergeStatement mergeStatement = mock(MergeStatement.class);
    given(executeStatement.getStatement()).willReturn(mergeStatement);
    when(sqlDialect.convertStatementToSQL(eq((Statement)mergeStatement), any(Schema.class), any(Table.class))).thenCallRealMethod();

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(mergeStatement);
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testVisitStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    Statement statement = mock(Statement.class);
    given(executeStatement.getStatement()).willReturn(statement);
    when(sqlDialect.convertStatementToSQL(eq(statement), any(Schema.class), any(Table.class))).thenCallRealMethod();

    // when
    try {
      upgrader.visit(executeStatement);
      fail("UnsupportedOperationException expected");
    } catch (UnsupportedOperationException e) {
      // Correct!
    }
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)}.
   */
  @Test
  public void testExecutePortableSqlStatement() {
    // given
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    PortableSqlStatement statement = mock(PortableSqlStatement.class);
    given(executeStatement.getStatement()).willReturn(statement);

    DatabaseType databaseType = mock(DatabaseType.class);
    given(databaseType.identifier()).willReturn("Foo");
    given(sqlDialect.getDatabaseType()).willReturn(databaseType);

    // when
    upgrader.visit(executeStatement);

    // then
    verify(statement).getStatement(eq("Foo"), anyString());
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }


  @Test
  public void testAnalyseTableStatment(){
    // given
    AnalyseTable analyseTable = mock(AnalyseTable.class);
    given(analyseTable.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(analyseTable);

    // then
    verify(sqlDialect).getSqlForAnalyseTable(any(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollectionOf(String.class));
  }
}
