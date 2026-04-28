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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.mockito.ArgumentMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class TestInlineTableUpgrader {

  private static final String ID_TABLE_NAME = "idTable";

  private InlineTableUpgrader upgrader;
  private Schema              schema;
  private SqlDialect          sqlDialect;
  private SqlStatementWriter  sqlStatementWriter;
  private UpgradeConfigAndContext upgradeConfigAndContext;

  /**
   * Setup method run before each test.
   */
  @Before
  public void setUp() {
    schema = mock(Schema.class);
    sqlDialect = mock(SqlDialect.class);
    sqlStatementWriter = mock(SqlStatementWriter.class);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
    upgradeConfigAndContext.setExclusiveExecutionSteps(Set.of());
    upgradeConfigAndContext.setDeferredIndexCreationEnabled(true);
    when(sqlDialect.supportsDeferredIndexCreation()).thenReturn(true);
    // Default: allow DeployedIndexes DML to be converted without error
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.InsertStatement.class))).thenReturn(List.of("INSERT INTO DeployedIndexes ..."));
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.UpdateStatement.class))).thenReturn("UPDATE DeployedIndexes ...");
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.DeleteStatement.class))).thenReturn("DELETE FROM DeployedIndexes ...");

    upgrader = new InlineTableUpgrader(schema, upgradeConfigAndContext, sqlDialect, sqlStatementWriter, SqlDialect.IdTable.withDeterministicName(ID_TABLE_NAME),
        org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexSession.create());
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
    Table mockTable = mock(Table.class);
    when(mockTable.getName()).thenReturn("TestTable");
    when(mockTable.indexes()).thenReturn(List.of());
    AddTable addTable = mock(AddTable.class);
    given(addTable.apply(schema)).willReturn(schema);
    when(addTable.getTable()).thenReturn(mockTable);

    // when
    upgrader.visit(addTable);

    // then
    verify(addTable).apply(schema);
    verify(sqlDialect, atLeastOnce()).tableDeploymentStatements(nullable(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveTable)}.
   */
  @Test
  public void testVisitRemoveTable() {
    // given
    Table mockTable = mock(Table.class);
    when(mockTable.getName()).thenReturn("SomeTable");
    RemoveTable removeTable = mock(RemoveTable.class);
    given(removeTable.apply(schema)).willReturn(schema);
    when(removeTable.getTable()).thenReturn(mockTable);

    // when
    upgrader.visit(removeTable);

    // then
    verify(removeTable).apply(schema);
    verify(sqlDialect).dropStatements(nullable(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddIndex)}.
   */
  @Test
  public void testVisitAddIndex() {
    // given
    Index newIndex = mock(Index.class);
    when(newIndex.getName()).thenReturn("TestIdx");
    when(newIndex.isDeferred()).thenReturn(false);
    when(newIndex.isUnique()).thenReturn(false);
    when(newIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex.getNewIndex()).thenReturn(newIndex);

    Table newTable = mock(Table.class);
    when(newTable.getName()).thenReturn(ID_TABLE_NAME);
    when(schema.getTable(ID_TABLE_NAME)).thenReturn(newTable);

    // when
    upgrader.visit(addIndex);

    // then
    verify(addIndex).apply(schema);
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddIndex)}.
   */
  @Test
  public void testVisitAddIndexWithPRFIndex() {
    // given
    Index newIndex = mock(Index.class);
    when(newIndex.getName()).thenReturn(ID_TABLE_NAME + "_1");
    when(newIndex.columnNames()).thenReturn(Collections.singletonList("column_1"));
    when(newIndex.isUnique()).thenReturn(false);
    when(newIndex.isDeferred()).thenReturn(false);

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex.getNewIndex()).thenReturn(newIndex);

    Index indexPrf = mock(Index.class);
    when(indexPrf.getName()).thenReturn(ID_TABLE_NAME + "_PRF1");
    when(indexPrf.columnNames()).thenReturn(Collections.singletonList("column_1"));
    when(indexPrf.isUnique()).thenReturn(false);
    Index indexPrf1 = mock(Index.class);
    when(indexPrf1.getName()).thenReturn(ID_TABLE_NAME + "_PRF2");
    when(indexPrf1.columnNames()).thenReturn(List.of("column_2"));
    when(indexPrf1.isUnique()).thenReturn(true);
    Index indexPrf2 = mock(Index.class);
    when(indexPrf2.getName()).thenReturn(ID_TABLE_NAME + "_PRF3");
    when(indexPrf2.columnNames()).thenReturn(List.of("column_3"));
    // idTable_PRF3 isn't unique
    when(indexPrf2.isUnique()).thenReturn(false);

    Map<String, List<Index>> ignoredIndexes = Maps.newHashMap();
    ignoredIndexes.put(ID_TABLE_NAME.toUpperCase(), Lists.newArrayList(indexPrf, indexPrf1, indexPrf2));
    upgradeConfigAndContext.setIgnoredIndexes(ignoredIndexes);

    Index newIndex1 = mock(Index.class);
    when(newIndex1.getName()).thenReturn(ID_TABLE_NAME + "_2");
    when(newIndex1.columnNames()).thenReturn(Collections.singletonList("column_2"));
    when(newIndex1.isUnique()).thenReturn(true);
    when(newIndex1.isDeferred()).thenReturn(false);
    AddIndex addIndex1 = mock(AddIndex.class);
    given(addIndex1.apply(schema)).willReturn(schema);
    when(addIndex1.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex1.getNewIndex()).thenReturn(newIndex1);

    Index newIndex2 = mock(Index.class);
    when(newIndex2.getName()).thenReturn(ID_TABLE_NAME + "_3");
    when(newIndex2.columnNames()).thenReturn(Collections.singletonList("column_4"));
    when(newIndex2.isUnique()).thenReturn(true);
    when(newIndex2.isDeferred()).thenReturn(false);
    AddIndex addIndex2 = mock(AddIndex.class);
    given(addIndex2.apply(schema)).willReturn(schema);
    when(addIndex2.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex2.getNewIndex()).thenReturn(newIndex2);

    Index newIndex3 = mock(Index.class);
    when(newIndex3.getName()).thenReturn(ID_TABLE_NAME + "_4");
    when(newIndex3.columnNames()).thenReturn(Collections.singletonList("column_3"));
    // index 3 is unique idTable_PRF3 has same columns but isn't unique
    when(newIndex3.isUnique()).thenReturn(true);
    when(newIndex3.isDeferred()).thenReturn(false);
    AddIndex addIndex3 = mock(AddIndex.class);
    given(addIndex3.apply(schema)).willReturn(schema);
    when(addIndex3.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex3.getNewIndex()).thenReturn(newIndex3);

    Table newTable = mock(Table.class);
    when(newTable.getName()).thenReturn(ID_TABLE_NAME);
    when(schema.getTable(ID_TABLE_NAME)).thenReturn(newTable);

    // when
    upgrader.visit(addIndex);
    upgrader.visit(addIndex1);
    upgrader.visit(addIndex2);
    upgrader.visit(addIndex3);

    // then
    verify(addIndex).apply(schema);
    verify(sqlDialect).renameIndexStatements(nullable(Table.class), eq(ID_TABLE_NAME + "_PRF1"), eq(ID_TABLE_NAME + "_1"));
    verify(sqlDialect).renameIndexStatements(nullable(Table.class), eq(ID_TABLE_NAME + "_PRF2"), eq(ID_TABLE_NAME + "_2"));
    verify(sqlDialect, never()).renameIndexStatements(nullable(Table.class), eq(ID_TABLE_NAME + "_PRF3"), eq(ID_TABLE_NAME + "_4"));
    verify(sqlDialect, times(2)).addIndexStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlStatementWriter, atLeast(4)).writeSql(anyCollection());
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
    verify(sqlDialect).alterTableAddColumnStatements(nullable(Table.class), nullable(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ChangeColumn)}.
   */
  @Test
  public void testVisitChangeColumn() {
    // given
    Column fromCol = mock(Column.class);
    when(fromCol.getName()).thenReturn("col");
    Column toCol = mock(Column.class);
    when(toCol.getName()).thenReturn("col");
    ChangeColumn changeColumn = mock(ChangeColumn.class);
    given(changeColumn.apply(schema)).willReturn(schema);
    when(changeColumn.getTableName()).thenReturn("SomeTable");
    when(changeColumn.getFromColumn()).thenReturn(fromCol);
    when(changeColumn.getToColumn()).thenReturn(toCol);

    // when
    upgrader.visit(changeColumn);

    // then
    verify(changeColumn).apply(schema);
    verify(sqlDialect).alterTableChangeColumnStatements(nullable(Table.class), nullable(Column.class), nullable(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveColumn)}.
   */
  @Test
  public void testVisitRemoveColumn() {
    // given
    Column col = mock(Column.class);
    when(col.getName()).thenReturn("col");
    RemoveColumn removeColumn = mock(RemoveColumn.class);
    given(removeColumn.apply(schema)).willReturn(schema);
    when(removeColumn.getTableName()).thenReturn("SomeTable");
    when(removeColumn.getColumnDefinition()).thenReturn(col);

    // when
    upgrader.visit(removeColumn);

    // then
    verify(removeColumn).apply(schema);
    verify(sqlDialect).alterTableDropColumnStatements(nullable(Table.class), nullable(Column.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveIndex)}.
   */
  @Test
  public void testVisitRemoveIndex() {
    // given — physically present index
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("SomeIdx");

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIndex));
    when(schema.getTable("SomeTable")).thenReturn(mockTable);
    when(schema.tableExists("SomeTable")).thenReturn(true);

    RemoveIndex removeIndex = mock(RemoveIndex.class);
    given(removeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(removeIndex.getTableName()).thenReturn("SomeTable");
    when(removeIndex.getIndexToBeRemoved()).thenReturn(mockIndex);

    // when
    upgrader.visit(removeIndex);

    // then
    verify(sqlDialect).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(mockIndex));
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.ChangeIndex)}.
   */
  @Test
  public void testVisitChangeIndex() {
    // given — physically present index being changed
    Index fromIndex = mock(Index.class);
    when(fromIndex.getName()).thenReturn("SomeIndex");

    Index toIndex = mock(Index.class);
    when(toIndex.getName()).thenReturn("SomeIndex");
    when(toIndex.isDeferred()).thenReturn(false);
    when(toIndex.isUnique()).thenReturn(false);
    when(toIndex.columnNames()).thenReturn(List.of("col1"));

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(fromIndex));
    when(schema.getTable("SomeTable")).thenReturn(mockTable);
    when(schema.tableExists("SomeTable")).thenReturn(true);

    ChangeIndex changeIndex = mock(ChangeIndex.class);
    given(changeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    given(changeIndex.getTableName()).willReturn("SomeTable");
    given(changeIndex.getFromIndex()).willReturn(fromIndex);
    given(changeIndex.getToIndex()).willReturn(toIndex);

    // when
    upgrader.visit(changeIndex);

    // then
    verify(sqlDialect).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(fromIndex));
    verify(sqlDialect).addIndexStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(toIndex));
    verify(sqlStatementWriter, atLeast(2)).writeSql(anyCollection());
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
    when(sqlDialect.convertStatementToSQL(eq((Statement)insertStatement), nullable(Schema.class), nullable(Table.class))).thenCallRealMethod();

    // when
    upgrader.visit(executeStatement);

    // then
    ArgumentCaptor<SqlDialect.IdTable> captor = ArgumentCaptor.forClass(SqlDialect.IdTable.class);
    verify(sqlDialect).convertStatementToSQL(Mockito.eq(insertStatement), Mockito.eq(schema), captor.capture());
    assertEquals("Id Table name differed", ID_TABLE_NAME, captor.getValue().getName());
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    when(sqlDialect.convertStatementToSQL(eq((Statement)updateStatement), nullable(Schema.class), nullable(Table.class))).thenCallRealMethod();
    when(sqlDialect.convertStatementToSQL(eq(updateStatement))).thenReturn("dummy");

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(updateStatement);
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    when(sqlDialect.convertStatementToSQL(eq((Statement)deleteStatement), nullable(Schema.class), nullable(Table.class))).thenCallRealMethod();
    when(sqlDialect.convertStatementToSQL(eq(deleteStatement))).thenReturn("dummy");

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(deleteStatement);
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    when(sqlDialect.convertStatementToSQL(eq((Statement)mergeStatement), nullable(Schema.class), nullable(Table.class))).thenCallRealMethod();
    when(sqlDialect.convertStatementToSQL(eq(mergeStatement))).thenReturn("dummy");

    // when
    upgrader.visit(executeStatement);

    // then
    verify(sqlDialect).convertStatementToSQL(mergeStatement);
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    when(sqlDialect.convertStatementToSQL(eq(statement), nullable(Schema.class), nullable(Table.class))).thenCallRealMethod();

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
    verify(statement).getStatement(eq("Foo"), nullable(String.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  @Test
  public void testAnalyseTableStatment(){
    // given
    AnalyseTable analyseTable = mock(AnalyseTable.class);
    given(analyseTable.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(analyseTable);

    // then
    verify(sqlDialect).getSqlForAnalyseTable(nullable(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for
   * {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddSequence)}.
   */
  @Test
  public void testVisitAddSequence() {
    // given
    AddSequence addSequence = mock(AddSequence.class);
    given(addSequence.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(addSequence);

    // then
    verify(addSequence).apply(schema);
    verify(sqlDialect, atLeastOnce()).sequenceDeploymentStatements(nullable(Sequence.class));
    verify(sqlStatementWriter).writeSql(anyCollection()); // deploying the specified table and indexes
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.RemoveSequence)}.
   */
  @Test
  public void testVisitRemoveSequence() {
    // given
    RemoveSequence removeSequence = mock(RemoveSequence.class);
    given(removeSequence.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(removeSequence);

    // then
    verify(removeSequence).apply(schema);
    verify(sqlDialect).dropStatements(nullable(Sequence.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Tests that a deferred AddIndex emits an INSERT into DeployedIndexes
   * without emitting physical CREATE INDEX DDL.
   */
  @Test
  public void testVisitDeferredAddIndex() {
    // given -- a deferred index
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1", "col2"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);

    // when
    upgrader.visit(addIndex);

    // then -- INSERT into DeployedIndexes, no physical DDL
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
    verify(sqlDialect, never()).addIndexStatements(ArgumentMatchers.any(), ArgumentMatchers.any());
  }


  /** When the dialect does not support deferred, a deferred AddIndex falls back to immediate build. */
  @Test
  public void testVisitDeferredAddIndexFallsBackWhenDialectUnsupported() {
    // given — dialect does not support deferred
    when(sqlDialect.supportsDeferredIndexCreation()).thenReturn(false);

    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);

    Table mockTable = mock(Table.class);
    when(mockTable.getName()).thenReturn("TestTable");
    when(schema.getTable("TestTable")).thenReturn(mockTable);

    // when
    upgrader.visit(addIndex);

    // then — should fall back to addIndexStatements (immediate build)
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));
  }


  /**
   * Tests that ChangeIndex for a deferred index that is not physically present
   * emits DELETE + INSERT in DeployedIndexes without physical DROP INDEX DDL.
   */
  @Test
  public void testChangeIndexCancelsPendingDeferredAddAndAddsNewIndex() {
    // given — a tracked deferred index (not physically built) on TestTable/TestIdx
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — change the same index to a new definition
    Index toIndex = mock(Index.class);
    when(toIndex.getName()).thenReturn("TestIdx");
    when(toIndex.isUnique()).thenReturn(false);
    when(toIndex.isDeferred()).thenReturn(false);
    when(toIndex.columnNames()).thenReturn(List.of("col2"));

    Table mockTable = mock(Table.class);
    when(schema.getTable("TestTable")).thenReturn(mockTable);

    // Make the model show the index as not physically present
    when(mockTable.indexes()).thenReturn(List.of(mockIndex));

    ChangeIndex changeIndex = mock(ChangeIndex.class);
    given(changeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(changeIndex.getTableName()).thenReturn("TestTable");
    when(changeIndex.getFromIndex()).thenReturn(mockIndex);
    when(changeIndex.getToIndex()).thenReturn(toIndex);

    // when
    upgrader.visit(changeIndex);

    // then — no physical DROP INDEX (not built), but DeployedIndexes updated
    verify(sqlDialect, never()).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.any());
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that RenameIndex for a deferred index not physically built updates
   * only the DeployedIndexes table without emitting RENAME INDEX DDL.
   */
  @Test
  public void testRenameIndexUpdatesPendingDeferredAdd() {
    // given — a tracked deferred index (not physically built)
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — rename; model shows index as not physically present
    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIndex));
    when(schema.getTable("TestTable")).thenReturn(mockTable);
    when(schema.tableExists("TestTable")).thenReturn(true);

    RenameIndex renameIndex = mock(RenameIndex.class);
    given(renameIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(renameIndex.getTableName()).thenReturn("TestTable");
    when(renameIndex.getFromIndexName()).thenReturn("TestIdx");
    when(renameIndex.getToIndexName()).thenReturn("RenamedIdx");

    // when
    upgrader.visit(renameIndex);

    // then — no physical RENAME INDEX DDL (index not built)
    verify(sqlDialect, never()).renameIndexStatements(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that RemoveIndex for a deferred index not physically built emits
   * DELETE from DeployedIndexes without physical DROP INDEX DDL.
   */
  @Test
  public void testRemoveIndexCancelsPendingDeferredAdd() {
    // given — a tracked deferred index (not physically built)
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — model shows index as not physically present
    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIndex));
    when(schema.getTable("TestTable")).thenReturn(mockTable);
    when(schema.tableExists("TestTable")).thenReturn(true);

    RemoveIndex removeIndex = mock(RemoveIndex.class);
    given(removeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(removeIndex.getTableName()).thenReturn("TestTable");
    when(removeIndex.getIndexToBeRemoved()).thenReturn(mockIndex);

    // when
    upgrader.visit(removeIndex);

    // then — no physical DROP INDEX (index not built)
    verify(sqlDialect, never()).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.any());
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that RemoveIndex for a non-deferred, physically present index emits DROP INDEX DDL.
   */
  @Test
  public void testRemoveIndexDropsNonDeferredIndex() {
    // given — a non-deferred index that is physically present
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIndex));
    when(schema.getTable("TestTable")).thenReturn(mockTable);
    when(schema.tableExists("TestTable")).thenReturn(true);

    RemoveIndex removeIndex = mock(RemoveIndex.class);
    given(removeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(removeIndex.getTableName()).thenReturn("TestTable");
    when(removeIndex.getIndexToBeRemoved()).thenReturn(mockIndex);

    // when
    upgrader.visit(removeIndex);

    // then — physical DROP INDEX DDL emitted
    verify(sqlDialect).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(mockIndex));
  }


  /**
   * Tests that RemoveTable removes all tracked indexes for that table from DeployedIndexes.
   */
  @Test
  public void testRemoveTableCancelsPendingDeferredIndexes() {
    // given — a tracked deferred index on TestTable
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — remove the table
    Table mockTable = mock(Table.class);
    when(mockTable.getName()).thenReturn("TestTable");
    RemoveTable removeTable = mock(RemoveTable.class);
    given(removeTable.apply(ArgumentMatchers.any())).willReturn(schema);
    when(removeTable.getTable()).thenReturn(mockTable);

    // when
    upgrader.visit(removeTable);

    // then — DROP TABLE + DELETE from DeployedIndexes
    verify(sqlDialect).dropStatements(mockTable);
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that RemoveColumn removes tracked indexes referencing the column from DeployedIndexes.
   */
  @Test
  public void testRemoveColumnCancelsPendingDeferredIndexContainingColumn() {
    // given — a tracked deferred index referencing col1
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1", "col2"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — remove col1 from TestTable
    Column mockColumn = mock(Column.class);
    when(mockColumn.getName()).thenReturn("col1");
    Table mockTable = mock(Table.class);
    when(schema.getTable("TestTable")).thenReturn(mockTable);

    RemoveColumn removeColumn = mock(RemoveColumn.class);
    given(removeColumn.apply(ArgumentMatchers.any())).willReturn(schema);
    when(removeColumn.getTableName()).thenReturn("TestTable");
    when(removeColumn.getColumnDefinition()).thenReturn(mockColumn);

    // when
    upgrader.visit(removeColumn);

    // then — DELETE from DeployedIndexes + DROP COLUMN
    verify(sqlDialect).alterTableDropColumnStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(mockColumn));
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that RenameTable updates table name in DeployedIndexes for tracked indexes.
   */
  @Test
  public void testRenameTableUpdatesPendingDeferredIndexTableName() {
    // given — a tracked deferred index on OldTable
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("OldTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — rename OldTable to NewTable
    Table oldTable = mock(Table.class);
    Table newTable = mock(Table.class);
    when(schema.getTable("OldTable")).thenReturn(oldTable);
    when(schema.getTable("NewTable")).thenReturn(newTable);

    RenameTable renameTable = mock(RenameTable.class);
    given(renameTable.apply(ArgumentMatchers.any())).willReturn(schema);
    when(renameTable.getOldTableName()).thenReturn("OldTable");
    when(renameTable.getNewTableName()).thenReturn("NewTable");

    // when
    upgrader.visit(renameTable);

    // then — UPDATE in DeployedIndexes + RENAME TABLE DDL
    verify(sqlDialect).renameTableStatements(oldTable, newTable);
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Tests that ChangeColumn with a column rename updates column references in DeployedIndexes.
   */
  @Test
  public void testChangeColumnUpdatesPendingDeferredIndexColumnName() {
    // given — a tracked deferred index referencing "oldCol"
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.isDeferred()).thenReturn(true);
    when(mockIndex.columnNames()).thenReturn(List.of("oldCol"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn("TestTable");
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    upgrader.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, sqlStatementWriter);

    // given — rename column oldCol → newCol on TestTable
    Column fromColumn = mock(Column.class);
    when(fromColumn.getName()).thenReturn("oldCol");
    Column toColumn = mock(Column.class);
    when(toColumn.getName()).thenReturn("newCol");
    Table mockTable = mock(Table.class);
    when(schema.getTable("TestTable")).thenReturn(mockTable);

    ChangeColumn changeColumn = mock(ChangeColumn.class);
    given(changeColumn.apply(ArgumentMatchers.any())).willReturn(schema);
    when(changeColumn.getTableName()).thenReturn("TestTable");
    when(changeColumn.getFromColumn()).thenReturn(fromColumn);
    when(changeColumn.getToColumn()).thenReturn(toColumn);

    // when
    upgrader.visit(changeColumn);

    // then — UPDATE in DeployedIndexes + ALTER TABLE DDL
    verify(sqlDialect).alterTableChangeColumnStatements(ArgumentMatchers.any(), ArgumentMatchers.eq(fromColumn), ArgumentMatchers.eq(toColumn));
    verify(sqlStatementWriter, atLeast(1)).writeSql(anyCollection());
  }


  /**
   * Slim invariant + dialect-support normalization: on a dialect without
   * deferred-index-creation support, a declared-deferred AddIndex is
   * normalized to immediate (CREATE INDEX runs now) AND — because the slim
   * model only tracks deferred indexes — produces NO DeployedIndexes INSERT
   * at all. The app-side executor therefore cannot double-CREATE.
   */
  @Test
  public void testVisitAddIndexDeferredOnDialectWithoutDeferredSupport() {
    // given — dialect does not support deferred index creation
    when(sqlDialect.supportsDeferredIndexCreation()).thenReturn(false);

    Index declared = mock(Index.class);
    when(declared.getName()).thenReturn("TestIdx");
    when(declared.isDeferred()).thenReturn(true);
    when(declared.isUnique()).thenReturn(false);
    when(declared.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex.getNewIndex()).thenReturn(declared);

    Table newTable = mock(Table.class);
    when(newTable.getName()).thenReturn(ID_TABLE_NAME);
    when(schema.getTable(ID_TABLE_NAME)).thenReturn(newTable);

    // when
    upgrader.visit(addIndex);

    // then — physical CREATE INDEX emitted (declared-deferred promoted-to-immediate)
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));

    // and — NO tracking INSERT (slim: non-deferred is not tracked)
    verify(sqlDialect, never()).convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.InsertStatement.class));
  }


  /**
   * Slim invariant + dialect-support normalization: ChangeIndex from
   * immediate to declared-deferred on a dialect without deferred support
   * emits physical DROP + CREATE (the to-index normalizes to immediate) AND
   * produces no DeployedIndexes INSERT for the new row. No DELETE either,
   * since the from-index wasn't tracked in the first place.
   */
  @Test
  public void testVisitChangeIndexToDeferredOnDialectWithoutDeferredSupport() {
    // given — dialect does not support deferred index creation
    when(sqlDialect.supportsDeferredIndexCreation()).thenReturn(false);

    Index fromIndex = mock(Index.class);
    when(fromIndex.getName()).thenReturn("TestIdx");
    when(fromIndex.isDeferred()).thenReturn(false);
    when(fromIndex.isUnique()).thenReturn(false);
    when(fromIndex.columnNames()).thenReturn(List.of("col1"));

    Index toIndex = mock(Index.class);
    when(toIndex.getName()).thenReturn("TestIdx");
    when(toIndex.isDeferred()).thenReturn(true);  // declared deferred
    when(toIndex.isUnique()).thenReturn(false);
    when(toIndex.columnNames()).thenReturn(List.of("col2"));

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(fromIndex));
    when(schema.getTable("TestTable")).thenReturn(mockTable);
    when(schema.tableExists("TestTable")).thenReturn(true);

    ChangeIndex changeIndex = mock(ChangeIndex.class);
    given(changeIndex.apply(ArgumentMatchers.any())).willReturn(schema);
    when(changeIndex.getTableName()).thenReturn("TestTable");
    when(changeIndex.getFromIndex()).thenReturn(fromIndex);
    when(changeIndex.getToIndex()).thenReturn(toIndex);

    // when
    upgrader.visit(changeIndex);

    // then — both DROP and CREATE are emitted (declared-deferred promoted-to-immediate)
    verify(sqlDialect).indexDropStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));

    // and — NO tracking INSERT (slim: non-deferred is not tracked)
    verify(sqlDialect, never()).convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.InsertStatement.class));
  }
}
