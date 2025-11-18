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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
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

    upgrader = new InlineTableUpgrader(schema, upgradeConfigAndContext, sqlDialect, sqlStatementWriter, SqlDialect.IdTable.withDeterministicName(ID_TABLE_NAME));
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
    verify(sqlDialect, atLeastOnce()).tableDeploymentStatements(nullable(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollection()); // deploying the specified table and indexes
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
    verify(sqlDialect).dropStatements(nullable(Table.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddIndex)}.
   */
  @Test
  public void testVisitAddIndex() {
    // given
    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(schema)).willReturn(schema);
    when(addIndex.getTableName()).thenReturn(ID_TABLE_NAME);

    Table newTable = mock(Table.class);
    when(newTable.getName()).thenReturn(ID_TABLE_NAME);
    when(schema.getTable(ID_TABLE_NAME)).thenReturn(newTable);

    // when
    upgrader.visit(addIndex);

    // then
    verify(addIndex).apply(schema);
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    AddIndex addIndex1 = mock(AddIndex.class);
    given(addIndex1.apply(schema)).willReturn(schema);
    when(addIndex1.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex1.getNewIndex()).thenReturn(newIndex1);

    Index newIndex2 = mock(Index.class);
    when(newIndex2.getName()).thenReturn(ID_TABLE_NAME + "_3");
    when(newIndex2.columnNames()).thenReturn(Collections.singletonList("column_4"));
    when(newIndex2.isUnique()).thenReturn(true);
    AddIndex addIndex2 = mock(AddIndex.class);
    given(addIndex2.apply(schema)).willReturn(schema);
    when(addIndex2.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex2.getNewIndex()).thenReturn(newIndex2);

    Index newIndex3 = mock(Index.class);
    when(newIndex3.getName()).thenReturn(ID_TABLE_NAME + "_4");
    when(newIndex3.columnNames()).thenReturn(Collections.singletonList("column_3"));
    // index 3 is unique idTable_PRF3 has same columns but isn't unique
    when(newIndex3.isUnique()).thenReturn(true);
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
    verify(sqlStatementWriter, times(4)).writeSql(anyCollection());
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
    ChangeColumn changeColumn = mock(ChangeColumn.class);
    given(changeColumn.apply(schema)).willReturn(schema);

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
    RemoveColumn removeColumn = mock(RemoveColumn.class);
    given(removeColumn.apply(schema)).willReturn(schema);

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
    // given
    RemoveIndex removeIndex = mock(RemoveIndex.class);
    given(removeIndex.apply(schema)).willReturn(schema);

    // when
    upgrader.visit(removeIndex);

    // then
    verify(removeIndex).apply(schema);
    verify(sqlDialect).indexDropStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlStatementWriter).writeSql(anyCollection());
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
    verify(sqlDialect).indexDropStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));
    verify(sqlStatementWriter, times(2)).writeSql(anyCollection()); // index drop and index deployment
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

}
