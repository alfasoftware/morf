package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeSchemaChangeVisitor.GraphBasedUpgradeSchemaChangeVisitorFactory;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexState;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests of {@link GraphBasedUpgradeSchemaChangeVisitor}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeSchemaChangeVisitor {

  private GraphBasedUpgradeSchemaChangeVisitor visitor;

  @Mock
  private Schema sourceSchema;

  @Mock
  private SqlDialect sqlDialect;

  @Mock
  private Table idTable;

  @Mock
  private GraphBasedUpgradeNode n1, n2;

  UpgradeConfigAndContext upgradeConfigAndContext;

  private final static List<String> STATEMENTS = Lists.newArrayList("a", "b");

  private Map<String, GraphBasedUpgradeNode> nodes;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(n1.getName()).thenReturn(U1.class.getName());
    when(n2.getName()).thenReturn(U2.class.getName());

    nodes = new HashMap<>();
    nodes.put(U1.class.getName(), n1);
    nodes.put(U2.class.getName(), n2);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
    upgradeConfigAndContext.setDeferredIndexCreationEnabled(true);
    when(sqlDialect.supportsDeferredIndexCreation()).thenReturn(true);
    // Default: allow DeployedIndexes DML to be converted without error
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.InsertStatement.class))).thenReturn(List.of("INSERT INTO DeployedIndexes ..."));
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.UpdateStatement.class))).thenReturn("UPDATE DeployedIndexes ...");
    when(sqlDialect.convertStatementToSQL(ArgumentMatchers.any(org.alfasoftware.morf.sql.DeleteStatement.class))).thenReturn("DELETE FROM DeployedIndexes ...");
    visitor = new GraphBasedUpgradeSchemaChangeVisitor(sourceSchema, upgradeConfigAndContext, sqlDialect, idTable, DeployedIndexState.empty(),
        new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesServiceImpl(
            new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesStatementFactoryImpl()),
        nodes);
  }


  @Test
  public void testAddTableVisit() {
    // given
    visitor.startStep(U1.class);
    AddTable addTable = mock(AddTable.class);
    when(addTable.getTable()).thenReturn(mock(Table.class));
    when(sqlDialect.tableDeploymentStatements(any(Table.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(addTable);

    // then
    verify(addTable).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRemoveTableVisit() {
    // given
    visitor.startStep(U1.class);
    RemoveTable removeTable = mock(RemoveTable.class);
    Table mockTable = mock(Table.class);
    when(mockTable.getName()).thenReturn("SomeTable");
    when(removeTable.getTable()).thenReturn(mockTable);
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(removeTable);

    // then
    verify(removeTable).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testAddIndexVisit() {
    // given
    visitor.startStep(U1.class);
    String idTableName = "IdTableName";
    Index mockIndex = mock(Index.class);
    when(mockIndex.getName()).thenReturn("TestIdx");
    when(mockIndex.isDeferred()).thenReturn(false);
    when(mockIndex.isUnique()).thenReturn(false);
    when(mockIndex.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    when(addIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(addIndex.getTableName()).thenReturn(idTableName);
    when(addIndex.getNewIndex()).thenReturn(mockIndex);
    when(sqlDialect.addIndexStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(addIndex);

    // then
    verify(addIndex).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  /**
   * Test method for {@link org.alfasoftware.morf.upgrade.InlineTableUpgrader#visit(org.alfasoftware.morf.upgrade.AddIndex)}.
   */
  @Test
  public void testVisitAddIndexWithPRFIndex() {
    // given
    visitor.startStep(U1.class);

    String idTableName = "IdTableName";
    Index newIndex = mock(Index.class);
    when(newIndex.getName()).thenReturn(idTableName + "_1");
    when(newIndex.columnNames()).thenReturn(Collections.singletonList("column_1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(sourceSchema)).willReturn(sourceSchema);
    when(addIndex.getTableName()).thenReturn(idTableName);
    when(addIndex.getNewIndex()).thenReturn(newIndex);

    Index indexPrf = mock(Index.class);
    when(indexPrf.getName()).thenReturn(idTableName + "_PRF1");
    when(indexPrf.columnNames()).thenReturn(List.of("column_1"));

    Index indexPrf1 = mock(Index.class);
    when(indexPrf1.getName()).thenReturn(idTableName + "_PRF2");
    when(indexPrf1.columnNames()).thenReturn(List.of("column_2"));

    Table newTable = mock(Table.class);
    when(newTable.getName()).thenReturn(idTableName);
    when(sourceSchema.getTable(idTableName)).thenReturn(newTable);
    Map<String, List<Index>> ignoredIndexes = Maps.newHashMap();
    ignoredIndexes.put(idTableName.toUpperCase(), Lists.newArrayList(indexPrf, indexPrf1));
    upgradeConfigAndContext.setIgnoredIndexes(ignoredIndexes);

    when(sqlDialect.renameIndexStatements(nullable(Table.class), eq(idTableName + "_PRF1"), eq(idTableName + "_1"))).thenReturn(STATEMENTS);
    when(sqlDialect.renameIndexStatements(nullable(Table.class), eq(idTableName + "_PRF2"), eq(idTableName + "_2"))).thenReturn(STATEMENTS);
    when(sqlDialect.addIndexStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    Index newIndex1 = mock(Index.class);
    when(newIndex1.getName()).thenReturn(idTableName + "_2");
    when(newIndex1.columnNames()).thenReturn(Collections.singletonList("column_2"));
    AddIndex addIndex1 = mock(AddIndex.class);
    given(addIndex1.apply(sourceSchema)).willReturn(sourceSchema);
    when(addIndex1.getTableName()).thenReturn(idTableName);
    when(addIndex1.getNewIndex()).thenReturn(newIndex1);

    Index newIndex2 = mock(Index.class);
    when(newIndex2.getName()).thenReturn(idTableName + "_3");
    when(newIndex2.columnNames()).thenReturn(Collections.singletonList("column_3"));
    AddIndex addIndex2 = mock(AddIndex.class);
    given(addIndex2.apply(sourceSchema)).willReturn(sourceSchema);
    when(addIndex2.getTableName()).thenReturn(idTableName);
    when(addIndex2.getNewIndex()).thenReturn(newIndex2);

    // when
    visitor.visit(addIndex);
    visitor.visit(addIndex1);
    visitor.visit(addIndex2);

    // then
    verify(addIndex).apply(sourceSchema);
    verify(sqlDialect).renameIndexStatements(nullable(Table.class), eq(idTableName + "_PRF1"), eq(idTableName + "_1"));
    verify(sqlDialect).renameIndexStatements(nullable(Table.class), eq(idTableName + "_PRF2"), eq(idTableName + "_2"));
    verify(sqlDialect).addIndexStatements(nullable(Table.class), nullable(Index.class));
    verify(n1, times(3)).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testAddColumnVisit() {
    // given
    visitor.startStep(U1.class);
    AddColumn addColumn = mock(AddColumn.class);
    when(addColumn.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.alterTableAddColumnStatements(nullable(Table.class), nullable(Column.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(addColumn);

    // then
    verify(addColumn).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testChangeColumnVisit() {
    // given
    visitor.startStep(U1.class);
    Column fromCol = mock(Column.class);
    when(fromCol.getName()).thenReturn("col");
    Column toCol = mock(Column.class);
    when(toCol.getName()).thenReturn("col");
    ChangeColumn changeColumn = mock(ChangeColumn.class);
    when(changeColumn.apply(sourceSchema)).thenReturn(sourceSchema);
    when(changeColumn.getTableName()).thenReturn("SomeTable");
    when(changeColumn.getFromColumn()).thenReturn(fromCol);
    when(changeColumn.getToColumn()).thenReturn(toCol);
    when(sqlDialect.alterTableChangeColumnStatements(nullable(Table.class), nullable(Column.class), nullable(Column.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(changeColumn);

    // then
    verify(changeColumn).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRemoveColumnVisit() {
    // given
    visitor.startStep(U1.class);
    Column col = mock(Column.class);
    when(col.getName()).thenReturn("col");
    RemoveColumn removeColumn = mock(RemoveColumn.class);
    when(removeColumn.apply(sourceSchema)).thenReturn(sourceSchema);
    when(removeColumn.getTableName()).thenReturn("SomeTable");
    when(removeColumn.getColumnDefinition()).thenReturn(col);
    when(sqlDialect.alterTableDropColumnStatements(nullable(Table.class), nullable(Column.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(removeColumn);

    // then
    verify(removeColumn).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRemoveIndexVisit() {
    // given — physically present index
    visitor.startStep(U1.class);
    Index mockIdx = mock(Index.class);
    when(mockIdx.getName()).thenReturn("SomeIdx");

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    RemoveIndex removeIndex = mock(RemoveIndex.class);
    when(removeIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(removeIndex.getTableName()).thenReturn("SomeTable");
    when(removeIndex.getIndexToBeRemoved()).thenReturn(mockIdx);
    when(sqlDialect.indexDropStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(removeIndex);

    // then
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  /**
   * Regression test: before P1.1, GraphBasedUpgradeSchemaChangeVisitor was
   * constructed via its 4-arg super constructor, silently substituting
   * DeployedIndexState.empty(). As a result the visitor emitted DROP INDEX
   * DDL even for unbuilt deferred indexes (state = ABSENT) because the
   * defaulted-empty state returned UNKNOWN, which is interpreted as "present".
   * This test confirms that a non-empty DeployedIndexState threaded through
   * to the graph-based visitor is actually consulted: when state says ABSENT,
   * DROP INDEX DDL must not be emitted.
   */
  @Test
  public void testRemoveIndexVisitRespectsAbsentStateForGraphBasedPath() {
    // given — enricher reports SomeIdx as ABSENT (unbuilt deferred index)
    DeployedIndexState absentState = DeployedIndexState.of("SomeTable", "SomeIdx", org.alfasoftware.morf.upgrade.deployedindexes.IndexPresence.ABSENT);
    GraphBasedUpgradeSchemaChangeVisitor visitorWithAbsentState =
        new GraphBasedUpgradeSchemaChangeVisitor(sourceSchema, upgradeConfigAndContext, sqlDialect, idTable, absentState,
            new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesServiceImpl(
                new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesStatementFactoryImpl()),
            nodes);
    visitorWithAbsentState.startStep(U1.class);

    Index mockIdx = mock(Index.class);
    when(mockIdx.getName()).thenReturn("SomeIdx");

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    RemoveIndex removeIndex = mock(RemoveIndex.class);
    when(removeIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(removeIndex.getTableName()).thenReturn("SomeTable");
    when(removeIndex.getIndexToBeRemoved()).thenReturn(mockIdx);
    when(sqlDialect.indexDropStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    // when
    visitorWithAbsentState.visit(removeIndex);

    // then — no DROP INDEX DDL emitted (state was ABSENT)
    verify(n1, never()).addAllUpgradeStatements(ArgumentMatchers.argThat(c -> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testChangeIndexVisit() {
    // given — physically present index
    visitor.startStep(U1.class);
    Index fromIdx = mock(Index.class);
    when(fromIdx.getName()).thenReturn("SomeIndex");

    Index toIdx = mock(Index.class);
    when(toIdx.getName()).thenReturn("SomeIndex");
    when(toIdx.isDeferred()).thenReturn(false);
    when(toIdx.isUnique()).thenReturn(false);
    when(toIdx.columnNames()).thenReturn(List.of("col1"));

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(fromIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    ChangeIndex changeIndex = mock(ChangeIndex.class);
    when(changeIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(changeIndex.getTableName()).thenReturn("SomeTable");
    when(changeIndex.getFromIndex()).thenReturn(fromIdx);
    when(changeIndex.getToIndex()).thenReturn(toIdx);
    when(sqlDialect.indexDropStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);
    when(sqlDialect.addIndexStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(changeIndex);

    // then
    verify(n1, atLeast(2)).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> ((java.util.Collection<?>)c).containsAll(STATEMENTS)));
  }


  @Test
  public void testRenameIndexVisit() {
    // given — physically present index
    visitor.startStep(U1.class);
    Index mockIdx = mock(Index.class);
    when(mockIdx.getName()).thenReturn("OldIndex");

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(mockIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    RenameIndex renameIndex = mock(RenameIndex.class);
    when(renameIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(renameIndex.getTableName()).thenReturn("SomeTable");
    when(renameIndex.getFromIndexName()).thenReturn("OldIndex");
    when(renameIndex.getToIndexName()).thenReturn("NewIndex");
    when(sqlDialect.renameIndexStatements(nullable(Table.class), nullable(String.class), nullable(String.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(renameIndex);

    // then
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  /**
   * ChangeIndex for a deferred index not physically present should not call
   * indexDropStatements (nothing to drop).
   */
  @Test
  public void testChangeIndexCancelsPendingDeferredAdd() {
    // given — a tracked deferred index (not physically built)
    visitor.startStep(U1.class);
    Index deferredIdx = mock(Index.class);
    when(deferredIdx.getName()).thenReturn("SomeIndex");
    when(deferredIdx.isUnique()).thenReturn(false);
    when(deferredIdx.isDeferred()).thenReturn(true);
    when(deferredIdx.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    when(addIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(addIndex.getTableName()).thenReturn("SomeTable");
    when(addIndex.getNewIndex()).thenReturn(deferredIdx);
    visitor.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, n1);

    // given — change the same index
    Index toIdx = mock(Index.class);
    when(toIdx.getName()).thenReturn("SomeIndex");
    when(toIdx.isUnique()).thenReturn(false);
    when(toIdx.isDeferred()).thenReturn(false);
    when(toIdx.columnNames()).thenReturn(List.of("col2"));

    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(deferredIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    ChangeIndex changeIndex = mock(ChangeIndex.class);
    when(changeIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(changeIndex.getTableName()).thenReturn("SomeTable");
    when(changeIndex.getFromIndex()).thenReturn(deferredIdx);
    when(changeIndex.getToIndex()).thenReturn(toIdx);

    // when
    visitor.visit(changeIndex);

    // then — no physical DROP INDEX (not built)
    verify(sqlDialect, never()).indexDropStatements(ArgumentMatchers.any(), ArgumentMatchers.any());
  }


  /**
   * RenameIndex for a deferred index not physically present should not call
   * renameIndexStatements (nothing to rename physically).
   */
  @Test
  public void testRenameIndexUpdatesPendingDeferredAdd() {
    // given — a tracked deferred index (not physically built)
    visitor.startStep(U1.class);
    Index deferredIdx = mock(Index.class);
    when(deferredIdx.getName()).thenReturn("OldIndex");
    when(deferredIdx.isUnique()).thenReturn(false);
    when(deferredIdx.isDeferred()).thenReturn(true);
    when(deferredIdx.columnNames()).thenReturn(List.of("col1"));

    AddIndex addIndex = mock(AddIndex.class);
    when(addIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(addIndex.getTableName()).thenReturn("SomeTable");
    when(addIndex.getNewIndex()).thenReturn(deferredIdx);
    visitor.visit(addIndex);
    Mockito.clearInvocations(sqlDialect, n1);

    // given — model shows index as not physically present
    Table mockTable = mock(Table.class);
    when(mockTable.indexes()).thenReturn(List.of(deferredIdx));
    when(sourceSchema.getTable("SomeTable")).thenReturn(mockTable);
    when(sourceSchema.tableExists("SomeTable")).thenReturn(true);

    RenameIndex renameIndex = mock(RenameIndex.class);
    when(renameIndex.apply(ArgumentMatchers.any())).thenReturn(sourceSchema);
    when(renameIndex.getTableName()).thenReturn("SomeTable");
    when(renameIndex.getFromIndexName()).thenReturn("OldIndex");
    when(renameIndex.getToIndexName()).thenReturn("NewIndex");

    // when
    visitor.visit(renameIndex);

    // then — no physical RENAME INDEX DDL
    verify(sqlDialect, never()).renameIndexStatements(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
  }


  @Test
  public void testExecuteStatementVisit() {
    // given
    visitor.startStep(U1.class);
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    Statement statement = mock(Statement.class);
    when(executeStatement.getStatement()).thenReturn(statement);
    when(sqlDialect.convertStatementToSQL(statement, sourceSchema, idTable)).thenReturn(STATEMENTS);

    // when
    visitor.visit(executeStatement);

    // then
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testExecutePortableSqlStatementVisit() {
    // given
    visitor.startStep(U1.class);
    ExecuteStatement executeStatement = mock(ExecuteStatement.class);
    PortableSqlStatement statement = mock(PortableSqlStatement.class);
    when(executeStatement.getStatement()).thenReturn(statement);
    when(sqlDialect.getDatabaseType()).thenReturn(mock(DatabaseType.class));
    when(statement.getStatement(sqlDialect.getDatabaseType().identifier(), sqlDialect.schemaNamePrefix())).thenReturn("a");

    // when
    visitor.visit(executeStatement);

    // then
    verify(statement).inplaceUpdateTransitionalTableNames(nullable(TableNameResolver.class));
    verify(n1).addUpgradeStatements("a");
  }


  @Test
  public void testRenameTableVisit() {
    // given
    visitor.startStep(U1.class);
    RenameTable renameTable = mock(RenameTable.class);
    when(renameTable.apply(sourceSchema)).thenReturn(sourceSchema);
    when(renameTable.getOldTableName()).thenReturn("OldTable");
    when(renameTable.getNewTableName()).thenReturn("NewTable");
    when(sqlDialect.renameTableStatements(nullable(Table.class), nullable(Table.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(renameTable);

    // then
    verify(renameTable).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testChangePrimaryKeyColumnsVisit() {
    // given
    visitor.startStep(U1.class);
    ChangePrimaryKeyColumns changePrimaryKeyColumns = mock(ChangePrimaryKeyColumns.class);
    when(changePrimaryKeyColumns.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.changePrimaryKeyColumns(nullable(Table.class), anyList(), anyList())).thenReturn(STATEMENTS);

    // when
    visitor.visit(changePrimaryKeyColumns);

    // then
    verify(changePrimaryKeyColumns).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testAddAuditRecord() {
    // given
    visitor.startStep(U1.class);
    when(sourceSchema.tableExists(any(String.class))).thenReturn(true);
    when(sqlDialect.convertStatementToSQL(any(Statement.class), eq(sourceSchema), eq(idTable))).thenReturn(STATEMENTS);

    // when
    visitor.addAuditRecord(new UUID(1, 1), "xxx");

    // then
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testStartStep() {
    // given
    visitor.startStep(U1.class);

    // when
    visitor.startStep(U2.class);

    // then
    assertEquals(n2, visitor.currentNode);
  }


  @Test(expected = IllegalStateException.class)
  public void testStartStepException() {
    // given
    visitor.startStep(U1.class);

    // when
    visitor.startStep(U3.class);

    // then exception
  }


  @Test
  public void testAddTableFromVisit() {
    // given
    visitor.startStep(U1.class);
    AddTableFrom addTableFrom = mock(AddTableFrom.class);
    when(addTableFrom.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.addTableFromStatements(nullable(Table.class), nullable(SelectStatement.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(addTableFrom);

    // then
    verify(addTableFrom).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testAnalyseTableVisit() {
    // given
    visitor.startStep(U1.class);
    AnalyseTable analyseTable = mock(AnalyseTable.class);
    when(analyseTable.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.getSqlForAnalyseTable(nullable(Table.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(analyseTable);

    // then
    verify(analyseTable).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testAddSequence() {
    // given
    visitor.startStep(U1.class);
    AddSequence addSequence = mock(AddSequence.class);
    when(addSequence.apply(sourceSchema)).thenReturn(sourceSchema);
    when(addSequence.getSequence()).thenReturn(mock(Sequence.class));
    when(sqlDialect.sequenceDeploymentStatements(any(Sequence.class))).thenReturn(STATEMENTS);

    //When
    visitor.visit(addSequence);

    //then
    verify(addSequence).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c -> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRemoveSequence() {
    // given
    visitor.startStep(U1.class);
    RemoveSequence removeSequence = mock(RemoveSequence.class);
    when(removeSequence.apply(sourceSchema)).thenReturn(sourceSchema);
    when(removeSequence.getSequence()).thenReturn(mock(Sequence.class));
    when(sqlDialect.dropStatements(any(Sequence.class))).thenReturn(STATEMENTS);

    //When
    visitor.visit(removeSequence);

    //then
    verify(removeSequence).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c -> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testFactory() {
    // given
    GraphBasedUpgradeSchemaChangeVisitorFactory factory = new GraphBasedUpgradeSchemaChangeVisitorFactory();

    // when
    GraphBasedUpgradeSchemaChangeVisitor created = factory.create(sourceSchema, upgradeConfigAndContext, sqlDialect, idTable, DeployedIndexState.empty(),
        new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesServiceImpl(
            new org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesStatementFactoryImpl()),
        nodes);

    // then
    assertNotNull(created);
  }

  /**
   * Test UpgradeStep
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  class U1 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      // nothing
    }
  }

  /**
   * Test UpgradeStep
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  class U2 extends U1 {}

  /**
   * Test UpgradeStep
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  class U3 extends U1 {}

}
