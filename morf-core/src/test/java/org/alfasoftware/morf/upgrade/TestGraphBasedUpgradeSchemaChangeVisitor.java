package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.DatabaseType;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeSchemaChangeVisitor.GraphBasedUpgradeSchemaChangeVisitorFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

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
  private SchemaResource schemaResource;

  @Mock
  private Table idTable;

  @Mock
  private GraphBasedUpgradeNode n1, n2;

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

    visitor = new GraphBasedUpgradeSchemaChangeVisitor(sourceSchema, schemaResource, sqlDialect, idTable, nodes);
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
    when(removeTable.getTable()).thenReturn(mock(Table.class));
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
    AddIndex addIndex = mock(AddIndex.class);
    when(addIndex.apply(sourceSchema)).thenReturn(sourceSchema);
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

    String ID_TABLE_NAME = "IdTableName";
    Index newIndex = mock(Index.class);
    when(newIndex.getName()).thenReturn(ID_TABLE_NAME + "_1");
    when(newIndex.columnNames()).thenReturn(Collections.singletonList("column_1"));

    AddIndex addIndex = mock(AddIndex.class);
    given(addIndex.apply(sourceSchema)).willReturn(sourceSchema);
    when(addIndex.getTableName()).thenReturn(ID_TABLE_NAME);
    when(addIndex.getNewIndex()).thenReturn(newIndex);

    AdditionalMetadata additionalMetadata = mock(AdditionalMetadata.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(additionalMetadata));
    Index indexPrf = mock(Index.class);
    when(indexPrf.getName()).thenReturn(ID_TABLE_NAME + "_PRF1");
    when(indexPrf.columnNames()).thenReturn(List.of("column_1"));

    Map<String, List<Index>> ignoredIndexes = new HashMap<>();
    ignoredIndexes.put(ID_TABLE_NAME.toUpperCase(Locale.ROOT), Collections.singletonList(indexPrf));
    when(additionalMetadata.ignoredIndexes()).thenReturn(ignoredIndexes);

    when(sqlDialect.renameIndexStatements(nullable(Table.class), eq(ID_TABLE_NAME + "_PRF1"), eq(ID_TABLE_NAME + "_1"))).thenReturn(STATEMENTS);

    // when
    visitor.visit(addIndex);

    // then
    verify(addIndex).apply(sourceSchema);
    verify(sqlDialect).renameIndexStatements(nullable(Table.class), eq(ID_TABLE_NAME + "_PRF1"), eq(ID_TABLE_NAME + "_1"));
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
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
    ChangeColumn changeColumn = mock(ChangeColumn.class);
    when(changeColumn.apply(sourceSchema)).thenReturn(sourceSchema);
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
    RemoveColumn removeColumn = mock(RemoveColumn.class);
    when(removeColumn.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.alterTableDropColumnStatements(nullable(Table.class), nullable(Column.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(removeColumn);

    // then
    verify(removeColumn).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRemoveIndexVisit() {
    // given
    visitor.startStep(U1.class);
    RemoveIndex removeIndex = mock(RemoveIndex.class);
    when(removeIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.indexDropStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(removeIndex);

    // then
    verify(removeIndex).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testChangeIndexVisit() {
    // given
    visitor.startStep(U1.class);
    ChangeIndex changeIndex = mock(ChangeIndex.class);
    when(changeIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.indexDropStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);
    when(sqlDialect.addIndexStatements(nullable(Table.class), nullable(Index.class))).thenReturn(STATEMENTS);


    // when
    visitor.visit(changeIndex);

    // then
    verify(changeIndex).apply(sourceSchema);
    verify(n1, times(2)).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
  }


  @Test
  public void testRenameIndexVisit() {
    // given
    visitor.startStep(U1.class);
    RenameIndex renameIndex = mock(RenameIndex.class);
    when(renameIndex.apply(sourceSchema)).thenReturn(sourceSchema);
    when(sqlDialect.renameIndexStatements(nullable(Table.class), nullable(String.class), nullable(String.class))).thenReturn(STATEMENTS);

    // when
    visitor.visit(renameIndex);

    // then
    verify(renameIndex).apply(sourceSchema);
    verify(n1).addAllUpgradeStatements(ArgumentMatchers.argThat(c-> c.containsAll(STATEMENTS)));
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
    GraphBasedUpgradeSchemaChangeVisitor created = factory.create(sourceSchema, schemaResource, sqlDialect, idTable, nodes);

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
