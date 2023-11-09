package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
import static org.alfasoftware.morf.sql.SqlUtils.truncate;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Join;
import org.alfasoftware.morf.sql.element.JoinType;
import org.alfasoftware.morf.sql.element.TableReference;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * All tests of {@link UpgradeTableResolutionVisitor}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestUpgradeTableResolutionVisitor {

  @Mock
  private Column col, col2;

  @Mock
  private AliasedField field;

  @Mock
  private Table table;

  @Mock
  private Index index;

  @Mock
  private Criterion crit;

  @Mock
  private SelectStatement select;

  private UpgradeTableResolutionVisitor visitor;

  private TableReference tableRef, tableRef2;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    visitor = new UpgradeTableResolutionVisitor();

    tableRef = new TableReference("tableRef");
    tableRef2 = new TableReference("tableRef2");

    when(col.isNullable()).thenReturn(true);
    when(col.getType()).thenReturn(DataType.STRING);
    when(col2.isNullable()).thenReturn(true);
    when(col2.getType()).thenReturn(DataType.STRING);

    when(table.getName()).thenReturn("t3");

    when(field.build()).thenReturn(field);
  }


  @Test
  public void testDeleteHandling() {
    // given
    DeleteStatement deleteStatement = new DeleteStatement(tableRef);

    // when
    visitor.visit(deleteStatement);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testInsertHandling() {
    // given
    InsertStatement insertStatement = InsertStatement.insert().into(tableRef).from(tableRef2).build();

    // when
    visitor.visit(insertStatement);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
    assertThat(visitor.getResolvedTables().getReadTables(),
      Matchers.containsInAnyOrder("TABLEREF2"));
  }


  @Test
  public void testMergeHandling() {
    // given
    MergeStatement mergeStatement = MergeStatement.merge().from(select).into(tableRef).build();

    // when
    visitor.visit(mergeStatement);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testPortableSqlStatementHandling() {
    // given
    PortableSqlStatement statement = new PortableSqlStatement();

    // when
    visitor.visit(statement);

    // then
    assertTrue(visitor.getResolvedTables().isPortableSqlStatementUsed());
  }


  @Test
  public void testSelectFirstStatementHandling() {
    // given
    SelectFirstStatement sel1 = selectFirst(field)
        .from(tableRef);

    // when
    visitor.visit(sel1);

    // then
    assertThat(visitor.getResolvedTables().getReadTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testSelectStatementHandling() {
    // given
    SelectStatement sel1 = select()
        .from(tableRef);

    // when
    visitor.visit(sel1);

    // then
    assertThat(visitor.getResolvedTables().getReadTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testTruncateStatementHandling() {
    // given
    TruncateStatement trun = truncate(tableRef);

    // when
    visitor.visit(trun);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testUpdateStatementHandling() {
    // given
    UpdateStatement up1 = update(tableRef)
        .set(field)
        .where(crit);

    // when
    visitor.visit(up1);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testFieldReferenceHandling() {
    // given
    FieldReference onTest = new FieldReference(tableRef, "x");

    // when
    visitor.visit(onTest);

    // then
    assertThat(visitor.getResolvedTables().getReadTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testJoinHandling() {
    // given
    Join join = new Join(JoinType.INNER_JOIN, tableRef, crit);

    // when
    visitor.visit(join);

    // then
    assertThat(visitor.getResolvedTables().getReadTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testAddColumnHandling() {
    // given
    AddColumn add1 = new AddColumn("tableref", col);

    // when
    visitor.visit(add1);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("TABLEREF"));
  }


  @Test
  public void testAddTableHandling() {
    // given
    AddTable add1 = new AddTable(table);

    // when
    visitor.visit(add1);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testAddTableFromHandling() {
    // given
    AddTableFrom add1 = new AddTableFrom(table, select);

    // when
    visitor.visit(add1);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testAddIndexHandling() {
    // given
    AddIndex add1 = new AddIndex("t3", index);

    // when
    visitor.visit(add1);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testRemoveTableHandling() {
    // given
    RemoveTable remove = new RemoveTable(table);

    // when
    visitor.visit(remove);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testRemoveColumnHandling() {
    // given
    RemoveColumn remove = new RemoveColumn("t3", col);

    // when
    visitor.visit(remove);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testRemoveIndexHandling() {
    // given
    RemoveIndex remove = new RemoveIndex("t3", index);

    // when
    visitor.visit(remove);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testChangeColumnHandling() {
    // given
    ChangeColumn change = new ChangeColumn("t3", col, col2);

    // when
    visitor.visit(change);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testChangeIndexHandling() {
    // given
    ChangeIndex change = new ChangeIndex("t3", index, index);

    // when
    visitor.visit(change);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testChangePrimaryKeyColumnsHandling() {
    // given
    ChangePrimaryKeyColumns change = new ChangePrimaryKeyColumns("t3", new ArrayList<>(), new ArrayList<>());

    // when
    visitor.visit(change);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testCorrectPrimaryKeyColumnsHandling() {
    // given
    CorrectPrimaryKeyColumns change = new CorrectPrimaryKeyColumns("t3", new ArrayList<>());

    // when
    visitor.visit(change);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testRenameTableHandling() {
    // given
    RenameTable rename = new RenameTable("t3", "t4");

    // when
    visitor.visit(rename);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3", "T4"));
  }


  @Test
  public void testRenameIndexHandling() {
    // given
    RenameIndex rename = new RenameIndex("t3", "x", "y");

    // when
    visitor.visit(rename);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }


  @Test
  public void testAnalyseTableHandling() {
    // given
    AnalyseTable analyse = new AnalyseTable("t3");

    // when
    visitor.visit(analyse);

    // then
    assertThat(visitor.getResolvedTables().getModifiedTables(),
      Matchers.containsInAnyOrder("T3"));
  }
}

