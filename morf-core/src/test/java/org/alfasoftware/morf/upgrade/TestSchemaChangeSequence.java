package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.hamcrest.MockitoHamcrest;

/**
 * Tests of {@link SchemaChangeSequence}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestSchemaChangeSequence {

  @Mock
  Column col, col2;

  @Mock
  Table table, table2, table3;

  @Mock
  Statement statement;

  @Mock
  Index index;

  @Mock
  SelectStatement select;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(index.getName()).thenReturn("mockIndex");
  }


  @Test
  public void testTableResolution() {
    // given
    when(col.isNullable()).thenReturn(true);
    when(col.getType()).thenReturn(DataType.STRING);

    when(table.getName()).thenReturn("t3");
    when(table2.getName()).thenReturn("t4");
    when(table3.getName()).thenReturn("t16");

    List<UpgradeStep> upgSteps = new ArrayList<>();
    upgSteps.add(new UpgradeStep1());

    // when
    SchemaChangeSequence schemaChangeSequence = new SchemaChangeSequence(new UpgradeConfigAndContext(), upgSteps, SchemaUtils.schema());

    // then
    UpgradeTableResolution res = schemaChangeSequence.getUpgradeTableResolution();
    assertThat(res.getModifiedTables(UpgradeStep1.class.getName()),
      Matchers.containsInAnyOrder("T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10",
        "T11", "T12", "T13", "T14", "T15", "T16", "T17"));
    verify(statement).accept(MockitoHamcrest.argThat(any(UpgradeTableResolutionVisitor.class)));
    verify(select).accept(MockitoHamcrest.argThat(any(UpgradeTableResolutionVisitor.class)));
  }


  /**
   * Tests that addIndexDeferred() records a DeferredAddIndex in the change sequence with the
   * correct table, index, and upgradeUUID taken from the step's {@code @UUID} annotation.
   */
  @Test
  public void testAddIndexDeferredProducesDeferredAddIndex() {
    // given
    when(index.getName()).thenReturn("TestIdx");
    when(index.columnNames()).thenReturn(List.of("col1"));
    when(index.isDeferred()).thenReturn(true);

    // when
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    SchemaChangeSequence seq = new SchemaChangeSequence(config, List.of(new StepWithDeferredAddIndex()), SchemaUtils.schema());
    List<SchemaChange> changes = seq.getAllChanges();

    // then -- now produces AddIndex with isDeferred()=true
    assertThat(changes, hasSize(1));
    assertThat(changes.get(0), instanceOf(AddIndex.class));
    AddIndex change = (AddIndex) changes.get(0);
    assertEquals("TestTable", change.getTableName());
    assertEquals("TestIdx", change.getNewIndex().getName());
    assertTrue("Index should be deferred", change.getNewIndex().isDeferred());
  }


  /** Tests that addIndexDeferred with force-immediate config produces an AddIndex instead of DeferredAddIndex. */
  @Test
  public void testAddIndexDeferredWithForceImmediateProducesAddIndex() {
    // given
    when(index.getName()).thenReturn("TestIdx");
    when(index.columnNames()).thenReturn(List.of("col1"));

    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceImmediateIndexes(Set.of("TestIdx"));

    // when
    SchemaChangeSequence seq = new SchemaChangeSequence(config, List.of(new StepWithDeferredAddIndex()), SchemaUtils.schema());
    List<SchemaChange> changes = seq.getAllChanges();

    // then
    assertThat(changes, hasSize(1));
    assertThat(changes.get(0), instanceOf(AddIndex.class));
    AddIndex change = (AddIndex) changes.get(0);
    assertEquals("TestTable", change.getTableName());
    assertEquals("TestIdx", change.getNewIndex().getName());
  }


  /** Tests that force-immediate matching is case-insensitive (H2 folds to uppercase). */
  @Test
  public void testAddIndexDeferredWithForceImmediateCaseInsensitive() {
    // given
    when(index.getName()).thenReturn("TestIdx");
    when(index.columnNames()).thenReturn(List.of("col1"));

    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceImmediateIndexes(Set.of("TESTIDX"));

    // when
    SchemaChangeSequence seq = new SchemaChangeSequence(config, List.of(new StepWithDeferredAddIndex()), SchemaUtils.schema());
    List<SchemaChange> changes = seq.getAllChanges();

    // then
    assertThat(changes, hasSize(1));
    assertThat(changes.get(0), instanceOf(AddIndex.class));
  }


  /** Tests that isForceImmediateIndex returns correct results with case-insensitive matching. */
  @Test
  public void testIsForceImmediateIndex() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceImmediateIndexes(Set.of("Idx_One", "IDX_TWO"));

    assertEquals(true, config.isForceImmediateIndex("Idx_One"));
    assertEquals(true, config.isForceImmediateIndex("idx_one"));
    assertEquals(true, config.isForceImmediateIndex("IDX_ONE"));
    assertEquals(true, config.isForceImmediateIndex("idx_two"));
    assertEquals(false, config.isForceImmediateIndex("Idx_Three"));
    assertEquals(2, config.getForceImmediateIndexes().size());
  }


  /** Tests that addIndex with force-deferred config produces a DeferredAddIndex instead of AddIndex. */
  @Test
  public void testAddIndexWithForceDeferredProducesDeferredAddIndex() {
    // given
    when(index.getName()).thenReturn("TestIdx");
    when(index.columnNames()).thenReturn(List.of("col1"));

    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceDeferredIndexes(Set.of("TestIdx"));

    // when
    SchemaChangeSequence seq = new SchemaChangeSequence(config, List.of(new StepWithAddIndex()), SchemaUtils.schema());
    List<SchemaChange> changes = seq.getAllChanges();

    // then -- force-deferred produces AddIndex with isDeferred()=true
    assertThat(changes, hasSize(1));
    assertThat(changes.get(0), instanceOf(AddIndex.class));
    AddIndex change = (AddIndex) changes.get(0);
    assertEquals("TestTable", change.getTableName());
    assertEquals("TestIdx", change.getNewIndex().getName());
    assertTrue("Index should be deferred", change.getNewIndex().isDeferred());
  }


  /** Tests that force-deferred matching is case-insensitive. */
  @Test
  public void testAddIndexWithForceDeferredCaseInsensitive() {
    // given
    when(index.getName()).thenReturn("TestIdx");
    when(index.columnNames()).thenReturn(List.of("col1"));

    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceDeferredIndexes(Set.of("TESTIDX"));

    // when
    SchemaChangeSequence seq = new SchemaChangeSequence(config, List.of(new StepWithAddIndex()), SchemaUtils.schema());
    List<SchemaChange> changes = seq.getAllChanges();

    // then
    assertThat(changes, hasSize(1));
    assertThat(changes.get(0), instanceOf(AddIndex.class));
    assertTrue("Index should be deferred", ((AddIndex) changes.get(0)).getNewIndex().isDeferred());
  }


  /** Tests that isForceDeferredIndex returns correct results with case-insensitive matching. */
  @Test
  public void testIsForceDeferredIndex() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceDeferredIndexes(Set.of("Idx_One", "IDX_TWO"));

    assertEquals(true, config.isForceDeferredIndex("Idx_One"));
    assertEquals(true, config.isForceDeferredIndex("idx_one"));
    assertEquals(true, config.isForceDeferredIndex("IDX_ONE"));
    assertEquals(true, config.isForceDeferredIndex("idx_two"));
    assertEquals(false, config.isForceDeferredIndex("Idx_Three"));
    assertEquals(2, config.getForceDeferredIndexes().size());
  }


  /** Tests that configuring the same index as both force-immediate and force-deferred throws. */
  @Test(expected = IllegalStateException.class)
  public void testConflictingForceImmediateAndForceDeferredThrows() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceImmediateIndexes(Set.of("ConflictIdx"));
    config.setForceDeferredIndexes(Set.of("ConflictIdx"));
  }


  /** Tests that the conflict check is case-insensitive. */
  @Test(expected = IllegalStateException.class)
  public void testConflictingForceImmediateAndForceDeferredCaseInsensitive() {
    UpgradeConfigAndContext config = new UpgradeConfigAndContext();
    config.setDeferredIndexCreationEnabled(true);
    config.setForceImmediateIndexes(Set.of("MyIndex"));
    config.setForceDeferredIndexes(Set.of("MYINDEX"));
  }


  @UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
  private class StepWithAddIndex implements UpgradeStep {
    @Override public String getJiraId() { return "TEST-2"; }
    @Override public String getDescription() { return "test"; }
    @Override public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndex("TestTable", index);
    }
  }


  @UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
  private class StepWithDeferredAddIndex implements UpgradeStep {
    @Override public String getJiraId() { return "TEST-1"; }
    @Override public String getDescription() { return "test"; }
    @Override public void execute(SchemaEditor schema, DataEditor data) {
      schema.addIndex("TestTable", index);
    }
  }


  private class UpgradeStep1 implements UpgradeStep {

    @Override
    public String getJiraId() {
      return "x";
    }

    @Override
    public String getDescription() {
      return "x";
    }

    @SuppressWarnings("deprecation")
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addColumn("t1", col);
      schema.addColumn("t2", col, FieldLiteral.fromObject("x"));
      schema.addTable(table);
      schema.removeTable(table2);
      schema.changeColumn("t5", col, col);
      schema.removeColumn("t6", col);
      schema.removeColumns("t7", col, col2);
      schema.addIndex("t8", index);
      schema.removeIndex("t9", index);
      schema.changeIndex("t10", index, index);
      schema.renameIndex("t11", "x", "y");
      schema.renameTable("t12", "t13");
      schema.changePrimaryKeyColumns("t14", new ArrayList<>(), new ArrayList<>());
      schema.correctPrimaryKeyColumns("t15", new ArrayList<>());
      schema.addTableFrom(table3, select);
      schema.analyseTable("t17");

      data.executeStatement(statement);
    }

  }
}

