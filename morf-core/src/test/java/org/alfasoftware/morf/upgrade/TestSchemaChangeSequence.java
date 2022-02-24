package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
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
    SchemaChangeSequence schemaChangeSequence = new SchemaChangeSequence(upgSteps);

    // then
    UpgradeTableResolution res = schemaChangeSequence.getUpgradeTableResolution();
    assertThat(res.getModifiedTables(UpgradeStep1.class.getName()),
      Matchers.containsInAnyOrder("T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10",
        "T11", "T12", "T13", "T14", "T15", "T16", "T17"));
    verify(statement).accept(MockitoHamcrest.argThat(any(UpgradeTableResolutionVisitor.class)));
    verify(select).accept(MockitoHamcrest.argThat(any(UpgradeTableResolutionVisitor.class)));
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

