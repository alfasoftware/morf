package org.alfasoftware.morf.upgrade;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeScriptGenerator.GraphBasedUpgradeScriptGeneratorFactory;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.Lists;

/**
 * Tests of {@link GraphBasedUpgradeScriptGenerator}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestGraphBasedUpgradeScriptGenerator {

  private GraphBasedUpgradeScriptGenerator gen;

  @Mock
  private Schema sourceSchema;

  @Mock
  private Schema targetSchema;

  @Mock
  private SqlDialect sqlDialect;

  @Mock
  private Table idTable;

  @Mock
  private ViewChanges viewChanges;

  @Mock
  private View view;

  @Mock
  private Table table;

  @Mock
  private UpgradeStatusTableService upgradeStatusTableService;

  @Mock
  private UpgradeScriptAddition upgradeScriptAddition;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    gen = new GraphBasedUpgradeScriptGenerator(sourceSchema, targetSchema, sqlDialect, idTable, viewChanges,
        upgradeStatusTableService, Sets.newSet(upgradeScriptAddition));


  }


  @Test
  public void testPreUpgradeStatementGeneration() {
    // given
    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS)).thenReturn(Lists.newArrayList("1"));
    when(sqlDialect.tableDeploymentStatements(idTable)).thenReturn(Lists.newArrayList("2"));
    when(viewChanges.getViewsToDrop()).thenReturn(Lists.newArrayList(view));
    when(view.getName()).thenReturn("x");
    when(sourceSchema.viewExists(nullable(String.class))).thenReturn(true);
    when(sqlDialect.dropStatements(view)).thenReturn(Lists.newArrayList("3"));
    when(sourceSchema.tableExists(nullable(String.class))).thenReturn(true);
    when(targetSchema.tableExists(nullable(String.class))).thenReturn(true);
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("4");

    // when
    List<String> statements = gen.generatePreUpgradeStatements();

    // then
    assertThat(statements, Matchers.contains("1", "2", "3", "4"));
  }


  @Test
  public void testPostUpgradeStatementGeneration() {
    // given
    when(sqlDialect.truncateTableStatements(idTable)).thenReturn(Lists.newArrayList("1"));
    when(sqlDialect.dropStatements(idTable)).thenReturn(Lists.newArrayList("2"));
    when(viewChanges.getViewsToDeploy()).thenReturn(Lists.newArrayList(view));
    when(view.getName()).thenReturn("x");
    when(sqlDialect.viewDeploymentStatements(view)).thenReturn(Lists.newArrayList("3"));
    when(targetSchema.tableExists(nullable(String.class))).thenReturn(true);
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), eq(targetSchema))).thenReturn("4");
    when(targetSchema.tables()).thenReturn(Lists.newArrayList(table));
    when(sqlDialect.convertCommentToSQL(any(String.class))).thenReturn("5");
    when(sqlDialect.rebuildTriggers(table)).thenReturn(Lists.newArrayList("6"));
    when(upgradeScriptAddition.sql()).thenReturn(Lists.newArrayList("7"));
    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED)).thenReturn(Lists.newArrayList("8"));


    // when
    List<String> statements = gen.generatePostUpgradeStatements();

    // then
    assertThat(statements, Matchers.contains("1", "2", "3", "4", "5", "6", "7", "8"));
  }


  @Test
  public void testFactory() {
    // given
    GraphBasedUpgradeScriptGeneratorFactory factory = new GraphBasedUpgradeScriptGeneratorFactory(upgradeStatusTableService, Sets.newSet(upgradeScriptAddition));

    // when
    GraphBasedUpgradeScriptGenerator created = factory.create(sourceSchema, targetSchema, sqlDialect, idTable, viewChanges);

    // then
    assertNotNull(created);
  }
}
