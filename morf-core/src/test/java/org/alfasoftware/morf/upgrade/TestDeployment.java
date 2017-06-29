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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static com.google.common.collect.FluentIterable.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Test {@link Deployment} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestDeployment {

  private final SqlDialect dialect = mock(SqlDialect.class);

  private final UpgradePathFactory upgradePathFactory = mock(UpgradePathFactory.class);

  @Before
  public void setUp() {
    when(upgradePathFactory.create(any(SqlDialect.class))).thenAnswer(
      new Answer<UpgradePath>() {
        @Override
        public UpgradePath answer(InvocationOnMock invocation) throws Throwable {
          return new UpgradePath(Sets.<UpgradeScriptAddition>newHashSet(), (SqlDialect)invocation.getArguments()[0]);
        }
      });
  }

  /**
   * Test that an empty schema is deployed into correctly, honouring view
   * dependencies.
   */
  @Test
  public void testGetPath() {
    // Given
    Table testTable = table("Foo").columns(column("name", DataType.STRING, 32));
    View  testView  = view("FooView", select(field("name")).from(tableRef("Foo")));
    View  testView2  = view("BarView", select(field("name")).from(tableRef("MooView")), "MooView");
    View  testView3  = view("MooView", select(field("name")).from(tableRef("FooView")), "FooView");

    when(dialect.tableDeploymentStatements(same(testTable))).thenReturn(ImmutableList.of("A"));
    when(dialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("B"));
    when(dialect.viewDeploymentStatements(same(testView2))).thenReturn(ImmutableList.of("C"));
    when(dialect.viewDeploymentStatements(same(testView3))).thenReturn(ImmutableList.of("D"));

    Schema targetSchema = schema(
      schema(testTable),
      schema(testView, testView2, testView3)
    );

    // When
    Deployment deployment = new Deployment(dialect, null, upgradePathFactory);
    UpgradePath path = deployment.getPath(targetSchema, Lists.<Class<? extends UpgradeStep>>newArrayList());

    // Then
    assertTrue("Steps to apply", path.hasStepsToApply());
    assertEquals("Steps", "[]", path.getSteps().toString());
    assertEquals("SQL", ImmutableList.of("A", "B", "D", "C"), path.getSql());
  }


  /**
   * Test that an empty schema is deployed containing {@code DeployedViews}.
   */
  @Test
  public void testGetPathWithDeployedViews() {
    // Given
    Table testTable     = table("Foo").columns(column("name", DataType.STRING, 32));
    Table deployedViews = table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME).columns(column("name", DataType.STRING, 30), column("hash", DataType.STRING, 64));
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));

    when(dialect.tableDeploymentStatements(same(testTable))).thenReturn(ImmutableList.of("A"));
    when(dialect.tableDeploymentStatements(same(deployedViews))).thenReturn(ImmutableList.of("B"));
    when(dialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("C"));
    when(dialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("D"));
    when(dialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("E");

    Schema targetSchema = schema(
      schema(testTable, deployedViews),
      schema(testView)
    );


    Deployment deployment = new Deployment(dialect, null, upgradePathFactory);
    UpgradePath path = deployment.getPath(targetSchema, Lists.<Class<? extends UpgradeStep>>newArrayList());

    // Then
    assertTrue("Steps to apply", path.hasStepsToApply());
    assertEquals("Steps", "[]", path.getSteps().toString());
    assertEquals("SQL", ImmutableList.of("A", "B", "C", "D"), path.getSql());

    ArgumentCaptor<InsertStatement> captor = ArgumentCaptor.forClass(InsertStatement.class);
    verify(dialect).convertStatementToSQL(captor.capture());
    InsertStatement stmt = captor.getValue();
    assertEquals("Table", "DeployedViews", stmt.getTable().getName());

    List<String> values = Lists.newArrayList();
    for (AliasedField value : stmt.getValues())
      values.add(((FieldLiteral)value).getValue());

    assertEquals("Values", "[FOOVIEW, E]", values.toString());
  }


  /**
   * Test that an empty schema is deployed containing the upgrade steps passed to the method.
   */
  @Test
  public void testGetPathWithUpgradeSteps() {
    // Given
    Table testTable     = table("Foo").columns(column("name", DataType.STRING, 32));
    Collection<Class<? extends UpgradeStep>> stepsToApply = new ArrayList<Class<? extends UpgradeStep>>();
    stepsToApply.add(AddFooTable.class);
    when(dialect.tableDeploymentStatements(same(testTable))).thenReturn(ImmutableList.of("A"));

    Schema targetSchema = schema(testTable);

    // When
    Deployment deployment = new Deployment(dialect, null, upgradePathFactory);
    UpgradePath path = deployment.getPath(targetSchema, stepsToApply);

    // Then
    assertTrue("Steps to apply", path.hasStepsToApply());
    assertEquals("Steps", "[]", path.getSteps().toString());
    assertEquals("SQL", ImmutableList.of("A"), path.getSql());

    ArgumentCaptor<InsertStatement> captor = ArgumentCaptor.forClass(InsertStatement.class);
    verify(dialect).convertStatementToSQL(captor.capture());
    InsertStatement stmt = captor.getValue();
    assertEquals("Upgrade audit table name", "UpgradeAudit", stmt.getTable().getName());

    List<String> values = from(stmt.getValues()).filter(FieldLiteral.class).transform(new Function<FieldLiteral, String>() {
      @Override
      public String apply(FieldLiteral input) {
        return input.getValue();
      }
    }).toList();

    assertEquals("Number of columns", 3, stmt.getValues().size());
    assertEquals("UUID", "ab1b9f5a-cb3b-473c-8ec6-c6c1134f500f", values.get(0).toString());
    assertEquals("Description", "org.alfasoftware.morf.upgrade.TestDeployment$AddFooTable", values.get(1).toString());
    assertEquals("Date", 1, from(stmt.getValues()).filter(org.alfasoftware.morf.sql.element.Function.class).toList().size());
  }


  /**
   * Upgrade step that adds the table used in {@link TestDeployment#testGetPathWithUpgradeSteps()}.
   *
   * @author Copyright (c) Alfa Financial Software 2013
   */
  @Sequence(1)
  @UUID("ab1b9f5a-cb3b-473c-8ec6-c6c1134f500f")
  private static class AddFooTable implements UpgradeStep {

    /**
     * @see org.alfasoftware.morf.upgrade.UpgradeStep#getJiraId()
     */
    @Override
    public String getJiraId() {
      return "WEB-1234";
    }

    /**
     * @see org.alfasoftware.morf.upgrade.UpgradeStep#getDescription()
     */
    @Override
    public String getDescription() {
      return "My New Table";
    }

    /**
     * @see org.alfasoftware.morf.upgrade.UpgradeStep#execute(org.alfasoftware.morf.upgrade.SchemaEditor, org.alfasoftware.morf.upgrade.DataEditor)
     */
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.addTable(table("Foo").columns(column("name", DataType.STRING, 32)));
    }
  }
}
