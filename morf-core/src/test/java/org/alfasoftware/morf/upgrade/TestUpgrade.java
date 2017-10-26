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
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.MockDialect;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.MockConnectionResources.StubSchemaResource;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeCar;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeDriver;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.CreateDeployedViews;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Test {@link Upgrade} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestUpgrade {

  /**
   * Test {@link Upgrade}.
   */
  @Test
  public void testUpgrade() throws SQLException {
    Table upgradeAudit = upgradeAudit();

    Table car = originalCar();
    Table driver = table("Driver")
      .columns(
        idColumn(),
        versionColumn(),
        column("name", DataType.STRING, 10).nullable(),
        column("address", DataType.STRING, 10).nullable()
      );

    Table carUpgraded = upgradedCar();
    Table driverUpgraded = table("Driver")
      .columns(
        idColumn(),
        versionColumn(),
        column("name", DataType.STRING, 10).nullable(),
        column("address", DataType.STRING, 10).nullable(),
        column("postCode", DataType.STRING, 8).nullable()
      );
    //... this table should be excluded when findPath is invoked. If not it will be found in the trial upgrade schema and not in the target.
    Table excludedTable = table("Drivers");
    Table prefixExcludeTable1 = table("EXCLUDE_TABLE1");
    Table prefixExcludeTable2 = table("EXCLUDE_TABLE2");

    Schema targetSchema = schema(upgradeAudit, carUpgraded, driverUpgraded);
    Collection<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(ChangeCar.class);
    upgradeSteps.add(ChangeDriver.class);

    List<Table> tables = Arrays.asList(upgradeAudit, car, driver, excludedTable, prefixExcludeTable1, prefixExcludeTable2);
    
    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(false);
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(true, true, false);
    when(upgradeResultSet.getString(1)).thenReturn("0fde0d93-f57e-405c-81e9-245ef1ba0594", "0fde0d93-f57e-405c-81e9-245ef1ba0595");
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources mockConnectionResources = new MockConnectionResources().
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(mockConnectionResources.openSchemaResource(eq(mockConnectionResources.getDataSource()))).thenReturn(schemaResource);
    when(schemaResource.tables()).thenReturn(tables);

    UpgradePath results = new Upgrade(mockConnectionResources, mockConnectionResources.getDataSource(), upgradePathFactory()).findPath(targetSchema,
      upgradeSteps, Lists.newArrayList("^Drivers$", "^EXCLUDE_.*$"));

    assertEquals("Should be two steps.", 2, results.getSteps().size());
    assertEquals("Number of SQL statements", 18, results.getSql().size()); // Includes statements to create, truncate and then drop temp table, also 2 comments
  }


  /**
   * @return
   */
  private UpgradePathFactory upgradePathFactory() {
    UpgradePathFactory upgradePathFactory = mock(UpgradePathFactory.class);

    when(upgradePathFactory.create(anyListOf(UpgradeStep.class), any(SqlDialect.class))).thenAnswer(
      new Answer<UpgradePath>() {
        @Override
        public UpgradePath answer(InvocationOnMock invocation) throws Throwable {
          @SuppressWarnings("unchecked")
          UpgradePath upgradePath = new UpgradePath(Sets.<UpgradeScriptAddition>newHashSet(), (List<UpgradeStep>)invocation.getArguments()[0], (SqlDialect)invocation.getArguments()[1]);
          return upgradePath;
        }
      });

    return upgradePathFactory;
  }


  /**
   * @return a simple "Car" table.
   */
  private TableBuilder originalCar() {
    return table("Car")
      .columns(
        idColumn(),
        versionColumn(),
        column("name", DataType.STRING, 10).nullable(),
        column("engineCapacity", DataType.DECIMAL, 10).nullable()
      );
  }


  /**
   * @return an upgraded version of "Car".
   */
  private TableBuilder upgradedCar() {
    return table("Car")
      .columns(
        idColumn(),
        versionColumn(),
        column("name", DataType.STRING, 10).nullable(),
        column("engineVolume", DataType.DECIMAL, 20).nullable()
      );
  }


  /**
   * Test upgrade with no steps to apply.
   */
  @Test
  public void testUpgradeWithNoStepsToApply() {
    Table upgradeAudit = upgradeAudit();

    Schema targetSchema = schema(upgradeAudit);
    Collection<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();

    ConnectionResources mockConnectionResources = mock(ConnectionResources.class, RETURNS_DEEP_STUBS);
    SchemaResource schemaResource = mock(SchemaResource.class);
    when(mockConnectionResources.openSchemaResource(eq(mockConnectionResources.getDataSource()))).thenReturn(schemaResource);
    when(schemaResource.tables()).thenReturn(Arrays.asList(upgradeAudit));

    UpgradePath results = new Upgrade(mockConnectionResources, mockConnectionResources.getDataSource(), upgradePathFactory()).findPath(targetSchema,
      upgradeSteps, new HashSet<String>());
    assertTrue("No steps to apply", results.getSteps().isEmpty());
    assertTrue("No SQL statements", results.getSql().isEmpty());
  }


  /**
   * Test that if there are no upgrades to apply, but there is a new view,
   * that a pseudo-upgrade step is created and the SQL to apply the views defined.
   */
  @Test
  public void testUpgradeWithOnlyViewsToDeploy() {
    // Given
    Table upgradeAudit = upgradeAudit();
    View  testView     = view("FooView", select(field("name")).from(tableRef("Foo")));

    Schema sourceSchema = schema(upgradeAudit);
    Schema targetSchema = schema(
      schema(upgradeAudit),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = Collections.emptySet();

    ConnectionResources connection = mock(ConnectionResources.class, RETURNS_DEEP_STUBS);
    when(connection.sqlDialect().viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[A]", result.getSql().toString());
  }


  /**
   * Test that if there are no upgrades to apply, but there is a change to a view,
   * that a pseudo-upgrade step is created and the SQL to apply the views defined.
   */
  @Test
  public void testUpgradeWithChangedViewsToDeploy() {
    // Given
    Table upgradeAudit = upgradeAudit();
    View  otherView    = view("OldView", select(field("name")).from(tableRef("Old")));
    View  testView     = view("FooView", select(field("name")).from(tableRef("Foo")));

    Schema sourceSchema = schema(
      schema(upgradeAudit),
      schema(otherView)
    );
    Schema targetSchema = schema(
      schema(upgradeAudit),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = Collections.emptySet();

    ConnectionResources connection = mock(ConnectionResources.class, RETURNS_DEEP_STUBS);
    when(connection.sqlDialect().dropStatements(any(View.class))).thenReturn(ImmutableList.of("X"));
    when(connection.sqlDialect().viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[X, A]", result.getSql().toString());
  }


  /**
   * Test that if there are no views in the database, but views are declared in
   * {@code DeployedViews}, they are dropped; including when an upgrade is replacing
   * them all anyway.
   */
  @Test
  public void testUpgradeWithUpgradeStepsAndViewDeclaredButNotPresent() throws SQLException {
    // Given
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));
    Schema sourceSchema = schema(
      schema(upgradeAudit(), deployedViews(), originalCar())
    );
    Schema targetSchema = schema(
      schema(upgradeAudit(), deployedViews(), upgradedCar()),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = ImmutableSet.<Class<? extends UpgradeStep>>of(ChangeCar.class);

    SqlDialect sqlDialect = mock(SqlDialect.class);
    when(sqlDialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("XXX");
    when(sqlDialect.dropStatements(any(View.class))).thenReturn(ImmutableList.of("X"));
    when(sqlDialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));

    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), same(targetSchema))).thenReturn("C");
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), any(Schema.class), any(Table.class))).thenReturn(new ArrayList<String>());
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("G");
    when(sqlDialect.convertCommentToSQL(any(String.class))).thenReturn("CM");
    when(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).then(new Answer<String>() {
      @Override public String answer(InvocationOnMock invocation) throws Throwable {
        return new MockDialect().convertStatementToSQL((SelectStatement) invocation.getArguments()[0]);
      }
    });
    when(sqlDialect.tableDeploymentStatements(any(Table.class))).thenAnswer(new Answer<Collection<String>>() {
      @Override public Collection<String> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of(StringUtils.defaultString(((Table)invocation.getArguments()[0]).getName(), invocation.getArguments()[0].getClass().getSimpleName()));
      }
    });

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("FooView", "OldView");
    when(viewResultSet.getString(2)).thenReturn("XXX");
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources connection = new MockConnectionResources().
                                              withDialect(sqlDialect).
                                              withSchema(sourceSchema).
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Upgrade class", ChangeCar.class, result.getSteps().get(0).getClass());

    assertEquals("SQL", "[X, G, IdTable, CM, A, C]", result.getSql().toString());
  }


  /**
   * Test that if there are views in the database, and views are declared in
   * {@code DeployedViews}, they are dropped; including when an upgrade is replacing
   * them all anyway.
   */
  @Test
  public void testUpgradeWithUpgradeStepsAndViewDeclared() throws SQLException {
    // Given
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));
    Schema sourceSchema = schema(
      schema(upgradeAudit(), deployedViews(), originalCar()),
      schema(testView)
    );
    Schema targetSchema = schema(
      schema(upgradeAudit(), deployedViews(), upgradedCar()),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = ImmutableSet.<Class<? extends UpgradeStep>>of(ChangeCar.class);

    SqlDialect sqlDialect = mock(SqlDialect.class);
    when(sqlDialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("XXX");
    when(sqlDialect.dropStatements(any(View.class))).thenReturn(ImmutableList.of("X"));
    when(sqlDialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));

    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), same(targetSchema))).thenReturn("C");
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), any(Schema.class), any(Table.class))).thenReturn(new ArrayList<String>());
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("G");
    when(sqlDialect.convertCommentToSQL(any(String.class))).thenReturn("CM");
    when(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).then(new Answer<String>() {
      @Override public String answer(InvocationOnMock invocation) throws Throwable {
        return new MockDialect().convertStatementToSQL((SelectStatement) invocation.getArguments()[0]);
      }
    });
    when(sqlDialect.tableDeploymentStatements(any(Table.class))).thenAnswer(new Answer<Collection<String>>() {
      @Override public Collection<String> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of(StringUtils.defaultString(((Table)invocation.getArguments()[0]).getName(), invocation.getArguments()[0].getClass().getSimpleName()));
      }
    });

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("FooView", "OldView");
    when(viewResultSet.getString(2)).thenReturn("XXX");
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources connection = new MockConnectionResources().
                                              withDialect(sqlDialect).
                                              withSchema(sourceSchema).
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Upgrade class", ChangeCar.class, result.getSteps().get(0).getClass());

    assertEquals("SQL", "[X, G, IdTable, CM, A, C]", result.getSql().toString());
  }


  /**
   * Test that if there are no views in the database, but views are declared in
   * {@code DeployedViews}, they are dropped.
   */
  @Test
  public void testUpgradeWithViewDeclaredButNotPresent() throws SQLException {
    // Given
    Table upgradeAudit  = upgradeAudit();
    Table deployedViews = deployedViews();
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));

    Schema sourceSchema = schema(
      schema(upgradeAudit, deployedViews)
    );
    Schema targetSchema = schema(
      schema(upgradeAudit, deployedViews),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = Collections.emptySet();

    SqlDialect sqlDialect = mock(SqlDialect.class);
    when(sqlDialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("XXX");
    when(sqlDialect.dropStatements(any(View.class))).thenReturn(ImmutableList.of("X"));
    when(sqlDialect.viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));

    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), same(targetSchema))).thenReturn("C");
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).then(new Answer<String>() {
      @Override public String answer(InvocationOnMock invocation) throws Throwable {
        return new MockDialect().convertStatementToSQL((SelectStatement) invocation.getArguments()[0]);
      }
    });

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("FooView", "OldView");
    when(viewResultSet.getString(2)).thenReturn("XXX");
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources connection = new MockConnectionResources().
                                              withDialect(sqlDialect).
                                              withSchema(sourceSchema).
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[X, D, A, C]", result.getSql().toString());
  }


  /**
   * Similar to {@link #testUpgradeWithOnlyViewsToDeploy()} but when a {@code DeployedViews}
   * table exists, and so should be updated.
   */
  @Test
  public void testUpgradeWithOnlyViewsToDeployWithExistingDeployedViews() {
    // Given
    Table upgradeAudit  = upgradeAudit();
    Table deployedViews = table("DeployedViews").columns(column("name", DataType.STRING, 30), column("hash", DataType.STRING, 64));
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));

    Schema sourceSchema = schema(upgradeAudit, deployedViews);
    Schema targetSchema = schema(
      schema(upgradeAudit, deployedViews),
      schema(testView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = Collections.emptySet();

    ConnectionResources connection = mock(ConnectionResources.class, RETURNS_DEEP_STUBS);
    when(connection.sqlDialect().viewDeploymentStatements(same(testView))).thenReturn(ImmutableList.of("A"));
    when(connection.sqlDialect().convertStatementToSQL(any(InsertStatement.class), same(targetSchema))).thenReturn("C");
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[A, C]", result.getSql().toString());
  }


  /**
   * Similar to {@link #testUpgradeWithOnlyViewsToDeployWithExistingDeployedViews()} but where
   * {@code DeployedViews} only exists in the target schema, not the current schema. This also
   * tests the circumstance where there are upgrade steps to be run: so we do not need a
   * pseudo-upgrade step.
   *
   * <p>Existing views are dropped.</p>
   *
   * @throws SQLException if something goes wrong.
   */
  @Test
  public void testUpgradeWithToDeployAndNewDeployedViews() throws SQLException {
    // Given
    Table upgradeAudit  = upgradeAudit();
    Table deployedViews = deployedViews();
    View  otherView     = view("OldView", select(field("name")).from(tableRef("Old")));
    View  testView      = view("FooView", select(field("name")).from(tableRef("Foo")));
    View  staticView    = view("StaticView", select(field("name")).from(tableRef("Unchanged")));

    Schema sourceSchema = schema(
      schema(upgradeAudit),
      schema(otherView, staticView)
    );
    Schema targetSchema = schema(
      schema(upgradeAudit, deployedViews),
      schema(testView, staticView)
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = ImmutableList.<Class<? extends UpgradeStep>>of(CreateDeployedViews.class);

    SqlDialect sqlDialect = mock(SqlDialect.class);
    when(sqlDialect.convertStatementToHash(any(SelectStatement.class))).thenReturn("XXX");
    when(sqlDialect.viewDeploymentStatements(any(View.class))).thenReturn(ImmutableList.of("A"));
    when(sqlDialect.dropStatements(any(View.class))).thenReturn(ImmutableList.of("B"));
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), same(targetSchema))).thenReturn("C");
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class), any(Schema.class), any(Table.class))).thenReturn(new ArrayList<String>());
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.convertCommentToSQL(any(String.class))).thenReturn("CM");
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<String>());
    when(sqlDialect.convertStatementToSQL(any(SelectStatement.class))).then(new Answer<String>() {
      @Override public String answer(InvocationOnMock invocation) throws Throwable {
        return new MockDialect().convertStatementToSQL((SelectStatement) invocation.getArguments()[0]);
      }
    });
    when(sqlDialect.tableDeploymentStatements(any(Table.class))).thenAnswer(new Answer<Collection<String>>() {
      @Override public Collection<String> answer(InvocationOnMock invocation) throws Throwable {
        return ImmutableList.of(StringUtils.defaultString(((Table)invocation.getArguments()[0]).getName(), invocation.getArguments()[0].getClass().getSimpleName()));
      }
    });

    
    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("OtherView", "StaticView");
    when(viewResultSet.getString(2)).thenReturn("XXX");
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources connection = new MockConnectionResources().
                                         withDialect(sqlDialect).
                                         withSchema(sourceSchema).
                                         withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                         withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                         create();

    // When
    UpgradePath result = new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("JIRA ID", "WEB-18348", result.getSteps().get(0).getJiraId());
    assertEquals("Description", "Foo", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[B, B, IdTable, CM, DeployedViews, A, C, A, C]", result.getSql().toString());
  }


  /**
   * Test that if there are database steps to apply, then all table triggers will be rebuilt.
   */
  @Test
  public void testUpgradeWithStepsToApplyRebuildTriggers() throws SQLException {
    Schema sourceSchema = schema(
      schema(upgradeAudit(), deployedViews(), originalCar())
    );
    Schema targetSchema = schema(
      schema(upgradeAudit(), deployedViews(), upgradedCar())
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = ImmutableSet.<Class<? extends UpgradeStep>>of(ChangeCar.class);

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("FooView", "OldView");
    when(viewResultSet.getString(2)).thenReturn("XXX");
    
    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);
    
    ConnectionResources connection = new MockConnectionResources().
                                              withSchema(sourceSchema).
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();

    new Upgrade(connection, connection.getDataSource(), upgradePathFactory()).findPath(targetSchema, upgradeSteps, new HashSet<String>());

    ArgumentCaptor<Table> tableArgumentCaptor = ArgumentCaptor.forClass(Table.class);
    verify(connection.sqlDialect(), times(3)).rebuildTriggers(tableArgumentCaptor.capture());

    List<Table> rebuildTriggerTables = tableArgumentCaptor.getAllValues();

    List<String> rebuildTriggerTableNames = Lists.transform(rebuildTriggerTables, new Function<Table, String>() {
      @Override
      public String apply(Table input) {
        return input.getName();
      }
    });

    assertThat("Rebuild trigger table arguments are wrong", rebuildTriggerTableNames, containsInAnyOrder("UpgradeAudit", "Car", "DeployedViews"));
  }


  /**
   * @return the definition of {@code UpgradeAudit}.
   */
  private static Table upgradeAudit() {
    return table(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME)
        .columns(
          idColumn(),
          versionColumn(),
          column("upgradeUUID", DataType.STRING, 100).nullable(),
          column("description", DataType.STRING, 200).nullable(),
          column("appliedTime", DataType.BIG_INTEGER, 14).nullable()
        );
  }


  /**
   * @return the definition of {@code DeployedViews}.
   */
  public static Table deployedViews() {
    return table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME).columns(column("name", DataType.STRING, 30), column("hash", DataType.STRING, 64));
  }
}
