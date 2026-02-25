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
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.COMPLETED;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.IN_PROGRESS;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.NONE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.MockDialect;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.AdditionalMetadata;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.GraphBasedUpgradeBuilder.GraphBasedUpgradeBuilderFactory;
import org.alfasoftware.morf.upgrade.MockConnectionResources.StubSchemaResource;
import org.alfasoftware.morf.upgrade.SchemaAutoHealer.SchemaHealingResults;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeCar;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeDriver;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.CreateDeployedViews;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Test {@link Upgrade} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public class TestUpgrade {

  public UpgradeStatusTableService upgradeStatusTableService;
  public ViewDeploymentValidator viewDeploymentValidator;
  private DatabaseUpgradePathValidationService databaseUpgradePathValidationService;
  private UpgradeConfigAndContext upgradeConfigAndContext;
  private DataSource dataSource;
  @Mock
  private Table idTable;

  @Mock
  private GraphBasedUpgradeBuilderFactory graphBasedUpgradeScriptGeneratorFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    upgradeStatusTableService = mock(UpgradeStatusTableService.class);
    viewDeploymentValidator = mock(ViewDeploymentValidator.class);
    dataSource = mock(DataSource.class);
    upgradeConfigAndContext = new UpgradeConfigAndContext();
    when(upgradeStatusTableService.getStatus(Optional.of(dataSource))).thenReturn(NONE);
    when(viewDeploymentValidator.validateExistingView(any(View.class), any(UpgradeSchemas.class))).thenReturn(true);
    when(viewDeploymentValidator.validateMissingView(any(View.class), any(UpgradeSchemas.class))).thenReturn(true);
    databaseUpgradePathValidationService = mock(DatabaseUpgradePathValidationService.class);
    when(databaseUpgradePathValidationService.getPathValidationSql(anyLong())).thenReturn(List.of("INIT"));
  }


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
    AdditionalMetadata additionalMetadata = mock(AdditionalMetadata.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(additionalMetadata));
    Map<String, List<Index>> indexMap = Maps.newHashMap();
    Index indexPrf1 = mock(Index.class);
    indexMap.put("withtypes", ImmutableList.of(indexPrf1));

    when(additionalMetadata.ignoredIndexes()).thenReturn(indexMap);

    when(mockConnectionResources.openSchemaResource(eq(mockConnectionResources.getDataSource()))).thenReturn(schemaResource);
    when(schemaResource.tables()).thenReturn(tables);

    UpgradePath results = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(mockConnectionResources),
      viewChangesDeploymentHelperFactory(mockConnectionResources), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
        .withUpgradeConfiguration(upgradeConfigAndContext)
        .create(mockConnectionResources)
        .findPath(targetSchema, upgradeSteps, Lists.newArrayList("^Drivers$", "^EXCLUDE_.*$"), mockConnectionResources.getDataSource());

    verify(additionalMetadata).ignoredIndexes();
    assertEquals("ignored indexes must match", indexMap, upgradeConfigAndContext.getIgnoredIndexes());
    assertEquals("Should be two steps.", 2, results.getSteps().size());
    List<String> sql = results.getSql();
    assertEquals("Number of SQL statements", 19, sql.size()); // Includes statements to add optimistic locking; create, truncate and then drop temp table; also 2 comments
  }


  /**
   * Test {@link Upgrade} schema consistency auto-healing.
   */
  @Test
  public void testUpgradeWithSchemaConsistencyHealing() throws SQLException {
    Table upgradeAudit = upgradeAudit();

    Table car = originalCar();
    Table carUpgraded = upgradedCar();

    Schema targetSchema = schema(upgradeAudit, carUpgraded);
    Collection<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(ChangeCar.class);

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(false);

    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(true, true, false);
    when(upgradeResultSet.getString(1)).thenReturn("0fde0d93-f57e-405c-81e9-245ef1ba0594", "0fde0d93-f57e-405c-81e9-245ef1ba0595");
    when(upgradeResultSet.next()).thenReturn(false);

    SqlDialect dialect = spy(new MockDialect());
    ConnectionResources mockConnectionResources = new MockConnectionResources().
        withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
        withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
        withDialect(dialect).
        create();

    SchemaResource schemaResource = new StubSchemaResource(schema(ImmutableList.of(upgradeAudit, car)));
    when(mockConnectionResources.openSchemaResource(mockConnectionResources.getDataSource())).thenReturn(schemaResource);

    when(dialect.getSchemaConsistencyStatements(any(SchemaResource.class))).thenReturn(ImmutableList.of("HEALING1", "HEALING2"));

    UpgradePath results = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(mockConnectionResources), viewChangesDeploymentHelperFactory(mockConnectionResources), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
        .withUpgradeConfiguration(upgradeConfigAndContext)
        .create(mockConnectionResources)
        .findPath(targetSchema, upgradeSteps, Lists.newArrayList(), mockConnectionResources.getDataSource());

    assertEquals("Should be one step.", 1, results.getSteps().size());
    List<String> sql = results.getSql();
    assertEquals("Number of SQL statements", 13, sql.size());

    // The path validation SQL should be first, then the healing statements.
    assertEquals("Path validation SQL present.", "INIT", sql.get(0));
    assertEquals("Healing SQL 1.", "HEALING1", sql.get(1));
    assertEquals("Healing SQL 2.", "HEALING2", sql.get(2));
  }


  /**
   * Test {@link Upgrade} schema-modifying auto-healing.
   */
  @Test
  public void testUpgradeWithSchemaHealing() throws SQLException {
    Table upgradeAudit = upgradeAudit();

    Table car = originalCar();
    Table carUpgraded = upgradedCar();

    Schema targetSchema = schema(upgradeAudit, carUpgraded);
    Collection<Class<? extends UpgradeStep>> upgradeSteps = new ArrayList<>();
    upgradeSteps.add(ChangeCar.class);

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(false);

    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(true, true, false);
    when(upgradeResultSet.getString(1)).thenReturn("0fde0d93-f57e-405c-81e9-245ef1ba0594", "0fde0d93-f57e-405c-81e9-245ef1ba0595");
    when(upgradeResultSet.next()).thenReturn(false);

    SqlDialect dialect = spy(new MockDialect());
    ConnectionResources mockConnectionResources = new MockConnectionResources().
        withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
        withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
        withDialect(dialect).
        create();

    SchemaResource schemaResource = new StubSchemaResource(schema(ImmutableList.of(upgradeAudit, carUpgraded))); // note: car already upgraded in source schema
    when(mockConnectionResources.openSchemaResource(mockConnectionResources.getDataSource())).thenReturn(schemaResource);

    SchemaHealingResults schemaHealingResults = mock (SchemaHealingResults.class);
    when(schemaHealingResults.getHealingStatements(dialect)).thenReturn(ImmutableList.of("MODIFYING1", "MODIFYING2"));
    when(schemaHealingResults.getHealedSchema()).thenReturn(schema(ImmutableList.of(upgradeAudit, car))); // note: make car not-upgraded again, in the modified schema, allowing the upgrade step to run
    SchemaAutoHealer schemaAutoHealer = mock(SchemaAutoHealer.class);
    when(schemaAutoHealer.analyseSchema(any())).thenReturn(schemaHealingResults);
    upgradeConfigAndContext.setSchemaAutoHealer(schemaAutoHealer);

    UpgradePath results = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(mockConnectionResources), viewChangesDeploymentHelperFactory(mockConnectionResources), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
        .withUpgradeConfiguration(upgradeConfigAndContext)
        .create(mockConnectionResources)
        .findPath(targetSchema, upgradeSteps, Lists.newArrayList(), mockConnectionResources.getDataSource());

    assertEquals("Should be one step.", 1, results.getSteps().size());
    List<String> sql = results.getSql();
    assertEquals("Number of SQL statements", 13, sql.size());

    // The path validation SQL should be first, then the healing statements.
    assertEquals("Path validation SQL present.", "INIT", sql.get(0));
    assertEquals("Healing SQL 1.", "MODIFYING1", sql.get(1));
    assertEquals("Healing SQL 2.", "MODIFYING2", sql.get(2));
  }


  /**
   * Test for checking the number of the upgrade audit rows.
   */
  @Test
  public void testAuditRowCount() throws SQLException {
    // Given
    ConnectionResources connection = mock(ConnectionResources.class, RETURNS_DEEP_STUBS);
    when(connection.sqlDialect().convertStatementToSQL(any(SelectStatement.class))).thenReturn("SELECT COUNT(UpgradeAudit.upgradeUUID) FROM UpgradeAudit");
    SqlScriptExecutor.ResultSetProcessor<Long> upgradeRowProcessor = mock(SqlScriptExecutor.ResultSetProcessor.class);

    // When
    new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .getUpgradeAuditRowCount(upgradeRowProcessor);

    // Then
    verify(upgradeRowProcessor).process(any(ResultSet.class));
  }


  /**
   * Test {@link Upgrade} adds the correct trigger rebuild message.
   */
  @Test
  public void testUpgradeWithTriggerMessage() throws SQLException {

    ResultSet viewResultSet = mock(ResultSet.class);
    when(viewResultSet.next()).thenReturn(true, true, false);
    when(viewResultSet.getString(1)).thenReturn("FooView", "OldView");
    when(viewResultSet.getString(2)).thenReturn("XXX");

    ResultSet upgradeResultSet = mock(ResultSet.class);
    when(upgradeResultSet.next()).thenReturn(false);

    SqlDialect dialect = spy(new MockDialect());
    when(dialect.rebuildTriggers(any(Table.class))).thenReturn(ImmutableList.of("A"));

    ConnectionResources connection = new MockConnectionResources().
                                              withSchema(schema(upgradeAudit(), deployedViews(), originalCar())).
                                              withResultSet("SELECT upgradeUUID FROM UpgradeAudit", upgradeResultSet).
                                              withResultSet("SELECT name, hash FROM DeployedViews", viewResultSet).
                                              create();
    when(connection.sqlDialect()).thenReturn(dialect);

    UpgradePath results = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(
      schema(upgradeAudit(), deployedViews(), upgradedCar()),
      ImmutableSet.<Class<? extends UpgradeStep>>of(ChangeCar.class),
                    new HashSet<>(),
      connection.getDataSource());

    assertTrue("Trigger rebuild comment is missing.", results.getSql().contains("-- Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns"));
  }


  private UpgradePathFactory upgradePathFactory() {
    UpgradePathFactory upgradePathFactory = mock(UpgradePathFactory.class);
    when(upgradePathFactory.create(anyList(), any(ConnectionResources.class), nullable(GraphBasedUpgradeBuilder.class), anyList()))
        .thenAnswer(invocation -> new UpgradePath(Sets.newHashSet(), invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(3), Collections.emptyList()));

    return upgradePathFactory;
  }


  private UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory(ConnectionResources mockConnectionResources) {
    UpgradeStatusTableService.Factory factory = mock(UpgradeStatusTableService.Factory.class);
    UpgradeStatusTableService upgradeStatusTableServiceMock = mock(UpgradeStatusTableService.class);
    when(upgradeStatusTableServiceMock.getStatus(Optional.of(mockConnectionResources.getDataSource()))).thenReturn(NONE);
    when(factory.create(any(ConnectionResources.class))).thenReturn(upgradeStatusTableServiceMock);
    return factory;
  }

  private ViewChangesDeploymentHelper.Factory viewChangesDeploymentHelperFactory(ConnectionResources mockConnectionResources) {
    CreateViewListener.Factory createViewListenerFactory = mock(CreateViewListener.Factory.class);
    when(createViewListenerFactory.createCreateViewListener(mockConnectionResources)).thenReturn(new CreateViewListener.NoOp());
    DropViewListener.Factory dropViewListenerFactory = mock(DropViewListener.Factory.class);
    when(dropViewListenerFactory.createDropViewListener(mockConnectionResources)).thenReturn(new DropViewListener.NoOp());
    return new ViewChangesDeploymentHelper.Factory(createViewListenerFactory, dropViewListenerFactory);
  }

  private ViewDeploymentValidator.Factory viewDeploymentValidatorFactory() {
    ViewDeploymentValidator.Factory factory = mock(ViewDeploymentValidator.Factory.class);
    when(factory.createViewDeploymentValidator(any(ConnectionResources.class))).thenReturn(mock(ViewDeploymentValidator.class));
    return factory;
  }


  private DatabaseUpgradePathValidationService.Factory databaseUpgradeLockServiceFactory() {
    DatabaseUpgradePathValidationService.Factory factory = mock(DatabaseUpgradePathValidationService.Factory.class);
    when(factory.create(any(ConnectionResources.class))).thenReturn(databaseUpgradePathValidationService);
    return factory;
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
    when(mockConnectionResources.sqlDialect().truncateTableStatements(any(Table.class))).thenReturn(Lists.newArrayList("1"));
    when(mockConnectionResources.sqlDialect().dropStatements(any(Table.class))).thenReturn(Lists.newArrayList("2"));
    when(mockConnectionResources.sqlDialect().getSchemaConsistencyStatements(any(SchemaResource.class))).thenReturn(Lists.newArrayList());

    UpgradePath results = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(mockConnectionResources), viewChangesDeploymentHelperFactory(mockConnectionResources), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(mockConnectionResources)
            .findPath(targetSchema,
      upgradeSteps, new HashSet<>(), mockConnectionResources.getDataSource());
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
    when(connection.sqlDialect().viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));
    when(connection.sqlDialect().truncateTableStatements(any(Table.class))).thenReturn(Lists.newArrayList("1"));
    when(connection.sqlDialect().dropStatements(any(Table.class))).thenReturn(Lists.newArrayList("2"));
    when(connection.sqlDialect().getSchemaConsistencyStatements(any(SchemaResource.class))).thenReturn(Lists.newArrayList());

    // When
    UpgradePath result = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[INIT, A]", result.getSql().toString());
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
    when(connection.sqlDialect().viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));
    when(connection.sqlDialect().truncateTableStatements(any(Table.class))).thenReturn(Lists.newArrayList("1"));
    when(connection.sqlDialect().dropStatements(any(Table.class))).thenReturn(Lists.newArrayList("2"));
    when(connection.sqlDialect().getSchemaConsistencyStatements(any(SchemaResource.class))).thenReturn(Lists.newArrayList());

    // When
    UpgradePath result = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[INIT, X, A]", result.getSql().toString());
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
    when(sqlDialect.viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("C"));
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<>());
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<>());
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
    UpgradePath result = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Upgrade class", ChangeCar.class, result.getSteps().get(0).getClass());
    // no drop view, only delete from DeployedViews
    assertEquals("SQL", "[INIT, G, IdTable, CM, A, C]", result.getSql().toString());
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
    when(sqlDialect.viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("C"));
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<>());
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<>());
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
    UpgradePath result = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Upgrade class", ChangeCar.class, result.getSteps().get(0).getClass());

    assertEquals("SQL", "[INIT, X, G, IdTable, CM, A, C]", result.getSql().toString());
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
    when(sqlDialect.viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("C"));
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
    UpgradePath result = new Upgrade.Factory(upgradePathFactory(), upgradeStatusTableServiceFactory(connection), viewChangesDeploymentHelperFactory(connection), viewDeploymentValidatorFactory(), databaseUpgradeLockServiceFactory(), graphBasedUpgradeScriptGeneratorFactory)
            .create(connection)
            .findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());
    // no drop view, only delete from DeployedViews
    assertEquals("SQL", "[INIT, D, A, C]", result.getSql().toString());
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
    when(connection.sqlDialect().viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(connection.sqlDialect().convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("C"));
    when(connection.sqlDialect().rebuildTriggers(any(Table.class))).thenReturn(Collections.<String>emptyList());
    when(connection.openSchemaResource(eq(connection.getDataSource()))).thenReturn(new StubSchemaResource(sourceSchema));
    when(upgradeStatusTableService.getStatus(Optional.of(connection.getDataSource()))).thenReturn(NONE);
    when(connection.sqlDialect().truncateTableStatements(any(Table.class))).thenReturn(Lists.newArrayList("1"));
    when(connection.sqlDialect().dropStatements(any(Table.class))).thenReturn(Lists.newArrayList("2"));
    when(connection.sqlDialect().getSchemaConsistencyStatements(any(SchemaResource.class))).thenReturn(Lists.newArrayList());

    // When
    UpgradePath result = new Upgrade(connection, upgradePathFactory(), upgradeStatusTableService, new ViewChangesDeploymentHelper(connection.sqlDialect()), viewDeploymentValidator, databaseUpgradePathValidationService, graphBasedUpgradeScriptGeneratorFactory, upgradeConfigAndContext).findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("Marker step JIRA ID", "\u2014", result.getSteps().get(0).getJiraId());
    assertEquals("Marker step description", "Update database views", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[INIT, A, C]", result.getSql().toString());
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
    when(sqlDialect.viewDeploymentStatementsAsLiteral(any(View.class))).thenReturn(literal("W"));
    when(sqlDialect.dropStatements(any(View.class))).thenReturn(ImmutableList.of("B"));
    when(sqlDialect.convertStatementToSQL(any(InsertStatement.class))).thenReturn(ImmutableList.of("C"));
    when(sqlDialect.convertStatementToSQL(any(DeleteStatement.class))).thenReturn("D");
    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(new HashSet<>());
    when(sqlDialect.convertCommentToSQL(any(String.class))).thenReturn("CM");
    when(sqlDialect.truncateTableStatements(any(Table.class))).thenReturn(new HashSet<>());
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
    when(upgradeStatusTableService.getStatus(Optional.of(connection.getDataSource()))).thenReturn(NONE);

    // When
    UpgradePath result = new Upgrade(connection, upgradePathFactory(), upgradeStatusTableService, new ViewChangesDeploymentHelper(connection.sqlDialect()), viewDeploymentValidator, databaseUpgradePathValidationService, graphBasedUpgradeScriptGeneratorFactory, upgradeConfigAndContext).findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

    // Then
    assertEquals("Steps to apply " + result.getSteps(), 1, result.getSteps().size());
    assertEquals("JIRA ID", "WEB-18348", result.getSteps().get(0).getJiraId());
    assertEquals("Description", "Foo", result.getSteps().get(0).getDescription());

    assertEquals("SQL", "[INIT, B, B, IdTable, CM, DeployedViews, A, C, A, C]", result.getSql().toString());
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
    when(upgradeStatusTableService.getStatus(Optional.of(connection.getDataSource()))).thenReturn(NONE);


    new Upgrade(connection, upgradePathFactory(), upgradeStatusTableService, new ViewChangesDeploymentHelper(connection.sqlDialect()), viewDeploymentValidator, databaseUpgradePathValidationService, graphBasedUpgradeScriptGeneratorFactory, upgradeConfigAndContext).findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());

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
   * Test that if there changes in progress - which might be detected early in the upgrade process
   */
  @Test
  public void testInProgressEarlyOne() throws SQLException {
    assertInProgressUpgrade(IN_PROGRESS, IN_PROGRESS, IN_PROGRESS);
  }


  /**
   * Test that if there changes in progress - which might be detected early in the upgrade process
   */
  @Test
  public void testInProgressEarlyTwo() throws SQLException {
    assertInProgressUpgrade(NONE, IN_PROGRESS, IN_PROGRESS);
  }


  /**
   * Test that if there changes in progress - which might be detected through no
   * upgrade path being found - an "in progress" path is returned.
   */
  @Test
  public void testInProgressUpgrade() throws SQLException {
    assertInProgressUpgrade(NONE, NONE, IN_PROGRESS);
  }


  /**
   * Test that if the upgrade has completed an "in progress" path is returned.
   */
  @Test
  public void testCompletedUpgrade() throws SQLException {
    assertInProgressUpgrade(COMPLETED, COMPLETED, COMPLETED);
  }


  /**
   * Test that if there are no changes in progress but there is no upgrade path being found
   * - the {@link UpgradePathFinder.NoUpgradePathExistsException} is propagated
   */
  @Test(expected = UpgradePathFinder.NoUpgradePathExistsException.class)
  public void testNoUpgradePath() throws SQLException {
    assertInProgressUpgrade(NONE, NONE, NONE);
  }


  /**
   * Allow verification of an in-progress upgrade. The {@link UpgradePath}
   * should report no steps to apply and that it is in-progress.
   *
   * @param status1 Status to be represented.
   * @param status2 Status to be represented.
   * @param status3 Status to be represented.
   * @throws SQLException if something goes wrong.
   */
  private void assertInProgressUpgrade(UpgradeStatus status1, UpgradeStatus status2, UpgradeStatus status3) throws SQLException {
    Schema sourceSchema = schema(
      schema(upgradeAudit(), deployedViews(), originalCar())
    );
    Schema targetSchema = schema(
      schema(upgradeAudit(), deployedViews(), upgradedCar())
    );

    Collection<Class<? extends UpgradeStep>> upgradeSteps = Collections.emptySet();

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
    UpgradeStatusTableService upgradeStatusTableService = mock(UpgradeStatusTableService.class);
    when(upgradeStatusTableService.getStatus(Optional.of(connection.getDataSource()))).thenReturn(status1, status2, status3);

    UpgradePath path = new Upgrade(connection, upgradePathFactory(), upgradeStatusTableService, new ViewChangesDeploymentHelper(connection.sqlDialect()), viewDeploymentValidator, databaseUpgradePathValidationService, graphBasedUpgradeScriptGeneratorFactory, upgradeConfigAndContext).findPath(targetSchema, upgradeSteps, new HashSet<>(), connection.getDataSource());
    assertFalse("Steps to apply", path.hasStepsToApply());
    assertTrue("In progress", path.upgradeInProgress());
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
          column("appliedTime", DataType.BIG_INTEGER).nullable()
        );
  }


  /**
   * @return the definition of {@code DeployedViews}.
   */
  public static Table deployedViews() {
    return table(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME).columns(column("name", DataType.STRING, 30), column("hash", DataType.STRING, 64));
  }
}
