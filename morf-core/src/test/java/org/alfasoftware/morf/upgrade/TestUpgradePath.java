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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactory;
import org.alfasoftware.morf.upgrade.UpgradePath.UpgradePathFactoryImpl;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link UpgradePath} and {@link UpgradePathFactoryImpl}.
 *
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class TestUpgradePath {

  SqlDialect sqlDialect;

  UpgradeStatusTableService upgradeStatusTableService;

  UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory;

  UpgradePathFactory factory;

  ConnectionResources connectionResources;


  @Before
  public void setUp() {
    sqlDialect = mock(SqlDialect.class);
    connectionResources = mock(ConnectionResources.class);
    when(connectionResources.sqlDialect()).thenReturn(sqlDialect);

    upgradeStatusTableService = mock(UpgradeStatusTableService.class);
    upgradeStatusTableServiceFactory = mock(UpgradeStatusTableService.Factory.class);
    when(upgradeStatusTableServiceFactory.create(connectionResources)).thenReturn(upgradeStatusTableService);
    factory = new UpgradePathFactoryImpl(new UpgradeScriptAdditionsProvider.NoOpScriptAdditions(), upgradeStatusTableServiceFactory);
  }

  /**
   * Tests that upgrade script additions are appended to the upgrade path SQL.
   */
  @Test
  public void testUpgradeScriptAdditions() {
    Set<UpgradeScriptAddition> scriptAdditions = Sets.<UpgradeScriptAddition>newLinkedHashSet();

    scriptAdditions.add(createScriptAddition("ABC", "DEF"));
    scriptAdditions.add(createScriptAddition("GHI"));

    UpgradePath path = new UpgradePath(scriptAdditions, connectionResources, Collections.emptyList(), Collections.emptyList());

    path.writeSql(ImmutableList.of("A", "B", "C"));

    assertEquals("SQL", ImmutableList.of("A", "B", "C", "ABC", "DEF", "GHI"), path.getSql());
  }


  /**
   * Test that {@link UpgradePath#(UpgradeStatus)} creates a placeholder
   * without steps.
   */
  @Test
  public void testForInProgressUpgrade() {
    UpgradePath result = new UpgradePath(UpgradeStatus.DATA_TRANSFER_IN_PROGRESS);

    assertEquals("Steps should be empty", Collections.emptyList(), result.getSteps());
    assertEquals("SQL should be empty", Collections.emptyList(), result.getSql());
  }


  /**
   * Test SQL ordering for upgrade status.
   */
  @Test
  public void testSqlOrdering() {
    Set<UpgradeScriptAddition> upgradeScriptAdditions = ImmutableSet.of(createScriptAddition("ABC", "DEF"),
                                                                        createScriptAddition("GHI"));
    UpgradePath path = new UpgradePath(upgradeScriptAdditions, connectionResources, ImmutableList.of("INIT1", "INIT2"), ImmutableList.of("FIN1", "FIN2"));
    path.writeSql(ImmutableList.of("XYZZY"));

    List<String> sql = path.getSql();
    assertEquals("Result", "[INIT1, INIT2, XYZZY, ABC, DEF, GHI, FIN1, FIN2]", sql.toString());
  }


  /**
   * Test SQL initialisation and finalisation are not included when
   * there's nothing to do.
   */
  @Test
  public void testSqlOrderingWhenEmpty() {
    Set<UpgradeScriptAddition> upgradeScriptAdditions = Collections.emptySet();
    UpgradePath path = new UpgradePath(upgradeScriptAdditions, connectionResources, ImmutableList.of("INIT1", "INIT2"), ImmutableList.of("FIN1", "FIN2"));

    List<String> sql = path.getSql();
    assertEquals("Result", "[]", sql.toString());
  }


  /**
   * Test that {@link UpgradePathFactoryImpl#create(ConnectionResources)} correctly
   * uses {@link UpgradeStatusTableService} for deployments.
   */
  @Test
  public void testFactoryCreateDeployment() {
    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS)).thenReturn(ImmutableList.of("INIT1", "INIT2"));
    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED)).thenReturn(ImmutableList.of("FIN1", "FIN2"));

    UpgradePath path = factory.create(connectionResources);
    path.writeSql(ImmutableList.of("XYZZY"));

    List<String> sql = path.getSql();
    assertEquals("Result", "[INIT1, INIT2, XYZZY, FIN1, FIN2]", sql.toString());

    verify(upgradeStatusTableService, times(1)).updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS);
    verify(upgradeStatusTableService, times(1)).updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED);
    verifyNoMoreInteractions(upgradeStatusTableService);
  }


  /**
   * Test that {@link UpgradePathFactoryImpl#create(List, ConnectionResources)} correctly
   * uses {@link UpgradeStatusTableService} for upgrades.
   */
  @Test
  public void testFactoryCreateUpgrade() {

    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS)).thenReturn(ImmutableList.of("INIT1", "INIT2"));
    when(upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED)).thenReturn(ImmutableList.of("FIN1", "FIN2"));

    UpgradePath path = factory.create(ImmutableList.of(mock(UpgradeStep.class)), connectionResources);
    path.writeSql(ImmutableList.of("XYZZY"));

    List<String> sql = path.getSql();
    assertEquals("Result", "[INIT1, INIT2, XYZZY, FIN1, FIN2]", sql.toString());

    verify(upgradeStatusTableService, times(1)).updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS);
    verify(upgradeStatusTableService, times(1)).updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED);
    verifyNoMoreInteractions(upgradeStatusTableService);
  }


  /**
   * Test that the comments to drop {@value UpgradeStatusTableService#UPGRADE_STATUS} table
   * are added at the end of the list when using {@link UpgradePath#getUpgradeSqlScript()}.
   */
  @Test
  public void testAddCommentsToDropUpgradeStatusTable() {
    UpgradePath path = factory.create(ImmutableList.of(mock(UpgradeStep.class)), connectionResources);
    path.writeSql(ImmutableList.of("ABC", "DEF"));

    when(sqlDialect.dropStatements(any(Table.class))).thenReturn(Arrays.asList("FLUSH UPGRADE TABLE","DROP UPGRADE TABLE"));

    String result = path.getUpgradeSqlScript();

    assertTrue("At least one comment is missing.", result.contains("-- WARNING - If the upgrade is run automatically, the table will be automatically removed at a later point."));
    assertTrue("At least one comment is missing.", result.contains("-- WARNING - Manual removal should not be applied during full deployment of the application to an empty database."));
    assertTrue("Comment to flush table is missing.", result.contains("-- FLUSH UPGRADE TABLE"));
    assertTrue("Comment to drop table is missing.", result.contains("-- DROP UPGRADE TABLE"));
  }


  /**
   * Convenience method for creating an {@link UpgradeScriptAddition} from the
   * given list of SQL statements.
   *
   * @param sql SQL statements to include in the upgrade script addition.
   * @return Upgrade script addition class.
   */
  private UpgradeScriptAddition createScriptAddition(String... sql) {
    UpgradeScriptAddition scriptAddition = mock(UpgradeScriptAddition.class);
    when(scriptAddition.sql(connectionResources)).thenReturn(Lists.newArrayList(sql));
    return scriptAddition;
  }
}
