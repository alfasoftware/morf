package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertTrue;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.junit.Test;

/**
 * Test of {@link UpgradeTableResolution}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestUpgradeTableResolution {

  @Test
  public void testPortableSqlStatementUsed() {
    // given
    UpgradeTableResolution upgradeTableResolution = new UpgradeTableResolution();
    ResolvedTables resolvedTables = new ResolvedTables();
    resolvedTables.portableSqlStatementUsed();

    upgradeTableResolution.addDiscoveredTables("x", resolvedTables);

    // when then
    assertTrue(upgradeTableResolution.isPortableSqlStatementUsed("x"));
  }
}
