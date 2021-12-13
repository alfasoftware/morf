package org.alfasoftware.morf.upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.sql.ResolvedTables;

/**
 * Stores table resolution information about the upgrade.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class UpgradeTableResolution {
  private final Map<String, ResolvedTables> resolvedTablesMap = new HashMap<>();

  /**
   * @param upgradeStepName
   * @return all tables modified by given upgrade step
   */
  public Set<String> getModifiedTables(String upgradeStepName) {
    return resolvedTablesMap.get(upgradeStepName).getModifiedTables();
  }


  /**
   * @param upgradeStepName
   * @return all tables read by given upgrade step
   */
  public Set<String> getReadTables(String upgradeStepName) {
    return resolvedTablesMap.get(upgradeStepName).getReadTables();
  }


  /**
   * Adds information about read/modified tables by given upgrade step
   *
   * @param upgradeStepName
   * @param resolvedTables
   */
  public void addDiscoveredTables(String upgradeStepName, ResolvedTables resolvedTables) {
    resolvedTablesMap.put(upgradeStepName, resolvedTables);
  }


  /**
   * @param upgradeStepName
   * @return true if given upgrade step is using {@link PortableSqlStatement}
   */
  public boolean isPortableSqlStatementUsed(String upgradeStepName) {
    return resolvedTablesMap.get(upgradeStepName).isPortableSqlStatementUsed();
  }
}
