package org.alfasoftware.morf.upgrade;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableDiscovery {

  private final Map<String, DiscoveredTables> discoveredTablesMap = new HashMap<>();

  public Set<String> getModifiedTables(String upgradeStepName) {
    return Collections.unmodifiableSet(discoveredTablesMap.get(upgradeStepName).modifiedTables);
  }


  public Set<String> getReadTables(String upgradeStepName) {
    return Collections.unmodifiableSet(discoveredTablesMap.get(upgradeStepName).readTables);
  }

  public void addDiscoveredTables(String upgradeStepName, DiscoveredTables discoveredTables) {
    discoveredTablesMap.put(upgradeStepName, discoveredTables);
  }

  public static class DiscoveredTables {
    private final Set<String> modifiedTables = new HashSet<>();
    private final Set<String> readTables = new HashSet<>();
    private boolean portableSqlStatementUsed;

    public void addModifiedTable(String tableName) {
      modifiedTables.add(tableName.toUpperCase());
    }


    public void addReadTable(String tableName) {
      readTables.add(tableName.toUpperCase());
    }

    public void portableSqlStatementUsed() {
      portableSqlStatementUsed = true;
    }

    public boolean isPortableSqlStatementUsed() {
      return portableSqlStatementUsed;
    }
  }

}
