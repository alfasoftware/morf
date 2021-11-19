package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.upgrade.TableDiscovery.DiscoveredTables;

public interface TableDiscoverable {
  void discoverTables(DiscoveredTables discoveredTables);
}
