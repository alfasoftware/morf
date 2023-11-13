package org.alfasoftware.morf.upgrade;

import java.util.List;

import org.alfasoftware.morf.jdbc.ConnectionResources;

import com.google.inject.ImplementedBy;

/**
 * Generates the initialisation SQL run at the beginning of the upgrade script, which includes creation of
 * the upgrade status table, and an optimistic locking mechanism to prevent multiple executions of the upgrade.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
@ImplementedBy(DatabaseUpgradeLockServiceImpl.class)
public interface DatabaseUpgradeLockService {

  /**
   * Creates the initialisation SQL by combining the creation of the upgrade status table, followed by the optimised locking SQL.
   * The optimised locking statement should always come after, as the locking is dependent on the upgrade status table.
   */
  List<String> getInitialisationSql(long upgradeAuditCount);


  @ImplementedBy(DatabaseUpgradeLockServiceImpl.Factory.class)
  interface Factory {

    DatabaseUpgradeLockService create(ConnectionResources connectionResources);

  }

}
