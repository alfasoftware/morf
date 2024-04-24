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
@ImplementedBy(DatabaseUpgradePathValidationServiceImpl.class)
public interface DatabaseUpgradePathValidationService {

  /**
   * Creates the path validation SQL by combining creation of the upgrade status table, followed by optimistic locking SQL.
   * The optimistic locking statement should always come after, as the locking is dependent on the upgrade status table.
   */
  List<String> getPathValidationSql(long upgradeAuditCount);


  @ImplementedBy(DatabaseUpgradePathValidationServiceImpl.Factory.class)
  interface Factory {

    DatabaseUpgradePathValidationService create(ConnectionResources connectionResources);

  }

}
