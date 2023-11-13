package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Criterion.neq;
import static org.alfasoftware.morf.sql.element.Function.count;

import java.util.List;

import javax.inject.Inject;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.common.collect.ImmutableList;

/**
 * Implementation of {@link DatabaseUpgradeLockService}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
public class DatabaseUpgradeLockServiceImpl implements DatabaseUpgradeLockService {

  private final ConnectionResources connectionResources;
  private final UpgradeStatusTableService upgradeStatusTableService;

  /**
   * Used for direct injection
   */
  @Inject
  DatabaseUpgradeLockServiceImpl(ConnectionResources connectionResources,
                                 UpgradeStatusTableService upgradeStatusTableService) {
    this.connectionResources = connectionResources;
    this.upgradeStatusTableService = upgradeStatusTableService;
  }

  /**
   * Used by {@link org.alfasoftware.morf.upgrade.DatabaseUpgradeLockService.Factory}
   */
  DatabaseUpgradeLockServiceImpl(ConnectionResources connectionResources,
                                 UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory) {
    this.connectionResources = connectionResources;
    this.upgradeStatusTableService = upgradeStatusTableServiceFactory.create(connectionResources);
  }


  @Override
  public List<String> getInitialisationSql(long upgradeAuditCount) {
    ImmutableList.Builder<String> statements = ImmutableList.builder();
    statements.addAll(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS));
    statements.addAll(getOptimisticLockingInitialisationSql(upgradeAuditCount));
    return statements.build();
  }


  /**
   * Generates SQL to be run at the start of the upgrade script which ensures the upgrade can't be run twice
   */
  private List<String> getOptimisticLockingInitialisationSql(long upgradeAuditCount) {
    TableReference upgradeStatusTable = tableRef(UpgradeStatusTableService.UPGRADE_STATUS);

    SelectStatement selectStatement = select().from(upgradeStatusTable)
        .where(neq(selectUpgradeAuditTableCount().asField(), upgradeAuditCount))
        .build();

    InsertStatement insertStatement = insert().into(upgradeStatusTable)
        .from(selectStatement)
        .build();

    return connectionResources.sqlDialect().convertStatementToSQL(insertStatement);
  }


  /**
   * Creates a select statement which can be used to count the number of upgrade steps that have already been run
   */
  private SelectStatement selectUpgradeAuditTableCount() {
    TableReference upgradeAuditTable = tableRef(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME);
    return select(count(upgradeAuditTable.field("upgradeUUID")))
        .from(upgradeAuditTable)
        .build();
  }


  static class Factory implements DatabaseUpgradeLockService.Factory {

    private final UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory;

    @Inject
    Factory(UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory) {
      this.upgradeStatusTableServiceFactory = upgradeStatusTableServiceFactory;
    }


    @Override
    public DatabaseUpgradeLockService create(ConnectionResources connectionResources) {
      return new DatabaseUpgradeLockServiceImpl(connectionResources, upgradeStatusTableServiceFactory);
    }
  }

}
