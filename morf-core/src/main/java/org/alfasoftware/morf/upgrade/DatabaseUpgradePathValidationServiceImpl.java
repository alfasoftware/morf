package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Criterion.and;
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
 * Implementation of {@link DatabaseUpgradePathValidationService}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
public class DatabaseUpgradePathValidationServiceImpl implements DatabaseUpgradePathValidationService {

  private final ConnectionResources connectionResources;
  private final UpgradeStatusTableService upgradeStatusTableService;

  /**
   * Used for direct injection
   */
  @Inject
  DatabaseUpgradePathValidationServiceImpl(ConnectionResources connectionResources,
                                           UpgradeStatusTableService upgradeStatusTableService) {
    this.connectionResources = connectionResources;
    this.upgradeStatusTableService = upgradeStatusTableService;
  }

  /**
   * Used by {@link DatabaseUpgradePathValidationService.Factory}
   */
  DatabaseUpgradePathValidationServiceImpl(ConnectionResources connectionResources,
                                           UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory) {
    this.connectionResources = connectionResources;
    this.upgradeStatusTableService = upgradeStatusTableServiceFactory.create(connectionResources);
  }


  @Override
  public List<String> getPathValidationSql(long upgradeAuditCount) {
    ImmutableList.Builder<String> statements = ImmutableList.builder();
    statements.addAll(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS));
    statements.addAll(getOptimisticLockingInitialisationSql(upgradeAuditCount));
    return statements.build();
  }


  /**
   * Generates SQL to be run at the start of the upgrade script which ensures the upgrade can't be run twice.
   * If the upgradeAuditCount supplied is -1 then the optimistic locking will not be executed, as this means we
   * were unable to connect to the UpgradeAudit table to read the count prior to generating the script
   */
  private List<String> getOptimisticLockingInitialisationSql(long upgradeAuditCount) {
    TableReference upgradeStatusTable = tableRef(UpgradeStatusTableService.UPGRADE_STATUS);

    SelectStatement selectStatement = select().from(upgradeStatusTable)
      .where(and(neq(selectUpgradeAuditTableCount().asField(), upgradeAuditCount), neq(literal(upgradeAuditCount), -1)))
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


  static class Factory implements DatabaseUpgradePathValidationService.Factory {

    private final UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory;

    @Inject
    Factory(UpgradeStatusTableService.Factory upgradeStatusTableServiceFactory) {
      this.upgradeStatusTableServiceFactory = upgradeStatusTableServiceFactory;
    }


    @Override
    public DatabaseUpgradePathValidationService create(ConnectionResources connectionResources) {
      return new DatabaseUpgradePathValidationServiceImpl(connectionResources, upgradeStatusTableServiceFactory);
    }
  }

}
