package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

/**
 * Generates pre- and post- upgrade statements to be execute before/after the
 * Graph Based Upgrade.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class GraphBasedUpgradeScriptGenerator {

  private final Schema sourceSchema;
  private final Schema targetSchema;
  private final ConnectionResources connectionResources;
  private final Table idTable;
  private final ViewChanges viewChanges;
  private final UpgradeStatusTableService upgradeStatusTableService;
  private final Set<UpgradeScriptAddition> upgradeScriptAdditions;


  /**
   * Default constructor.
   *
   * @param sourceSchema schema prior to upgrade step
   * @param targetSchema target schema to upgrade to
   * @param connectionResources connection resources with a dialect to generate statements for the target database
   * @param idTable table for id generation
   * @param viewChanges view changes which need to be made to match the target schema
   * @param upgradeStatusTableService used to generate a script needed to update the transient "zzzUpgradeStatus" table
   */
  GraphBasedUpgradeScriptGenerator(Schema sourceSchema, Schema targetSchema, ConnectionResources connectionResources, Table idTable,
                                   ViewChanges viewChanges, UpgradeStatusTableService upgradeStatusTableService, Set<UpgradeScriptAddition> upgradeScriptAdditions) {
    this.sourceSchema = sourceSchema;
    this.targetSchema = targetSchema;
    this.connectionResources = connectionResources;
    this.idTable = idTable;
    this.viewChanges = viewChanges;
    this.upgradeStatusTableService = upgradeStatusTableService;
    this.upgradeScriptAdditions = upgradeScriptAdditions;
  }


  /**
   * @return pre-upgrade statements to be executed before the Graph Based Upgrade
   */
  public List<String> generatePreUpgradeStatements() {
    List<String> statements = new ArrayList<>();

    // zzzUpgradeStatus table
    statements.addAll(upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS));

    // temp table
    statements.addAll(connectionResources.sqlDialect().tableDeploymentStatements(idTable));

    // drop views
    for (View view : viewChanges.getViewsToDrop()) {
      // non-present views can be listed amongst ViewsToDrop due to how we calculate dependencies
      if (sourceSchema.viewExists(view.getName())) {
        statements.addAll(connectionResources.sqlDialect().dropStatements(view));
      }
      if (sourceSchema.tableExists(DEPLOYED_VIEWS_NAME) && targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        statements.addAll(ImmutableList.of(
                connectionResources.sqlDialect().convertStatementToSQL(
            delete(tableRef(DEPLOYED_VIEWS_NAME)).where(Criterion.eq(field("name"), view.getName().toUpperCase()))
          )));
      }
    }
    return statements;
  }


  /**
   * @return post-upgrade statements to be executed after the Graph Based Upgrade
   */
  public List<String> generatePostUpgradeStatements() {
    List<String> statements = new ArrayList<>();

    // temp table drop
    statements.addAll(connectionResources.sqlDialect().truncateTableStatements(idTable));
    statements.addAll(connectionResources.sqlDialect().dropStatements(idTable));

    // recreate views
    for (View view : viewChanges.getViewsToDeploy()) {
      statements.addAll(connectionResources.sqlDialect().viewDeploymentStatements(view));
      if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        statements.addAll(
                connectionResources.sqlDialect().convertStatementToSQL(
            insert().into(tableRef(DEPLOYED_VIEWS_NAME))
                                 .values(
                                   literal(view.getName().toUpperCase()).as("name"),
                                   literal(connectionResources.sqlDialect().convertStatementToHash(view.getSelectStatement())).as("hash"),
                                         connectionResources.sqlDialect().viewDeploymentStatementsAsLiteral(view).as("sqlDefinition"))
                                           ));
      }
    }

    // Since Oracle is not able to re-map schema references in trigger code, we need to rebuild all triggers
    // for id column autonumbering when exporting and importing data between environments.
    // We will drop-and-recreate triggers whenever there are upgrade steps to execute. Ideally we'd want to do
    // this step once, however there's no easy way to do that with our upgrade framework.
    AtomicBoolean first = new AtomicBoolean(true);
    targetSchema.tables().stream()
      .map(t -> connectionResources.sqlDialect().rebuildTriggers(t))
      .filter(triggerSql -> !triggerSql.isEmpty())
      .peek(triggerSql -> {
          if (first.compareAndSet(true, false)) {
            statements.addAll(ImmutableList.of(
                    connectionResources.sqlDialect().convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
            ));
          }
      })
      .forEach(statements::addAll);

    // upgrade script additions (if any)
    upgradeScriptAdditions.stream()
      .map(add -> Lists.newArrayList(add.sql(connectionResources)))
      .forEach(statements::addAll);

    // status table
    statements.addAll(upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED));

    return statements;
  }


  /**
   * Factory of {@link GraphBasedUpgradeScriptGenerator} instances.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  static class GraphBasedUpgradeScriptGeneratorFactory {
    private final UpgradeStatusTableService upgradeStatusTableService;
    private final Set<UpgradeScriptAddition> upgradeScriptAdditions;

    /**
     * Default constructor.
     *
     * @param upgradeStatusTableService used to generate a script needed to update the transient "zzzUpgradeStatus" table
     */
    @Inject
    public GraphBasedUpgradeScriptGeneratorFactory(UpgradeStatusTableService upgradeStatusTableService, Set<UpgradeScriptAddition> upgradeScriptAdditions) {
      this.upgradeStatusTableService = upgradeStatusTableService;
      this.upgradeScriptAdditions = upgradeScriptAdditions;
    }


    /**
     * Creates {@link GraphBasedUpgradeScriptGenerator}.
     *
     * @param sourceSchema schema prior to upgrade step
     * @param targetSchema target schema to upgrade to
     * @param connectionResources connection resources with a dialect to generate statements for the target database
     * @param idTable      table for id generation
     * @param viewChanges  view changes which need to be made to match the target
     *                       schema
     * @return new {@link GraphBasedUpgradeScriptGenerator} instance
     */
    GraphBasedUpgradeScriptGenerator create(Schema sourceSchema, Schema targetSchema, ConnectionResources connectionResources, Table idTable,
        ViewChanges viewChanges) {
      return new GraphBasedUpgradeScriptGenerator(sourceSchema, targetSchema, connectionResources, idTable, viewChanges,
          upgradeStatusTableService, upgradeScriptAdditions);
    }
  }
}

