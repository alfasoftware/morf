package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.alfasoftware.morf.sql.SqlUtils.*;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

/**
 * Helper class to for pre and post upgrade SQL statement generation.
 * Used to ensure all statements are created equally for graph and standard upgrade mode.
 */
class UpgradeHelper {

    /**
     * preUpgrade - generates a collection of SQL statements to run before the upgrade.
     * @param sourceSchema - The current schema.
     * @param targetSchema - Schema which is to be deployed.
     * @param viewChanges - Changes to be made to views.
     * @param viewChangesDeploymentHelper - Deployment helper for the view changes.
     * @return - Collection of SQL Statements.
     */
    static Collection<String> preSchemaUpgrade(Schema sourceSchema,
                                               Schema targetSchema,
                                               ViewChanges viewChanges,
                                               ViewChangesDeploymentHelper viewChangesDeploymentHelper) {
        List<String> upgrades = new ArrayList<>();
        final boolean deleteFromDeployedViews = sourceSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME) && targetSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);
        for (View view : viewChanges.getViewsToDrop()) {
            if (sourceSchema.viewExists(view.getName())) {
                upgrades.addAll(viewChangesDeploymentHelper.dropViewIfExists(view, deleteFromDeployedViews));
            }
            else {
                upgrades.addAll(viewChangesDeploymentHelper.deregisterViewIfExists(view, deleteFromDeployedViews));
            }
        }
        return upgrades;
    }

    /**
     * postUpgrade - generates a collection of SQL statements to run after he upgrade.
     * @param targetSchema - Schema which is to be deployed.
     * @param viewChanges - Changes to be made to views.
     * @param viewChangesDeploymentHelper - Deployment helper for the view changes.
     * @param upgradesToApply - upgrades which are to be applied to schema.
     * @param connectionResources - connection resources to connect to the database.
     * @return - Collection of SQL Statements.
     */
    static Collection<String> postSchemaUpgrade(Schema targetSchema,
                                                ViewChanges viewChanges,
                                                ViewChangesDeploymentHelper viewChangesDeploymentHelper,
                                                List<UpgradeStep> upgradesToApply,
                                                ConnectionResources connectionResources,
                                                Table idTable) {
        List<String> upgrades = new ArrayList<>();

        // temp table drop
        upgrades.addAll(connectionResources.sqlDialect().truncateTableStatements(idTable));
        upgrades.addAll(connectionResources.sqlDialect().dropStatements(idTable));

        final boolean insertToDeployedViews = targetSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);
        for (View view : viewChanges.getViewsToDeploy()) {
            upgrades.addAll(viewChangesDeploymentHelper.createView(view, insertToDeployedViews));
        }

        // Since Oracle is not able to re-map schema references in trigger code, we need to rebuild all triggers
        // for id column autonumbering when exporting and importing data between environments.
        // We will drop-and-recreate triggers whenever there are upgrade steps to execute. Ideally we'd want to do
        // this step once, however there's no easy way to do that with our upgrade framework.
        if (!upgradesToApply.isEmpty()) {

            AtomicBoolean first = new AtomicBoolean(true);
            targetSchema.tables().stream()
                    .map(t -> connectionResources.sqlDialect().rebuildTriggers(t))
                    .filter(sql -> !sql.isEmpty())
                    .peek(sql -> {
                        if (first.compareAndSet(true, false)) {
                            upgrades.addAll(ImmutableList.of(
                                    connectionResources.sqlDialect().convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
                            ));
                        }
                    })
                    .forEach(upgrades::addAll);
        }
        return upgrades;
    }


    /**
     * postUpgrade - generates a collection of SQL statements to run after he upgrade.
     * @param targetSchema - Schema which is to be deployed.
     * @param viewChanges - Changes to be made to views.
     * @param viewChangesDeploymentHelper - Deployment helper for the view changes.
     * @param upgradesToApply - upgrades which are to be applied to schema.
     * @param connectionResources - connection resources to connect to the database.
     * @return - Collection of SQL Statements.
     */
    static Collection<String> postGraphSchemaUpgrade(Schema targetSchema,
                                                ViewChanges viewChanges,
                                                ViewChangesDeploymentHelper viewChangesDeploymentHelper,
                                                List<UpgradeStep> upgradesToApply,
                                                ConnectionResources connectionResources,
                                                Table idTable) {
        List<String> upgrades = new ArrayList<>();

        // temp table drop
        upgrades.addAll(connectionResources.sqlDialect().truncateTableStatements(idTable));
        upgrades.addAll(connectionResources.sqlDialect().dropStatements(idTable));

        // recreate views
        for (View view : viewChanges.getViewsToDeploy()) {
            upgrades.addAll(connectionResources.sqlDialect().viewDeploymentStatements(view));
            if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
                upgrades.addAll(
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
                        upgrades.addAll(ImmutableList.of(
                                connectionResources.sqlDialect().convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
                        ));
                    }
                })
                .forEach(upgrades::addAll);
        return upgrades;
    }
}
