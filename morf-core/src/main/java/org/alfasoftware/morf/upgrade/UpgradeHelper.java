package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
                                                ViewChangesDeploymentHelper viewChangesDeploymentHelper) {
        List<String> upgrades = new ArrayList<>();

        final boolean insertToDeployedViews = targetSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);
        for (View view : viewChanges.getViewsToDeploy()) {
            upgrades.addAll(viewChangesDeploymentHelper.createView(view, insertToDeployedViews));
        }

        return upgrades;
    }

}
