package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import javax.sql.DataSource;
import java.util.Collection;

import static org.alfasoftware.morf.metadata.SchemaUtils.copy;

/**
 * Helper class to for pre and post upgrade SQL statement generation.
 * Used to ensure all statements are created equally for graph and standard upgrade mode.
 */
public final class UpgradeHelper {

    /**
     * private constructor to hide implicit public one.
     */
    private UpgradeHelper() {

    }

    /**
     * preUpgrade - generates a collection of SQL statements to run before the upgrade.
     * @param upgradeSchemas - Holds the source and target schema.
     * @param viewChanges - Changes to be made to views.
     * @param viewChangesDeploymentHelper - Deployment helper for the view changes.
     * @return - Collection of SQL Statements.
     */
    static Collection<String> preSchemaUpgrade(UpgradeSchemas upgradeSchemas,
                                               ViewChanges viewChanges,
                                               ViewChangesDeploymentHelper viewChangesDeploymentHelper) {
        ImmutableList.Builder<String> statements = ImmutableList.builder();
        final boolean deleteFromDeployedViews = upgradeSchemas.getSourceSchema().tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME)
                && upgradeSchemas.getTargetSchema().tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);

        for (View view : viewChanges.getViewsToDrop()) {
            if (upgradeSchemas.getSourceSchema().viewExists(view.getName())) {
                statements.addAll(viewChangesDeploymentHelper.dropViewIfExists(view, deleteFromDeployedViews, upgradeSchemas));
            }
            else {
                statements.addAll(viewChangesDeploymentHelper.deregisterViewIfExists(view, deleteFromDeployedViews, upgradeSchemas));
            }
        }
        return statements.build();
    }

    /**
     * postUpgrade - generates a collection of SQL statements to run after he upgrade.
     * @param targetSchema - Schema which is to be deployed.
     * @param viewChanges - Changes to be made to views.
     * @param viewChangesDeploymentHelper - Deployment helper for the view changes.
     * @return - Collection of SQL Statements.
     */
    static Collection<String> postSchemaUpgrade(Schema targetSchema,
                                                ViewChanges viewChanges,
                                                ViewChangesDeploymentHelper viewChangesDeploymentHelper) {
        ImmutableList.Builder<String> statements = ImmutableList.builder();


        final boolean insertToDeployedViews = targetSchema.tableExists(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME);
        for (View view : viewChanges.getViewsToDeploy()) {
            statements.addAll(viewChangesDeploymentHelper.createView(view, insertToDeployedViews));
        }

        return statements.build();
    }

    /**
     * Gets the source schema from the {@code database}.
     *
     * @param database the database to connect to.
     * @param dataSource the dataSource to use.
     * @param exceptionRegexes
     * @return the schema.
     */
    public static Schema copySourceSchema(ConnectionResources database, DataSource dataSource, Collection<String> exceptionRegexes) {
        SchemaResource databaseSchemaResource = database.openSchemaResource(dataSource);
        try {
            return copy(databaseSchemaResource, exceptionRegexes);
        } finally {
            databaseSchemaResource.close();
        }
    }

}
