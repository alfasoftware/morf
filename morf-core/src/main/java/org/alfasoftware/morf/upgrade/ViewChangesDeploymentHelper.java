package org.alfasoftware.morf.upgrade;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import javax.inject.Inject;
import java.util.List;

import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

/**
 * View deployment helper.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class ViewChangesDeploymentHelper {

  private final SqlDialect sqlDialect;
  private final CreateViewListener createViewListener;
  private final DropViewListener dropViewListener;

  @Inject
  ViewChangesDeploymentHelper(SqlDialect sqlDialect, CreateViewListener createViewListener, DropViewListener dropViewListener) {
    this.sqlDialect = sqlDialect;
    this.createViewListener = createViewListener;
    this.dropViewListener = dropViewListener;

  }

  @VisibleForTesting
  public ViewChangesDeploymentHelper(SqlDialect sqlDialect) {
    this.sqlDialect = sqlDialect;
    this.createViewListener = new CreateViewListener.NoOp();
    this.dropViewListener = new DropViewListener.NoOp();
  }


  /**
   * Creates SQL statements for creating given view.
   *
   * @param view View to be created.
   * @param upgradeSchemas source and target schemas for upgrade.
   * @return SQL statements to be run to create the view.
   */
  public List<String> createView(View view, UpgradeSchemas upgradeSchemas) {
    return createView(view, true, upgradeSchemas);
  }

  /**
   * Creates SQL statements for creating given view.
   *
   * @param view View to be created.
   * @return SQL statements to be run to create the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  public List<String> createView(View view) {
    return createView(view, true);
  }


  /**
   * Creates SQL statements for creating given view.
   *
   * @param view View to be created.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @param upgradeSchemas contains source and target schemas for upgrade.
   * @return SQL statements to be run to create the view.
   */
  List<String> createView(View view, boolean updateDeployedViews, UpgradeSchemas upgradeSchemas) {
    Builder<String> builder = ImmutableList.builder();

    // create the view
    builder.addAll(sqlDialect.viewDeploymentStatements(view));

    // update deployed views
    if (updateDeployedViews) {
      builder.addAll(
        sqlDialect.convertStatementToSQL(
          insert().into(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
            .values(
              literal(view.getName().toUpperCase()).as("name"),
              literal(sqlDialect.convertStatementToHash(view.getSelectStatement())).as("hash"),
              sqlDialect.viewDeploymentStatementsAsLiteral(view).as("sqlDefinition")
            )
        ));
    }

    // add statements from the listener
    builder.addAll(createViewListener.registerView(view, upgradeSchemas));

    return builder.build();
  }

  /**
   * Creates SQL statements for creating given view.
   *
   * @param view View to be created.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to create the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  List<String> createView(View view, boolean updateDeployedViews) {
    return createView(view, updateDeployedViews, new UpgradeSchemas(schema(), schema()));
  }

  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param upgradeSchemas source and target schemas used for upgrade.
   * @return SQL statements to be run to drop the view.
   */
  public List<String> dropViewIfExists(View view, UpgradeSchemas upgradeSchemas) {
    return dropViewIfExists(view, true, true, upgradeSchemas);
  }

  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @return SQL statements to be run to drop the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  public List<String> dropViewIfExists(View view) {
    return dropViewIfExists(view, true, true);
  }

  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @param upgradeSchemas source and target schemas used for upgrade.
   * @return SQL statements to be run to drop the view.
   */
  List<String> dropViewIfExists(View view, boolean updateDeployedViews, UpgradeSchemas upgradeSchemas) {
    return dropViewIfExists(view, true, updateDeployedViews, upgradeSchemas);
  }

  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to drop the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  List<String> dropViewIfExists(View view, boolean updateDeployedViews) {
    return dropViewIfExists(view, true, updateDeployedViews);
  }

  /**
   * Creates SQL statements for removing given view from the view register.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @param upgradeSchemas source and target schemas used for upgrade.
   * @return SQL statements to be run to drop the view.
   */
  List<String> deregisterViewIfExists(View view, boolean updateDeployedViews, UpgradeSchemas upgradeSchemas) {
    return dropViewIfExists(view, false, updateDeployedViews, upgradeSchemas);
  }

  /**
   * Creates SQL statements for removing given view from the view register.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to drop the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  List<String> deregisterViewIfExists(View view, boolean updateDeployedViews) {
    return dropViewIfExists(view, false, updateDeployedViews);
  }


  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @param dropTheView Whether to actually drop the view from the database.
   * @param upgradeSchemas source and target schemas used for upgrade.
   * @return SQL statements to be run to drop the view.
   */
  private List<String> dropViewIfExists(View view, boolean dropTheView, boolean updateDeployedViews, UpgradeSchemas upgradeSchemas) {
    Builder<String> builder = ImmutableList.builder();

    // drop the view
    if (dropTheView) {
      builder.addAll(sqlDialect.dropStatements(view));
    }

    // update deployed views
    if (updateDeployedViews) {
      builder.add(
        sqlDialect.convertStatementToSQL(
          delete(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
            .where(field("name").eq(view.getName().toUpperCase()))
        ));
    }

    // call the listener
    builder.addAll(dropViewListener.deregisterView(view, upgradeSchemas));

    return builder.build();
  }


  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeployedViews Whether to update the DeployedViews table.
   * @param dropTheView Whether to actually drop the view from the database.
   * @return SQL statements to be run to drop the view.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  private List<String> dropViewIfExists(View view, boolean dropTheView, boolean updateDeployedViews) {
    return dropViewIfExists(view, dropTheView, updateDeployedViews, new UpgradeSchemas(schema(), schema()));
  }


  /**
   * Creates SQL statements for removing all views from the view register.
   *
   * @param upgradeSchemas source and target schemas used for upgrade.
   * @return SQL statements to be run to de-register all views.
   */
  public List<String> deregisterAllViews(UpgradeSchemas upgradeSchemas) {
    Builder<String> builder = ImmutableList.builder();

    // update deployed views
    builder.add(sqlDialect.convertStatementToSQL(
      delete(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
    ));

    // call the listener
    builder.addAll(dropViewListener.deregisterAllViews(upgradeSchemas));

    return builder.build();
  }

  /**
   * Creates SQL statements for removing all views from the view register.
   *
   * @return SQL statements to be run to de-register all views.
   * @deprecated kept to ensure backwards compatibility.
   */
  @Deprecated
  public List<String> deregisterAllViews() {
    return deregisterAllViews(new UpgradeSchemas(schema(), schema()));
  }

  /**
   * Factory that could be used to create {@link ViewChangesDeploymentHelper}s.
   *
   * @author Copyright (c) Alfa Financial Software 2022
   */
  public static class Factory  {
    private final CreateViewListener.Factory createViewListenerFactory;
    private final DropViewListener.Factory dropViewListenerFactory;

    @Inject
    public Factory(CreateViewListener.Factory createViewListenerFactory, DropViewListener.Factory dropViewListenerFactory) {
      this.createViewListenerFactory = createViewListenerFactory;
      this.dropViewListenerFactory = dropViewListenerFactory;
    }

    /**
     * Creates a {@link ViewChangesDeploymentHelper} implementation for the given connection details.
     * @param connectionResources connection resources for the data source.
     * @return ViewChangesDeploymentHelper.
     */
    public ViewChangesDeploymentHelper create(ConnectionResources connectionResources) {
      return new ViewChangesDeploymentHelper(connectionResources.sqlDialect(),
                                             createViewListenerFactory.createCreateViewListener(connectionResources),
                                             dropViewListenerFactory.createDropViewListener(connectionResources));
    }
  }
}
