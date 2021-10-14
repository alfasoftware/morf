package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import java.util.List;

import javax.inject.Inject;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

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
   * @return SQL statements to be run to create the view.
   */
  public List<String> createView(View view) {
    return createView(view, true);
  }


  /**
   * Creates SQL statements for creating given view.
   *
   * @param view View to be created.
   * @param updateDeloyedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to create the view.
   */
  List<String> createView(View view, boolean updateDeloyedViews) {
    Builder<String> builder = ImmutableList.builder();

    // create the view
    builder.addAll(sqlDialect.viewDeploymentStatements(view));

    // update deployed views
    if (updateDeloyedViews) {
      builder.addAll(
        sqlDialect.convertStatementToSQL(
          insert().into(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
            .values(
              literal(view.getName().toUpperCase()).as("name"),
              literal(sqlDialect.convertStatementToHash(view.getSelectStatement())).as("hash"),
              literal(sqlDialect.viewDeploymentStatementsAsScript(view)).as("sqlDefinition")
            )
        ));
    }

    // add statements from the listener
    builder.addAll(createViewListener.registerView(view));

    return builder.build();
  }


  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @return SQL statements to be run to drop the view.
   */
  public List<String> dropViewIfExists(View view) {
    return dropViewIfExists(view, true, true);
  }


  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeloyedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to drop the view.
   */
  List<String> dropViewIfExists(View view, boolean updateDeloyedViews) {
    return dropViewIfExists(view, true, updateDeloyedViews);
  }


  /**
   * Creates SQL statements for removing given view from the view register.
   *
   * @param view View to be dropped.
   * @param updateDeloyedViews Whether to update the DeployedViews table.
   * @return SQL statements to be run to drop the view.
   */
  List<String> deregisterViewIfExists(View view, boolean updateDeloyedViews) {
    return dropViewIfExists(view, false, updateDeloyedViews);
  }


  /**
   * Creates SQL statements for dropping given view.
   *
   * @param view View to be dropped.
   * @param updateDeloyedViews Whether to update the DeployedViews table.
   * @param dropTheView Whether to actually drop the view from the database.
   * @return SQL statements to be run to drop the view.
   */
  private List<String> dropViewIfExists(View view, boolean dropTheView, boolean updateDeloyedViews) {
    Builder<String> builder = ImmutableList.builder();

    // drop the view
    if (dropTheView) {
      builder.addAll(sqlDialect.dropStatements(view));
    }

    // update deployed views
    if (updateDeloyedViews) {
      builder.add(
        sqlDialect.convertStatementToSQL(
          delete(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
            .where(field("name").eq(view.getName().toUpperCase()))
        ));
    }

    // call the listener
    builder.addAll(dropViewListener.deregisterView(view));

    return builder.build();
  }


  /**
   * Creates SQL statements for removing all views from the view register.
   *
   * @return SQL statements to be run to de-register all views.
   */
  public List<String> deregisterAllViews() {
    return ImmutableList.of(sqlDialect.convertStatementToSQL(
      delete(tableRef(DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME))
    ));
  }
}
