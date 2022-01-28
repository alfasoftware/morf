package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.element.Criterion;

import com.google.common.collect.ImmutableList;

/**
 * Generates pre- and post- upgrade statements to be execute before/after the
 * Graph Based Upgrade.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class GraphBasedUpgradeScriptGenerator {

  private final Schema sourceSchema;
  private final Schema targetSchema;
  private final SqlDialect sqlDialect;
  private final Table idTable;
  private final ViewChanges viewChanges;
  private final List<String> initScript;

  /**
   * Default constructor.
   *
   * @param sourceSchema schema prior to upgrade step
   * @param targetSchema target schema to upgrade to
   * @param sqlDialect dialect to generate statements for the target database
   * @param idTable table for id generation
   * @param viewChanges view changes which need to be made to match the target schema
   * @param initScript script needed to update the transient "zzzUpgradeStatus" table
   */
  public GraphBasedUpgradeScriptGenerator(Schema sourceSchema, Schema targetSchema, SqlDialect sqlDialect, Table idTable,
      ViewChanges viewChanges, List<String> initScript) {
    this.sourceSchema = sourceSchema;
    this.targetSchema = targetSchema;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.viewChanges = viewChanges;
    this.initScript = initScript;
  }


  /**
   * @return pre-upgrade statements to be executed before the Graph Based Upgrade
   */
  public List<String> generatePreUpgradeStatements() {
    List<String> statements = new ArrayList<>();

    // zzzUpgradeStatus table
    statements.addAll(initScript);

    // temp table
    statements.addAll(sqlDialect.tableDeploymentStatements(idTable));

    // drop views
    for (View view : viewChanges.getViewsToDrop()) {
      // non-present views can be listed amongst ViewsToDrop due to how we calculate dependencies
      if (sourceSchema.viewExists(view.getName())) {
        statements.addAll(sqlDialect.dropStatements(view));
      }
      if (sourceSchema.tableExists(DEPLOYED_VIEWS_NAME) && targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        statements.addAll(ImmutableList.of(
          sqlDialect.convertStatementToSQL(
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
    statements.addAll(sqlDialect.truncateTableStatements(idTable));
    statements.addAll(sqlDialect.dropStatements(idTable));

    // recreate views
    for (View view : viewChanges.getViewsToDeploy()) {
      statements.addAll(sqlDialect.viewDeploymentStatements(view));
      if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        statements.addAll(ImmutableList.of(
          sqlDialect.convertStatementToSQL(
            insert().into(tableRef(DEPLOYED_VIEWS_NAME))
                                 .fields(
                                   literal(view.getName().toUpperCase()).as("name"),
                                   literal(sqlDialect.convertStatementToHash(view.getSelectStatement())).as("hash")),
                                           targetSchema)));
      }
    }

    // Since Oracle is not able to re-map schema references in trigger code, we need to rebuild all triggers
    // for id column autonumbering when exporting and importing data between environments.
    // We will drop-and-recreate triggers whenever there are upgrade steps to execute. Ideally we'd want to do
    // this step once, however there's no easy way to do that with our upgrade framework.
    AtomicBoolean first = new AtomicBoolean(true);
    targetSchema.tables().stream()
      .map(t -> sqlDialect.rebuildTriggers(t))
      .filter(triggerSql -> !triggerSql.isEmpty())
      .peek(triggerSql -> {
          if (first.compareAndSet(true, false)) {
            statements.addAll(ImmutableList.of(
              sqlDialect.convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
            ));
          }
      })
      .forEach(statements::addAll);

    return statements;
  }
}

