package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.Criterion;

import com.google.common.collect.ImmutableList;


public class ParallelUpgradeSchemaChangeVisitor implements SchemaChangeVisitor {

  private Schema                   sourceSchema;
  private final Schema             targetSchema;
  private final SqlDialect         sqlDialect;
  private final Table              idTable;
  private final TableNameResolver  tracker;
  private final ViewChanges        viewChanges;
  private final List<String>       initScript;
  private Map<String, UpgradeNode> upgradeNodes;
  private UpgradeNode currentNode;


  /**
   * Default constructor.
   *
   * @param sourceSchema schema prior to upgrade step.
   * @param targetSchema
   * @param sqlDialect Dialect to generate statements for the target database.
   * @param sqlStatementWriter recipient for all upgrade SQL statements.
   * @param idTable table for id generation.
   * @param viewChanges
   * @param initScript
   */
  public ParallelUpgradeSchemaChangeVisitor(Schema sourceSchema, Schema targetSchema, SqlDialect sqlDialect, Table idTable,
      ViewChanges viewChanges, List<String> initScript) {
    this.sourceSchema = sourceSchema;
    this.targetSchema = targetSchema;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.viewChanges = viewChanges;
    this.initScript = initScript;

    tracker = new IdTableTracker(idTable.getName());
  }

  public List<String> preUpgrade() {
    List<String> sql = new ArrayList<>();

    // zzz table
    sql.addAll(initScript);

    // temp table
    sql.addAll(sqlDialect.tableDeploymentStatements(idTable));

    // drop views
    for (View view : viewChanges.getViewsToDrop()) {
      // non-present views can be listed amongst ViewsToDrop due to how we calculate dependencies
      if (sourceSchema.viewExists(view.getName())) {
        sql.addAll(sqlDialect.dropStatements(view));
      }
      if (sourceSchema.tableExists(DEPLOYED_VIEWS_NAME) && targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        sql.addAll(ImmutableList.of(
          sqlDialect.convertStatementToSQL(
            delete(tableRef(DEPLOYED_VIEWS_NAME)).where(Criterion.eq(field("name"), view.getName().toUpperCase()))
          )));
      }
    }

    return sql;
  }

  public List<String> postUpgrade() {
    List<String> sql = new ArrayList<>();
    // temp table drop
    sql.addAll(sqlDialect.truncateTableStatements(idTable));
    sql.addAll(sqlDialect.dropStatements(idTable));

    // recreate views
    for (View view : viewChanges.getViewsToDeploy()) {
      sql.addAll(sqlDialect.viewDeploymentStatements(view));
      if (targetSchema.tableExists(DEPLOYED_VIEWS_NAME)) {
        sql.addAll(ImmutableList.of(
          sqlDialect.convertStatementToSQL(
            insert().into(tableRef(DEPLOYED_VIEWS_NAME))
                                 .fields(literal(view.getName().toUpperCase()).as("name"),
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
            sql.addAll(ImmutableList.of(
              sqlDialect.convertCommentToSQL("Upgrades executed. Rebuilding all triggers to account for potential changes to autonumbered columns")
            ));
          }
      })
      .forEach(sql::addAll);

    return sql;
  }


  /**
   * Write out SQL
   */
  private void writeStatements(Collection<String> statements) {
    currentNode.addAllUpgradeStatements(statements);
  }


  /**
   * Write out SQL
   */
  private void writeStatement(String statement) {
    currentNode.addUpgradeStatements(statement);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddTable)
   */
  @Override
  public void visit(AddTable addTable) {
    sourceSchema = addTable.apply(sourceSchema);
    writeStatements(sqlDialect.tableDeploymentStatements(addTable.getTable()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveTable)
   */
  @Override
  public void visit(RemoveTable removeTable) {
    sourceSchema = removeTable.apply(sourceSchema);
    writeStatements(sqlDialect.dropStatements(removeTable.getTable()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddIndex)
   */
  @Override
  public void visit(AddIndex addIndex) {
    sourceSchema = addIndex.apply(sourceSchema);
    writeStatements(sqlDialect.addIndexStatements(sourceSchema.getTable(addIndex.getTableName()), addIndex.getNewIndex()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddColumn)
   */
  @Override
  public void visit(AddColumn addColumn) {
    sourceSchema = addColumn.apply(sourceSchema);
    writeStatements(sqlDialect.alterTableAddColumnStatements(sourceSchema.getTable(addColumn.getTableName()), addColumn.getNewColumnDefinition()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangeColumn)
   */
  @Override
  public void visit(ChangeColumn changeColumn) {
    sourceSchema = changeColumn.apply(sourceSchema);
    writeStatements(sqlDialect.alterTableChangeColumnStatements(sourceSchema.getTable(changeColumn.getTableName()), changeColumn.getFromColumn(), changeColumn.getToColumn()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveColumn)
   */
  @Override
  public void visit(RemoveColumn removeColumn) {
    sourceSchema = removeColumn.apply(sourceSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(sourceSchema.getTable(removeColumn.getTableName()), removeColumn.getColumnDefinition()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveIndex)
   */
  @Override
  public void visit(RemoveIndex removeIndex) {
    sourceSchema = removeIndex.apply(sourceSchema);
    writeStatements(sqlDialect.indexDropStatements(sourceSchema.getTable(removeIndex.getTableName()), removeIndex.getIndexToBeRemoved()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangeIndex)
   */
  @Override
  public void visit(ChangeIndex changeIndex) {
    sourceSchema = changeIndex.apply(sourceSchema);
    writeStatements(sqlDialect.indexDropStatements(sourceSchema.getTable(changeIndex.getTableName()), changeIndex.getFromIndex()));
    writeStatements(sqlDialect.addIndexStatements(sourceSchema.getTable(changeIndex.getTableName()), changeIndex.getToIndex()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RenameIndex)
   */
  @Override
  public void visit(final RenameIndex renameIndex) {
    sourceSchema = renameIndex.apply(sourceSchema);
    writeStatements(sqlDialect.renameIndexStatements(sourceSchema.getTable(renameIndex.getTableName()),
      renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ExecuteStatement)
   */
  @Override
  public void visit(ExecuteStatement executeStatement) {
    if (executeStatement.getStatement() instanceof PortableSqlStatement) {
      PortableSqlStatement sql = (PortableSqlStatement) executeStatement.getStatement();
      visitPortableSqlStatement(sql);
    } else {
      visitStatement(executeStatement.getStatement());
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RenameTable)
   */
  @Override
  public void visit(RenameTable renameTable) {
    Table oldTable = sourceSchema.getTable(renameTable.getOldTableName());
    sourceSchema = renameTable.apply(sourceSchema);
    Table newTable = sourceSchema.getTable(renameTable.getNewTableName());

    writeStatements(sqlDialect.renameTableStatements(oldTable, newTable));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns)
   */
  @Override
  public void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
    sourceSchema = changePrimaryKeyColumns.apply(sourceSchema);
    writeStatements(sqlDialect.changePrimaryKeyColumns(sourceSchema.getTable(changePrimaryKeyColumns.getTableName()), changePrimaryKeyColumns.getOldPrimaryKeyColumns(), changePrimaryKeyColumns.getNewPrimaryKeyColumns()));
  }


  /**
   * Write the sql statement.
   *
   * @param sql The {@link PortableSqlStatement}
   */
  private void visitPortableSqlStatement(PortableSqlStatement sql) {
    sql.inplaceUpdateTransitionalTableNames(tracker);
    writeStatement(sql.getStatement(sqlDialect.getDatabaseType().identifier(), sqlDialect.schemaNamePrefix()));
  }


  /**
   * Write the DSL statement.
   *
   * @param statement The {@link Statement}.
   */
  private void visitStatement(Statement statement) {
    writeStatements(sqlDialect.convertStatementToSQL(statement, sourceSchema, idTable));
  }


  @Override
  public void addAuditRecord(UUID uuid, String description) {
    AuditRecordHelper.addAuditRecord(this, sourceSchema, uuid, description);
  }


  @Override
  public void startStep(Class<? extends UpgradeStep> upgradeClass) {
    currentNode = upgradeNodes.get(upgradeClass.getSimpleName());
    if (currentNode == null) {
      throw new IllegalStateException("UpgradeNode: " + upgradeClass.getSimpleName() + " doesn't exist.");
    }
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddTableFrom)
   */
  @Override
  public void visit(AddTableFrom addTableFrom) {
    sourceSchema = addTableFrom.apply(sourceSchema);
    writeStatements(sqlDialect.addTableFromStatements(addTableFrom.getTable(), addTableFrom.getSelectStatement()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AnalyseTable)
   */
  @Override
  public void visit(AnalyseTable analyseTable) {
    sourceSchema = analyseTable.apply(sourceSchema);
    writeStatements(sqlDialect.getSqlForAnalyseTable(sourceSchema.getTable(analyseTable.getTableName())));
  }

  public void setUpgradeNodes(Map<String, UpgradeNode> collect) {
    this.upgradeNodes = collect;
  }

}

