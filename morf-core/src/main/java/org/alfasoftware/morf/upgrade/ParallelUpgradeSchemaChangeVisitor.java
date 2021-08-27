package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Statement;


public class ParallelUpgradeSchemaChangeVisitor implements SchemaChangeVisitor {

  private Schema                   currentSchema;
  private final SqlDialect         sqlDialect;
  private final Table              idTable;
  private final TableNameResolver  tracker;
  private final Map<String, UpgradeNode> upgradeNodes;
  private UpgradeNode currentNode;


  /**
   * Default constructor.
   *
   * @param startSchema schema prior to upgrade step.
   * @param sqlDialect Dialect to generate statements for the target database.
   * @param sqlStatementWriter recipient for all upgrade SQL statements.
   * @param idTable table for id generation.
   */
  public ParallelUpgradeSchemaChangeVisitor(Schema startSchema, SqlDialect sqlDialect, Table idTable, Map<String, UpgradeNode> upgradeNodes) {
    this.currentSchema = startSchema;
    this.sqlDialect = sqlDialect;
    this.idTable= idTable;
    this.upgradeNodes = upgradeNodes;

    tracker = new IdTableTracker(idTable.getName());
  }

  public Collection<String> preUpgrade() {
    return sqlDialect.tableDeploymentStatements(idTable);
  }

  public Collection<String> postUpgrade() {
    List<String> sql = new ArrayList<>();
    sql.addAll(sqlDialect.truncateTableStatements(idTable));
    sql.addAll(sqlDialect.dropStatements(idTable));
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
    currentNode.addUpgradeStatement(statement);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddTable)
   */
  @Override
  public void visit(AddTable addTable) {
    currentSchema = addTable.apply(currentSchema);
    writeStatements(sqlDialect.tableDeploymentStatements(addTable.getTable()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveTable)
   */
  @Override
  public void visit(RemoveTable removeTable) {
    currentSchema = removeTable.apply(currentSchema);
    writeStatements(sqlDialect.dropStatements(removeTable.getTable()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddIndex)
   */
  @Override
  public void visit(AddIndex addIndex) {
    currentSchema = addIndex.apply(currentSchema);
    writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(addIndex.getTableName()), addIndex.getNewIndex()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddColumn)
   */
  @Override
  public void visit(AddColumn addColumn) {
    currentSchema = addColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableAddColumnStatements(currentSchema.getTable(addColumn.getTableName()), addColumn.getNewColumnDefinition()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangeColumn)
   */
  @Override
  public void visit(ChangeColumn changeColumn) {
    currentSchema = changeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableChangeColumnStatements(currentSchema.getTable(changeColumn.getTableName()), changeColumn.getFromColumn(), changeColumn.getToColumn()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveColumn)
   */
  @Override
  public void visit(RemoveColumn removeColumn) {
    currentSchema = removeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(currentSchema.getTable(removeColumn.getTableName()), removeColumn.getColumnDefinition()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveIndex)
   */
  @Override
  public void visit(RemoveIndex removeIndex) {
    currentSchema = removeIndex.apply(currentSchema);
    writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(removeIndex.getTableName()), removeIndex.getIndexToBeRemoved()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangeIndex)
   */
  @Override
  public void visit(ChangeIndex changeIndex) {
    currentSchema = changeIndex.apply(currentSchema);
    writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(changeIndex.getTableName()), changeIndex.getFromIndex()));
    writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(changeIndex.getTableName()), changeIndex.getToIndex()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RenameIndex)
   */
  @Override
  public void visit(final RenameIndex renameIndex) {
    currentSchema = renameIndex.apply(currentSchema);
    writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(renameIndex.getTableName()),
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
    Table oldTable = currentSchema.getTable(renameTable.getOldTableName());
    currentSchema = renameTable.apply(currentSchema);
    Table newTable = currentSchema.getTable(renameTable.getNewTableName());

    writeStatements(sqlDialect.renameTableStatements(oldTable, newTable));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.ChangePrimaryKeyColumns)
   */
  @Override
  public void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
    currentSchema = changePrimaryKeyColumns.apply(currentSchema);
    writeStatements(sqlDialect.changePrimaryKeyColumns(currentSchema.getTable(changePrimaryKeyColumns.getTableName()), changePrimaryKeyColumns.getOldPrimaryKeyColumns(), changePrimaryKeyColumns.getNewPrimaryKeyColumns()));
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
    writeStatements(sqlDialect.convertStatementToSQL(statement, currentSchema, idTable));
  }


  @Override
  public void addAuditRecord(UUID uuid, String description) {
    AuditRecordHelper.addAuditRecord(this, currentSchema, uuid, description);
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
    currentSchema = addTableFrom.apply(currentSchema);
    writeStatements(sqlDialect.addTableFromStatements(addTableFrom.getTable(), addTableFrom.getSelectStatement()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AnalyseTable)
   */
  @Override
  public void visit(AnalyseTable analyseTable) {
    currentSchema = analyseTable.apply(currentSchema);
    writeStatements(sqlDialect.getSqlForAnalyseTable(currentSchema.getTable(analyseTable.getTableName())));
  }

}

