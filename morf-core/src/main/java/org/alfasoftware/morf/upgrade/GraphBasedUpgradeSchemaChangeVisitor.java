package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Statement;

/**
 * Graph Based Upgrade implementation of the {@link SchemaChangeVisitor} which
 * is responsible for generation of all schema and data modifying statements for
 * each upgrade step.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class GraphBasedUpgradeSchemaChangeVisitor extends AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  private final Table idTable;
  private final TableNameResolver tracker;
  private final Map<String, GraphBasedUpgradeNode> upgradeNodes;
  GraphBasedUpgradeNode currentNode;


  /**
   * Default constructor.
   *
   * @param currentSchema schema prior to upgrade step.
   * @param upgradeConfigAndContext upgrade config
   * @param sqlDialect   dialect to generate statements for the target database.
   * @param idTable      table for id generation.
   * @param upgradeNodes all the {@link GraphBasedUpgradeNode} instances in the
   *                       upgrade for which the visitor will generate statements
   */
  GraphBasedUpgradeSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect, Table idTable, Map<String, GraphBasedUpgradeNode> upgradeNodes) {
    super(currentSchema, upgradeConfigAndContext, sqlDialect);
    this.currentSchema = currentSchema;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.upgradeNodes = upgradeNodes;
    tracker = new IdTableTracker(idTable.getName());
  }


  /**
   * Write statements to the current node
   */
  @Override
  protected void writeStatements(Collection<String> statements) {
    currentNode.addAllUpgradeStatements(statements);
  }


  /**
   * Write statement to the current node
   */
  private void writeStatement(String statement) {
    currentNode.addUpgradeStatements(statement);
  }


  @Override
  public void visit(AddTable addTable) {
    currentSchema = addTable.apply(currentSchema);
    writeStatements(sqlDialect.tableDeploymentStatements(addTable.getTable()));
  }


  @Override
  public void visit(RemoveTable removeTable) {
    currentSchema = removeTable.apply(currentSchema);
    writeStatements(sqlDialect.dropStatements(removeTable.getTable()));
  }


  @Override
  public void visit(AddColumn addColumn) {
    currentSchema = addColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableAddColumnStatements(currentSchema.getTable(addColumn.getTableName()), addColumn.getNewColumnDefinition()));
  }


  @Override
  public void visit(ChangeColumn changeColumn) {
    currentSchema = changeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableChangeColumnStatements(currentSchema.getTable(changeColumn.getTableName()), changeColumn.getFromColumn(), changeColumn.getToColumn()));
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    currentSchema = removeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(currentSchema.getTable(removeColumn.getTableName()), removeColumn.getColumnDefinition()));
  }


  @Override
  public void visit(RemoveIndex removeIndex) {
    currentSchema = removeIndex.apply(currentSchema);
    writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(removeIndex.getTableName()), removeIndex.getIndexToBeRemoved()));
  }


  @Override
  public void visit(ChangeIndex changeIndex) {
    currentSchema = changeIndex.apply(currentSchema);
    writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(changeIndex.getTableName()), changeIndex.getFromIndex()));
    writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(changeIndex.getTableName()), changeIndex.getToIndex()));
  }


  @Override
  public void visit(final RenameIndex renameIndex) {
    currentSchema = renameIndex.apply(currentSchema);
    writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(renameIndex.getTableName()),
      renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
  }


  @Override
  public void visit(ExecuteStatement executeStatement) {
    if (executeStatement.getStatement() instanceof PortableSqlStatement) {
      PortableSqlStatement sql = (PortableSqlStatement) executeStatement.getStatement();
      visitPortableSqlStatement(sql);
    } else {
      visitStatement(executeStatement.getStatement());
    }
  }


  @Override
  public void visit(RenameTable renameTable) {
    Table oldTable = currentSchema.getTable(renameTable.getOldTableName());
    currentSchema = renameTable.apply(currentSchema);
    Table newTable = currentSchema.getTable(renameTable.getNewTableName());

    writeStatements(sqlDialect.renameTableStatements(oldTable, newTable));
  }


  @Override
  public void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
    currentSchema = changePrimaryKeyColumns.apply(currentSchema);
    writeStatements(sqlDialect.changePrimaryKeyColumns(currentSchema.getTable(changePrimaryKeyColumns.getTableName()), changePrimaryKeyColumns.getOldPrimaryKeyColumns(), changePrimaryKeyColumns.getNewPrimaryKeyColumns()));
  }


  /**
   * Produce and write the statement based on {@link PortableSqlStatement}.
   *
   * @param sql The {@link PortableSqlStatement}
   */
  private void visitPortableSqlStatement(PortableSqlStatement sql) {
    sql.inplaceUpdateTransitionalTableNames(tracker);
    writeStatement(sql.getStatement(sqlDialect.getDatabaseType().identifier(), sqlDialect.schemaNamePrefix()));
  }


  /**
   * Produce and write the DSL statement.
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


  /**
   * Set the current {@link GraphBasedUpgradeNode} which is being processed.
   *
   * @param upgradeClass upgrade which is currently being processed
   */
  @Override
  public void startStep(Class<? extends UpgradeStep> upgradeClass) {
    currentNode = upgradeNodes.get(upgradeClass.getName());
    if (currentNode == null) {
      throw new IllegalStateException("UpgradeNode: " + upgradeClass.getName() + " doesn't exist.");
    }
  }


  @Override
  public void visit(AddTableFrom addTableFrom) {
    currentSchema = addTableFrom.apply(currentSchema);
    writeStatements(sqlDialect.addTableFromStatements(addTableFrom.getTable(), addTableFrom.getSelectStatement()));
  }


  @Override
  public void visit(AnalyseTable analyseTable) {
    currentSchema = analyseTable.apply(currentSchema);
    writeStatements(sqlDialect.getSqlForAnalyseTable(currentSchema.getTable(analyseTable.getTableName())));
  }


  /**
   * Produce and write the statement based on {@link AddSequence}.
   *
   * @param addSequence The {@link AddSequence}
   */  @Override
  public void visit(AddSequence addSequence) {
    currentSchema = addSequence.apply(currentSchema);
    writeStatements(sqlDialect.sequenceDeploymentStatements(addSequence.getSequence()));
  }


  /**
   * Produce and write the statement based on {@link RemoveSequence}.
   *
   * @param removeSequence The {@link RemoveSequence}
   */
  @Override
  public void visit(RemoveSequence removeSequence) {
    currentSchema = removeSequence.apply(currentSchema);
    writeStatements(sqlDialect.dropStatements(removeSequence.getSequence()));
  }


  /**
   * Factory of {@link GraphBasedUpgradeSchemaChangeVisitor} instances.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  static class GraphBasedUpgradeSchemaChangeVisitorFactory {

    /**
     * Creates {@link GraphBasedUpgradeSchemaChangeVisitor} instance.
     *
     * @param currentSchema schema prior to upgrade step
     * @param upgradeConfigAndContext upgrade config
     * @param sqlDialect   dialect to generate statements for the target database
     * @param idTable      table for id generation
     * @param upgradeNodes all the {@link GraphBasedUpgradeNode} instances in the upgrade for
     *                       which the visitor will generate statements
     * @return new {@link GraphBasedUpgradeSchemaChangeVisitor} instance
     */
    GraphBasedUpgradeSchemaChangeVisitor create(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect, Table idTable,
                                                Map<String, GraphBasedUpgradeNode> upgradeNodes) {
      return new GraphBasedUpgradeSchemaChangeVisitor(currentSchema, upgradeConfigAndContext, sqlDialect, idTable, upgradeNodes);
    }
  }
}

