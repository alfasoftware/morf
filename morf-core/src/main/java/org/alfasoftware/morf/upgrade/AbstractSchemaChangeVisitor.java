package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Statement;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema     currentSchema;
  protected SqlDialect sqlDialect;
  protected final UpgradeConfigAndContext upgradeConfigAndContext;
  protected final Table idTable;
  protected final TableNameResolver  tracker;


  public AbstractSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect,
                                     Table idTable) {
    this.currentSchema = currentSchema;
    this.upgradeConfigAndContext = upgradeConfigAndContext;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.tracker = new IdTableTracker(idTable.getName());
  }


  /**
   * Provides an entry point to write the generated sql statement to some defined place on the inheritors
   * The visit methods would add a statement by calling this.
   * @param statement the statement
   */
  protected abstract void writeStatement(String statement);

  /**
   * Provides an entry point to write the generated sql statements to some defined place on the inheritors
   * The visit methods would add a collection of statements by calling this.
   * @param statements the statements
   */
  protected abstract void writeStatements(Collection<String> statements);


  /**
   * Produce and write the DSL statement.
   *
   * @param statement The {@link Statement}.
   */
  protected void visitStatement(Statement statement) {
    writeStatements(sqlDialect.convertStatementToSQL(statement, currentSchema, idTable));
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


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddSequence)
   */
  @Override
  public void visit(AddSequence addSequence) {
    currentSchema = addSequence.apply(currentSchema);
    writeStatements(sqlDialect.sequenceDeploymentStatements(addSequence.getSequence()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RemoveSequence)
   */
  @Override
  public void visit(RemoveSequence removeSequence) {
    currentSchema = removeSequence.apply(currentSchema);
    writeStatements(sqlDialect.dropStatements(removeSequence.getSequence()));
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
   * Write the sql statement.
   *
   * @param sql The {@link PortableSqlStatement}
   */
  private void visitPortableSqlStatement(PortableSqlStatement sql) {
    sql.inplaceUpdateTransitionalTableNames(tracker);
    writeStatement(sql.getStatement(sqlDialect.getDatabaseType().identifier(), sqlDialect.schemaNamePrefix()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddIndex)
   */
  @Override
  public void visit(AddIndex addIndex) {
    currentSchema = addIndex.apply(currentSchema);
    Index foundIndex = null;
    List<Index> ignoredIndexes = upgradeConfigAndContext.getIgnoredIndexesForTable(addIndex.getTableName());
    for (Index index : ignoredIndexes) {
      if (index.columnNames().equals(addIndex.getNewIndex().columnNames())) {
        foundIndex = index;
        break;
      }
    }

    if (foundIndex != null) {
      writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(addIndex.getTableName()), foundIndex.getName(), addIndex.getNewIndex().getName()));
    } else {
      writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(addIndex.getTableName()), addIndex.getNewIndex()));
    }
  }
}
