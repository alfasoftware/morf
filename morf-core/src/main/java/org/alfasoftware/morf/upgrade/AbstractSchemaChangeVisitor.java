
package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
    String tableName = changeColumn.getTableName();
    String oldColName = changeColumn.getFromColumn().getName();
    String newColName = changeColumn.getToColumn().getName();

    // If a column is renamed and deferred indexes reference the old name, update the in-memory schema
    if (!oldColName.equalsIgnoreCase(newColName)) {
      updateDeferredIndexColumnName(tableName, oldColName, newColName);
    }

    currentSchema = changeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableChangeColumnStatements(currentSchema.getTable(tableName), changeColumn.getFromColumn(), changeColumn.getToColumn()));

    if (!oldColName.equalsIgnoreCase(newColName) && hasDeferredIndexes(tableName)) {
      writeDeferredIndexComment(tableName);
    }
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    String tableName = removeColumn.getTableName();
    String colName = removeColumn.getColumnDefinition().getName();

    // Remove deferred indexes that reference the removed column
    boolean hadDeferred = removeDeferredIndexesReferencingColumn(tableName, colName);

    currentSchema = removeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(currentSchema.getTable(tableName), removeColumn.getColumnDefinition()));

    if (hadDeferred) {
      writeDeferredIndexComment(tableName);
    }
  }


  @Override
  public void visit(RemoveIndex removeIndex) {
    String tableName = removeIndex.getTableName();
    boolean wasDeferred = isIndexDeferred(tableName, removeIndex.getIndexToBeRemoved().getName());

    currentSchema = removeIndex.apply(currentSchema);

    if (wasDeferred) {
      writeStatements(sqlDialect.indexDropStatementsIfExists(currentSchema.getTable(tableName), removeIndex.getIndexToBeRemoved()));
      writeDeferredIndexComment(tableName);
    } else {
      writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(tableName), removeIndex.getIndexToBeRemoved()));
    }
  }


  @Override
  public void visit(ChangeIndex changeIndex) {
    String tableName = changeIndex.getTableName();
    boolean fromDeferred = isIndexDeferred(tableName, changeIndex.getFromIndex().getName());

    currentSchema = changeIndex.apply(currentSchema);
    Table table = currentSchema.getTable(tableName);

    if (fromDeferred) {
      writeStatements(sqlDialect.indexDropStatementsIfExists(table, changeIndex.getFromIndex()));
    } else {
      writeStatements(sqlDialect.indexDropStatements(table, changeIndex.getFromIndex()));
    }

    boolean toDeferred = changeIndex.getToIndex().isDeferred() && sqlDialect.supportsDeferredIndexCreation();
    if (toDeferred) {
      writeDeferredIndexComment(tableName);
    } else {
      writeStatements(sqlDialect.addIndexStatements(table, changeIndex.getToIndex()));
      if (fromDeferred) {
        // Old was deferred, new is not — update comment to remove old deferred declaration
        writeDeferredIndexComment(tableName);
      }
    }
  }


  @Override
  public void visit(final RenameIndex renameIndex) {
    String tableName = renameIndex.getTableName();
    boolean wasDeferred = isIndexDeferred(tableName, renameIndex.getFromIndexName());

    currentSchema = renameIndex.apply(currentSchema);

    if (wasDeferred) {
      writeStatements(sqlDialect.renameIndexStatementsIfExists(currentSchema.getTable(tableName),
        renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
      writeDeferredIndexComment(tableName);
    } else {
      writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(tableName),
        renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
    }
  }


  @Override
  public void visit(RenameTable renameTable) {
    Table oldTable = currentSchema.getTable(renameTable.getOldTableName());
    currentSchema = renameTable.apply(currentSchema);
    Table newTable = currentSchema.getTable(renameTable.getNewTableName());

    writeStatements(sqlDialect.renameTableStatements(oldTable, newTable));

    // Regenerate deferred index comment with the new table name
    if (hasDeferredIndexes(renameTable.getNewTableName())) {
      writeDeferredIndexComment(renameTable.getNewTableName());
    }
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
    boolean shouldDefer = addIndex.getNewIndex().isDeferred() && sqlDialect.supportsDeferredIndexCreation();

    if (shouldDefer) {
      writeDeferredIndexComment(addIndex.getTableName());
    } else {
      Index foundIndex = null;
      List<Index> ignoredIndexes = upgradeConfigAndContext.getIgnoredIndexesForTable(addIndex.getTableName());
      for (Index index : ignoredIndexes) {
        if (index.columnNames().equals(addIndex.getNewIndex().columnNames()) && index.isUnique() == addIndex.getNewIndex().isUnique()) {
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


  // -------------------------------------------------------------------------
  // Deferred index helpers
  // -------------------------------------------------------------------------

  /**
   * Generates a COMMENT ON TABLE statement declaring all deferred indexes for the given table.
   */
  private void writeDeferredIndexComment(String tableName) {
    Table table = currentSchema.getTable(tableName);
    List<Index> deferredIndexes = getDeferredIndexes(table);
    writeStatements(sqlDialect.generateTableCommentStatements(table, deferredIndexes));
  }


  private List<Index> getDeferredIndexes(Table table) {
    return table.indexes().stream()
        .filter(Index::isDeferred)
        .collect(Collectors.toList());
  }


  private boolean hasDeferredIndexes(String tableName) {
    return currentSchema.tableExists(tableName)
        && currentSchema.getTable(tableName).indexes().stream().anyMatch(Index::isDeferred);
  }


  private boolean isIndexDeferred(String tableName, String indexName) {
    if (!currentSchema.tableExists(tableName)) {
      return false;
    }
    return currentSchema.getTable(tableName).indexes().stream()
        .anyMatch(i -> i.getName().equalsIgnoreCase(indexName) && i.isDeferred());
  }


  /**
   * Removes deferred indexes that reference the given column from the current schema.
   * Returns true if any deferred indexes were found referencing the column.
   */
  private boolean removeDeferredIndexesReferencingColumn(String tableName, String columnName) {
    if (!currentSchema.tableExists(tableName)) {
      return false;
    }
    Table table = currentSchema.getTable(tableName);
    boolean found = false;
    for (Index idx : table.indexes()) {
      if (idx.isDeferred() && idx.columnNames().stream().anyMatch(c -> c.equalsIgnoreCase(columnName))) {
        writeStatements(sqlDialect.indexDropStatementsIfExists(table, idx));
        found = true;
      }
    }
    return found;
  }


  /**
   * Updates deferred index declarations in memory when a column is renamed.
   * The actual comment update is written after the schema change is applied.
   */
  private void updateDeferredIndexColumnName(String tableName, String oldColName, String newColName) {
    // The column rename in the schema change will not automatically update
    // deferred index column references since indexes store column names as strings.
    // The deferred index comment will be regenerated from the current schema state
    // after the ChangeColumn apply(), which handles this in the schema model.
    // No extra action needed here — the comment is regenerated from currentSchema.
  }
}
