
package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.upgrade.deferred.DeferredAddIndex;
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexesChangeService;
import org.alfasoftware.morf.upgrade.deployed.DeployedIndexesChangeServiceImpl;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema     currentSchema;
  protected SqlDialect sqlDialect;
  protected final UpgradeConfigAndContext upgradeConfigAndContext;
  protected final Table idTable;
  protected final TableNameResolver  tracker;

  private final DeployedIndexesChangeService deployedIndexesChangeService = new DeployedIndexesChangeServiceImpl();

  /** Deferred indexes collected during visitation for getDeferredIndexStatements(). */
  private final List<AddIndex> deferredIndexes = new ArrayList<>();

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


  /**
   * Whether DeployedIndexes tracking is active.
   */
  private boolean isDeployedIndexesEnabled() {
    return upgradeConfigAndContext.isDeferredIndexCreationEnabled();
  }


  /**
   * Converts and writes a DSL statement for the DeployedIndexes table DML.
   * Uses conversion without schema validation, since DeployedIndexes is
   * a Morf infrastructure table not in the user schema model.
   */
  private void visitDeployedIndexesStatement(Statement statement) {
    if (!isDeployedIndexesEnabled()) {
      return;
    }
    if (statement instanceof org.alfasoftware.morf.sql.InsertStatement) {
      writeStatements(sqlDialect.convertStatementToSQL((org.alfasoftware.morf.sql.InsertStatement) statement));
    } else if (statement instanceof org.alfasoftware.morf.sql.UpdateStatement) {
      writeStatements(List.of(sqlDialect.convertStatementToSQL((org.alfasoftware.morf.sql.UpdateStatement) statement)));
    } else if (statement instanceof org.alfasoftware.morf.sql.DeleteStatement) {
      writeStatements(List.of(sqlDialect.convertStatementToSQL((org.alfasoftware.morf.sql.DeleteStatement) statement)));
    } else {
      visitStatement(statement);
    }
  }


  @Override
  public void visit(AddTable addTable) {
    currentSchema = addTable.apply(currentSchema);
    writeStatements(sqlDialect.tableDeploymentStatements(addTable.getTable()));

    // Track all indexes on the new table in DeployedIndexes
    for (Index index : addTable.getTable().indexes()) {
      deployedIndexesChangeService.trackIndex(addTable.getTable().getName(), index, null)
          .forEach(this::visitDeployedIndexesStatement);
    }
  }


  @Override
  public void visit(RemoveTable removeTable) {
    // Remove all tracked indexes for this table
    deployedIndexesChangeService.removeAllForTable(removeTable.getTable().getName())
        .forEach(this::visitDeployedIndexesStatement);
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

    currentSchema = changeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableChangeColumnStatements(currentSchema.getTable(tableName), changeColumn.getFromColumn(), changeColumn.getToColumn()));

    // Update column references in DeployedIndexes if column was renamed
    if (!oldColName.equalsIgnoreCase(newColName)) {
      deployedIndexesChangeService.updateColumnName(tableName, oldColName, newColName)
          .forEach(this::visitDeployedIndexesStatement);
    }
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    String tableName = removeColumn.getTableName();
    String colName = removeColumn.getColumnDefinition().getName();

    // Remove tracked indexes referencing the column
    deployedIndexesChangeService.removeIndexesReferencingColumn(tableName, colName)
        .forEach(this::visitDeployedIndexesStatement);

    currentSchema = removeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(currentSchema.getTable(tableName), removeColumn.getColumnDefinition()));
  }


  @Override
  public void visit(RemoveIndex removeIndex) {
    String tableName = removeIndex.getTableName();
    Index indexToRemove = removeIndex.getIndexToBeRemoved();

    // Check if the index is physically present via the model
    boolean physicallyPresent = isPhysicallyPresent(tableName, indexToRemove.getName());

    // Remove from DeployedIndexes tracking
    deployedIndexesChangeService.removeIndex(tableName, indexToRemove.getName())
        .forEach(this::visitDeployedIndexesStatement);

    currentSchema = removeIndex.apply(currentSchema);

    // Only emit physical DDL if the index is actually in the database
    if (physicallyPresent) {
      writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(tableName), indexToRemove));
    }
  }


  @Override
  public void visit(ChangeIndex changeIndex) {
    String tableName = changeIndex.getTableName();
    Index fromIndex = changeIndex.getFromIndex();
    Index toIndex = changeIndex.getToIndex();
    boolean fromPhysicallyPresent = isPhysicallyPresent(tableName, fromIndex.getName());

    // Remove old from DeployedIndexes
    deployedIndexesChangeService.removeIndex(tableName, fromIndex.getName())
        .forEach(this::visitDeployedIndexesStatement);

    currentSchema = changeIndex.apply(currentSchema);
    Table table = currentSchema.getTable(tableName);

    // Drop old physical index if present
    if (fromPhysicallyPresent) {
      writeStatements(sqlDialect.indexDropStatements(table, fromIndex));
    }

    // Add new index: deferred or immediate
    if (toIndex.isDeferred() && sqlDialect.supportsDeferredIndexCreation()) {
      deployedIndexesChangeService.trackIndex(tableName, toIndex, null)
          .forEach(this::visitDeployedIndexesStatement);
    } else {
      writeStatements(sqlDialect.addIndexStatements(table, toIndex));
      deployedIndexesChangeService.trackIndex(tableName, toIndex, null)
          .forEach(this::visitDeployedIndexesStatement);
    }
  }


  @Override
  public void visit(final RenameIndex renameIndex) {
    String tableName = renameIndex.getTableName();
    boolean physicallyPresent = isPhysicallyPresent(tableName, renameIndex.getFromIndexName());

    // Update in DeployedIndexes
    deployedIndexesChangeService.updateIndexName(tableName, renameIndex.getFromIndexName(), renameIndex.getToIndexName())
        .forEach(this::visitDeployedIndexesStatement);

    currentSchema = renameIndex.apply(currentSchema);

    // Only emit physical DDL if the index is actually in the database
    if (physicallyPresent) {
      writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(tableName),
          renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
    }
  }


  @Override
  public void visit(RenameTable renameTable) {
    Table oldTable = currentSchema.getTable(renameTable.getOldTableName());

    // Update table name in DeployedIndexes for ALL indexes on this table
    deployedIndexesChangeService.updateTableName(renameTable.getOldTableName(), renameTable.getNewTableName())
        .forEach(this::visitDeployedIndexesStatement);

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
   * Legacy visitor method for DeferredAddIndex. Delegates to visit(AddIndex)
   * since deferred is now a property on the Index itself.
   */
  @Override
  public void visit(DeferredAddIndex deferredAddIndex) {
    visit(new AddIndex(deferredAddIndex.getTableName(), deferredAddIndex.getNewIndex()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddIndex)
   */
  @Override
  public void visit(AddIndex addIndex) {
    currentSchema = addIndex.apply(currentSchema);

    boolean shouldDefer = addIndex.getNewIndex().isDeferred() && sqlDialect.supportsDeferredIndexCreation();

    if (shouldDefer) {
      // Deferred: only track in DeployedIndexes, no physical CREATE INDEX
      deployedIndexesChangeService.trackIndex(addIndex.getTableName(), addIndex.getNewIndex(), null)
          .forEach(this::visitDeployedIndexesStatement);
      deferredIndexes.add(addIndex);
    } else {
      // Immediate: check for ignored index rename optimization, then CREATE INDEX + track
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

      deployedIndexesChangeService.trackIndex(addIndex.getTableName(), addIndex.getNewIndex(), null)
          .forEach(this::visitDeployedIndexesStatement);
    }
  }


  /**
   * Returns the deferred indexes collected during visitation.
   *
   * @return list of deferred AddIndex operations.
   */
  public List<AddIndex> getDeferredIndexes() {
    return deferredIndexes;
  }


  // -------------------------------------------------------------------------
  // Model helpers
  // -------------------------------------------------------------------------

  /**
   * Checks whether an index physically exists in the database by consulting
   * the enriched model. Returns {@code true} if the index has
   * {@code isPhysicallyPresent()=true} or if no enrichment data is available
   * (pre-DeployedIndexes state).
   */
  private boolean isPhysicallyPresent(String tableName, String indexName) {
    if (!currentSchema.tableExists(tableName)) {
      return false;
    }
    return currentSchema.getTable(tableName).indexes().stream()
        .filter(i -> i.getName().equalsIgnoreCase(indexName))
        .findFirst()
        .map(Index::isPhysicallyPresent)
        .orElse(false);
  }
}
