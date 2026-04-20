
package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexState;
import org.alfasoftware.morf.upgrade.deployedindexes.DeployedIndexesService;
import org.alfasoftware.morf.upgrade.deployedindexes.IndexPresence;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema     currentSchema;
  protected SqlDialect sqlDialect;
  protected final UpgradeConfigAndContext upgradeConfigAndContext;
  protected final Table idTable;
  protected final TableNameResolver  tracker;

  private final DeployedIndexesService deployedIndexesService;
  private final DeployedIndexState deployedIndexState;


  public AbstractSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect,
                                     Table idTable, DeployedIndexState deployedIndexState,
                                     DeployedIndexesService deployedIndexesService) {
    this.currentSchema = currentSchema;
    this.upgradeConfigAndContext = upgradeConfigAndContext;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.tracker = new IdTableTracker(idTable.getName());
    this.deployedIndexState = deployedIndexState;
    this.deployedIndexesService = deployedIndexesService;
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
   * Converts and writes an INSERT against the DeployedIndexes table. Uses
   * the schema-free overload because DeployedIndexes is Morf infrastructure,
   * not part of the user schema model. Each typed overload is its own
   * compile-time entry point — new DML types (e.g. MERGE) force a new
   * overload rather than a runtime instanceof failure.
   *
   * @param s the INSERT.
   */
  private void writeDeployedIndexesDml(InsertStatement s) {
    if (!isDeployedIndexesEnabled()) {
      return;
    }
    writeStatements(sqlDialect.convertStatementToSQL(s));
  }


  /**
   * Converts and writes an UPDATE against the DeployedIndexes table.
   *
   * @param s the UPDATE.
   */
  private void writeDeployedIndexesDml(UpdateStatement s) {
    if (!isDeployedIndexesEnabled()) {
      return;
    }
    writeStatements(List.of(sqlDialect.convertStatementToSQL(s)));
  }


  /**
   * Converts and writes a DELETE against the DeployedIndexes table.
   *
   * @param s the DELETE.
   */
  private void writeDeployedIndexesDml(DeleteStatement s) {
    if (!isDeployedIndexesEnabled()) {
      return;
    }
    writeStatements(List.of(sqlDialect.convertStatementToSQL(s)));
  }


  @Override
  public void visit(AddTable addTable) {
    currentSchema = addTable.apply(currentSchema);
    writeStatements(sqlDialect.tableDeploymentStatements(addTable.getTable()));

    // Slim invariant: DeployedIndexes tracks ONLY deferred indexes. For each
    // index on the new table, first normalize against dialect support
    // (declared-deferred on an unsupported dialect becomes immediate, not
    // tracked) — then track only if the effective form is still deferred.
    for (Index index : addTable.getTable().indexes()) {
      Index effective = effectiveIndex(index);
      if (effective.isDeferred()) {
        trackInDeployedIndexes(addTable.getTable().getName(), effective);
      }
    }
  }


  @Override
  public void visit(RemoveTable removeTable) {
    // Remove all tracked indexes for this table
    deployedIndexesService.removeAllForTable(removeTable.getTable().getName())
        .forEach(this::writeDeployedIndexesDml);
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
      deployedIndexesService.updateColumnName(tableName, oldColName, newColName)
          .forEach(this::writeDeployedIndexesDml);
    }
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    String tableName = removeColumn.getTableName();
    String colName = removeColumn.getColumnDefinition().getName();

    // Remove tracked indexes referencing the column
    deployedIndexesService.removeIndexesReferencingColumn(tableName, colName)
        .forEach(this::writeDeployedIndexesDml);

    currentSchema = removeColumn.apply(currentSchema);
    writeStatements(sqlDialect.alterTableDropColumnStatements(currentSchema.getTable(tableName), removeColumn.getColumnDefinition()));
  }


  @Override
  public void visit(RemoveIndex removeIndex) {
    String tableName = removeIndex.getTableName();
    Index indexToRemove = removeIndex.getIndexToBeRemoved();

    // Capture BEFORE the tracking/schema mutations below: both
    // isTrackedDeferred and the enricher state would be out of sync by the
    // time the DDL emission runs otherwise.
    boolean willBePresent = willBePhysicallyPresentAtThisEmission(tableName, indexToRemove.getName());

    deployedIndexesService.removeIndex(tableName, indexToRemove.getName())
        .forEach(this::writeDeployedIndexesDml);

    currentSchema = removeIndex.apply(currentSchema);

    if (willBePresent) {
      writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(tableName), indexToRemove));
    }
  }


  @Override
  public void visit(ChangeIndex changeIndex) {
    String tableName = changeIndex.getTableName();
    Index fromIndex = changeIndex.getFromIndex();
    // Normalize the toIndex's deferred flag against dialect support so the
    // tracking row matches physical reality on dialects that don't support
    // deferred creation (CREATE runs immediately → nothing tracked in slim).
    Index toIndex = effectiveIndex(changeIndex.getToIndex());

    // Capture BEFORE the tracking/schema mutations below (see visit(RemoveIndex) note).
    boolean fromWillBePresent = willBePhysicallyPresentAtThisEmission(tableName, fromIndex.getName());

    // Always call removeIndex: the DELETE WHERE (table, index) clause is a
    // no-op if the row doesn't exist, and we want to purge any prior deferred
    // tracking row if we're changing away from a deferred index.
    deployedIndexesService.removeIndex(tableName, fromIndex.getName())
        .forEach(this::writeDeployedIndexesDml);
    currentSchema = changeIndex.apply(currentSchema);

    if (fromWillBePresent) {
      writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(tableName), fromIndex));
    }
    if (shouldEmitPhysicalIndexDdl(toIndex)) {
      writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(tableName), toIndex));
    }
    // Slim invariant: track only if the effective new index is deferred.
    if (toIndex.isDeferred()) {
      trackInDeployedIndexes(tableName, toIndex);
    }
  }


  @Override
  public void visit(final RenameIndex renameIndex) {
    String tableName = renameIndex.getTableName();

    // Capture BEFORE the tracking/schema mutations below (see visit(RemoveIndex) note).
    boolean willBePresent = willBePhysicallyPresentAtThisEmission(tableName, renameIndex.getFromIndexName());

    deployedIndexesService.updateIndexName(tableName, renameIndex.getFromIndexName(), renameIndex.getToIndexName())
        .forEach(this::writeDeployedIndexesDml);

    currentSchema = renameIndex.apply(currentSchema);

    if (willBePresent) {
      writeStatements(sqlDialect.renameIndexStatements(currentSchema.getTable(tableName),
          renameIndex.getFromIndexName(), renameIndex.getToIndexName()));
    }
  }


  @Override
  public void visit(RenameTable renameTable) {
    Table oldTable = currentSchema.getTable(renameTable.getOldTableName());

    // Update table name in DeployedIndexes for ALL indexes on this table
    deployedIndexesService.updateTableName(renameTable.getOldTableName(), renameTable.getNewTableName())
        .forEach(this::writeDeployedIndexesDml);

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
    String tableName = addIndex.getTableName();
    // Normalize against dialect support — see effectiveIndex Javadoc.
    Index newIndex = effectiveIndex(addIndex.getNewIndex());

    if (shouldEmitPhysicalIndexDdl(newIndex)) {
      emitAddIndexOrRename(tableName, newIndex);
    }
    // Slim invariant: track only if the effective index is deferred.
    if (newIndex.isDeferred()) {
      trackInDeployedIndexes(tableName, newIndex);
    }
  }


  /**
   * Whether a physical CREATE INDEX (or rename) should be emitted for this
   * index. Deferred indexes on dialects supporting deferred creation skip
   * the DDL (the app executes their deferred statements after the upgrade).
   *
   * @param index the index being added or changed-to.
   * @return true if physical DDL is required.
   */
  private boolean shouldEmitPhysicalIndexDdl(Index index) {
    return !(index.isDeferred() && sqlDialect.supportsDeferredIndexCreation());
  }


  /**
   * Emits CREATE INDEX for {@code newIndex}, unless the upgrade config lists
   * an ignored index with matching shape (columns + unique flag) — in which
   * case a RENAME INDEX reuses the existing physical index rather than
   * creating a new one.
   *
   * @param tableName the target table.
   * @param newIndex the index being added.
   */
  private void emitAddIndexOrRename(String tableName, Index newIndex) {
    Table table = currentSchema.getTable(tableName);
    Optional<Index> rename = findMatchingIgnoredIndex(tableName, newIndex);
    if (rename.isPresent()) {
      writeStatements(sqlDialect.renameIndexStatements(table, rename.get().getName(), newIndex.getName()));
    } else {
      writeStatements(sqlDialect.addIndexStatements(table, newIndex));
    }
  }


  /**
   * Looks for an ignored index on {@code tableName} that has the same shape
   * (columns and uniqueness) as {@code newIndex} — the rename-optimisation.
   *
   * @param tableName the table.
   * @param newIndex the index being added.
   * @return the matching ignored index if found.
   */
  private Optional<Index> findMatchingIgnoredIndex(String tableName, Index newIndex) {
    return upgradeConfigAndContext.getIgnoredIndexesForTable(tableName).stream()
        .filter(i -> i.columnNames().equals(newIndex.columnNames()) && i.isUnique() == newIndex.isUnique())
        .findFirst();
  }


  /**
   * Records the index in DeployedIndexes and emits the INSERT DML.
   *
   * @param tableName the table the index belongs to.
   * @param index the index being tracked.
   */
  private void trackInDeployedIndexes(String tableName, Index index) {
    deployedIndexesService.trackIndex(tableName, index)
        .forEach(this::writeDeployedIndexesDml);
  }


  /**
   * Returns the index as the framework will actually treat it, normalizing
   * the declared deferred flag against dialect support.
   *
   * <p>When the dialect doesn't support deferred creation
   * ({@link SqlDialect#supportsDeferredIndexCreation()} returns {@code false}),
   * an index declared {@code deferred} is effectively immediate — the visitor
   * emits {@code CREATE INDEX} at upgrade time rather than handing SQL to the
   * app-side executor. Under the slim invariant, normalizing to non-deferred
   * here means {@link #trackInDeployedIndexes} is skipped (no tracking row
   * is written), so the app-side executor sees nothing to build and cannot
   * issue a duplicate {@code CREATE INDEX}.</p>
   *
   * @param declared the index as declared in the schema.
   * @return an index whose {@code isDeferred()} reflects actual behaviour:
   *     true only if declared AND the dialect supports deferred creation.
   */
  private Index effectiveIndex(Index declared) {
    if (!declared.isDeferred() || sqlDialect.supportsDeferredIndexCreation()) {
      return declared;
    }
    SchemaUtils.IndexBuilder builder = SchemaUtils.index(declared.getName()).columns(declared.columnNames());
    if (declared.isUnique()) {
      builder = builder.unique();
    }
    return builder;
  }


  // -------------------------------------------------------------------------
  // Model helpers
  // -------------------------------------------------------------------------

  /**
   * Projects forward: will this index exist in the DB by the time the
   * generated script reaches the current emission point? Composes two
   * sources:
   *
   * <ul>
   *   <li>The at-start snapshot from the enricher ({@code deployedIndexState}).</li>
   *   <li>The in-session deltas recorded by earlier visits this run
   *       ({@code deployedIndexesService}).</li>
   * </ul>
   *
   * <p>Defaults to "present" when the state doesn't explicitly say
   * otherwise: in-session non-deferred additions are treated as present
   * (their CREATE INDEX is already queued), pre-existing non-tracked
   * indexes likewise. The name reflects the script-generation semantics —
   * nothing has hit the DB yet; this is a projection, not a query.</p>
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return true if the index will exist at script-emission time.
   */
  private boolean willBePhysicallyPresentAtThisEmission(String tableName, String indexName) {
    if (deployedIndexesService.isTrackedDeferred(tableName, indexName)) {
      return false;
    }
    return deployedIndexState.getPresence(tableName, indexName) != IndexPresence.ABSENT;
  }
}
