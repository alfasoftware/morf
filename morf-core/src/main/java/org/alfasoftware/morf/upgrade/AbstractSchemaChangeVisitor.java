
package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexSession;
import org.alfasoftware.morf.upgrade.deployedindexes.DeferredIndexTrackingPolicy;

/**
 * Common code between SchemaChangeVisitor implementors
 */
public abstract class AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  protected Schema     currentSchema;
  protected SqlDialect sqlDialect;
  protected final UpgradeConfigAndContext upgradeConfigAndContext;
  protected final Table idTable;
  protected final TableNameResolver  tracker;

  private final DeferredIndexSession deferredIndexSession;
  private final DeferredIndexTrackingPolicy trackingPolicy;


  public AbstractSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect,
                                     Table idTable, DeferredIndexSession deferredIndexSession) {
    this.currentSchema = currentSchema;
    this.upgradeConfigAndContext = upgradeConfigAndContext;
    this.sqlDialect = sqlDialect;
    this.idTable = idTable;
    this.tracker = new IdTableTracker(idTable.getName());
    this.deferredIndexSession = deferredIndexSession;
    this.trackingPolicy = new DeferredIndexTrackingPolicy(sqlDialect);
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
    Table original = addTable.getTable();
    currentSchema = addTable.apply(currentSchema);

    // Slim invariant: deferred indexes are NOT built immediately. Filter them
    // out of the CREATE TABLE statement so the adopter builds them via the
    // deferred pipeline. Track them as PENDING (same as addIndex separately).
    writeStatements(sqlDialect.tableDeploymentStatements(withoutDeferredOnSupportingDialect(original)));

    for (Index index : original.indexes()) {
      Index effective = trackingPolicy.effectiveIndex(index);
      if (trackingPolicy.shouldTrack(effective)) {
        trackInDeployedIndexes(original.getName(), effective);
      }
    }
  }


  @Override
  public void visit(RemoveTable removeTable) {
    // Remove all tracked indexes for this table
    deferredIndexSession.removeAllForTable(removeTable.getTable().getName())
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
      deferredIndexSession.updateColumnName(tableName, oldColName, newColName)
          .forEach(this::writeDeployedIndexesDml);
    }
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    String tableName = removeColumn.getTableName();
    String colName = removeColumn.getColumnDefinition().getName();

    // Remove tracked indexes referencing the column
    deferredIndexSession.removeIndexesReferencingColumn(tableName, colName)
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

    deferredIndexSession.removeIndex(tableName, indexToRemove.getName())
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
    Index toIndex = trackingPolicy.effectiveIndex(changeIndex.getToIndex());

    // Capture BEFORE the tracking/schema mutations below (see visit(RemoveIndex) note).
    boolean fromWillBePresent = willBePhysicallyPresentAtThisEmission(tableName, fromIndex.getName());

    // Always call removeIndex: the DELETE WHERE (table, index) clause is a
    // no-op if the row doesn't exist, and we want to purge any prior deferred
    // tracking row if we're changing away from a deferred index.
    deferredIndexSession.removeIndex(tableName, fromIndex.getName())
        .forEach(this::writeDeployedIndexesDml);
    currentSchema = changeIndex.apply(currentSchema);

    if (fromWillBePresent) {
      writeStatements(sqlDialect.indexDropStatements(currentSchema.getTable(tableName), fromIndex));
    }
    if (trackingPolicy.requiresImmediateBuild(toIndex)) {
      writeStatements(sqlDialect.addIndexStatements(currentSchema.getTable(tableName), toIndex));
    }
    if (trackingPolicy.shouldTrack(toIndex)) {
      trackInDeployedIndexes(tableName, toIndex);
    }
  }


  @Override
  public void visit(final RenameIndex renameIndex) {
    String tableName = renameIndex.getTableName();

    // Capture BEFORE the tracking/schema mutations below (see visit(RemoveIndex) note).
    boolean willBePresent = willBePhysicallyPresentAtThisEmission(tableName, renameIndex.getFromIndexName());

    deferredIndexSession.updateIndexName(tableName, renameIndex.getFromIndexName(), renameIndex.getToIndexName())
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
    deferredIndexSession.updateTableName(renameTable.getOldTableName(), renameTable.getNewTableName())
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
    Table original = addTableFrom.getTable();
    currentSchema = addTableFrom.apply(currentSchema);

    // Same actually-defer treatment as visit(AddTable): filter deferred-on-
    // supporting indexes out of the CTAS statement and track them as PENDING.
    writeStatements(sqlDialect.addTableFromStatements(
        withoutDeferredOnSupportingDialect(original), addTableFrom.getSelectStatement()));

    for (Index index : original.indexes()) {
      Index effective = trackingPolicy.effectiveIndex(index);
      if (trackingPolicy.shouldTrack(effective)) {
        trackInDeployedIndexes(original.getName(), effective);
      }
    }
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
    Index newIndex = trackingPolicy.effectiveIndex(addIndex.getNewIndex());

    if (trackingPolicy.requiresImmediateBuild(newIndex)) {
      emitAddIndexOrRename(tableName, newIndex);
    }
    if (trackingPolicy.shouldTrack(newIndex)) {
      trackInDeployedIndexes(tableName, newIndex);
    }
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
    deferredIndexSession.trackIndex(tableName, index)
        .forEach(this::writeDeployedIndexesDml);
  }


  /**
   * Returns a Table view of {@code original} with deferred-on-supporting-
   * dialect indexes filtered out and the remainder normalized via
   * {@link DeferredIndexTrackingPolicy#effectiveIndex}. Used at CREATE TABLE
   * (and CREATE TABLE AS SELECT) emission time so the adopter, not the
   * upgrade script, builds deferred indexes.
   *
   * @param original the table as declared by the upgrade step.
   * @return a Table preserving name, columns and isTemporary, with the
   *     index list filtered for immediate emission.
   */
  private Table withoutDeferredOnSupportingDialect(Table original) {
    List<Index> kept = new ArrayList<>();
    for (Index idx : original.indexes()) {
      Index effective = trackingPolicy.effectiveIndex(idx);
      // Skip deferred-on-supporting (adopter will build); keep everything
      // else (non-deferred + deferred-on-unsupported normalized to immediate).
      if (trackingPolicy.shouldTrack(effective)) continue;
      kept.add(effective);
    }
    TableBuilder builder = SchemaUtils.table(original.getName())
        .columns(original.columns())
        .indexes(kept);
    if (original.isTemporary()) {
      builder = builder.temporary();
    }
    return builder;
  }


  // -------------------------------------------------------------------------
  // Model helpers
  // -------------------------------------------------------------------------

  /**
   * Projects forward: will this index exist in the DB by the time the
   * generated script reaches the current emission point?
   *
   * <p>Under the "row-existence = declared deferred" model, the session
   * has the answer: an index is physically absent iff it's tracked AND its
   * status is non-terminal (declared deferred but not yet built by the
   * adopter). All other indexes — non-tracked (non-deferred physical) and
   * tracked-COMPLETED (built deferred) — are present.</p>
   *
   * @param tableName the table name.
   * @param indexName the index name.
   * @return true if the index will exist at script-emission time.
   */
  private boolean willBePhysicallyPresentAtThisEmission(String tableName, String indexName) {
    return !deferredIndexSession.isAwaitingBuild(tableName, indexName);
  }
}
