/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.Statement;

/**
 * Schema change visitor which doesn't use transitional tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class InlineTableUpgrader implements SchemaChangeVisitor {

  private Schema                   currentSchema;
  private final SqlDialect         sqlDialect;
  private final SqlStatementWriter sqlStatementWriter;
  private final Table              idTable;
  private final TableNameResolver  tracker;


  /**
   * Default constructor.
   *
   * @param startSchema schema prior to upgrade step.
   * @param sqlDialect Dialect to generate statements for the target database.
   * @param sqlStatementWriter recipient for all upgrade SQL statements.
   * @param idTable table for id generation.
   */
  public InlineTableUpgrader(Schema startSchema, SqlDialect sqlDialect, SqlStatementWriter sqlStatementWriter, Table idTable) {
    this.currentSchema = startSchema;
    this.sqlDialect = sqlDialect;
    this.sqlStatementWriter = sqlStatementWriter;
    this.idTable= idTable;

    tracker = new IdTableTracker(idTable.getName());
  }


  /**
   * Perform initialisation before the main upgrade steps occur.
   */
  public void preUpgrade() {
    sqlStatementWriter.writeSql(sqlDialect.tableDeploymentStatements(idTable));
  }


  /**
   * Perform clear up after the main upgrade is completed.
   */
  public void postUpgrade() {
    sqlStatementWriter.writeSql(sqlDialect.truncateTableStatements(idTable));
    sqlStatementWriter.writeSql(sqlDialect.dropStatements(idTable));
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


  /**
   * Write out SQL
   */
  private void writeStatements(Collection<String> statements) {
    sqlStatementWriter.writeSql(statements);
  }


  /**
   * Write out SQL
   */
  private void writeStatement(String statement) {
    writeStatements(Collections.singletonList(statement));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#addAuditRecord(java.util.UUID, java.lang.String)
   */
  @Override
  public void addAuditRecord(UUID uuid, String description) {
    AuditRecordHelper.addAuditRecord(this, currentSchema, uuid, description);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#startStep(java.lang.Class)
   */
  @Override
  public void startStep(Class<? extends UpgradeStep> upgradeClass) {
    writeStatement(sqlDialect.convertCommentToSQL("Upgrade step: " + upgradeClass.getName()));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddTableFrom)
   */
  @Override
  public void visit(AddTableFrom addTableFrom) {
    currentSchema = addTableFrom.apply(currentSchema);
    writeStatements(sqlDialect.addTableFromStatements(addTableFrom.getTable(), addTableFrom.getSelectStatement()));
  }
}