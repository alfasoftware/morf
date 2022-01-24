package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.ResolvedTables;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.Join;

/**
 * Implementation of {@link SchemaAndDataChangeVisitor} which gathers
 * information about the tables used in a given series of schema or data change
 * commands.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class UpgradeTableResolutionVisitor implements SchemaAndDataChangeVisitor {

  ResolvedTables resolvedTables = new ResolvedTables();

  /**
   * @return tables resolved by this visitor
   */
  public ResolvedTables getResolvedTables() {
    return resolvedTables;
  }


  @Override
  public void visit(AddColumn addColumn) {
    resolvedTables.addModifiedTable(addColumn.getTableName());
  }


  @Override
  public void visit(AddTable addTable) {
    resolvedTables.addModifiedTable(addTable.getTable().getName());
  }


  @Override
  public void visit(RemoveTable removeTable) {
    resolvedTables.addModifiedTable(removeTable.getTable().getName());
  }


  @Override
  public void visit(AddIndex addIndex) {
    resolvedTables.addModifiedTable(addIndex.getTableName());
  }


  @Override
  public void visit(ChangeColumn changeColumn) {
    resolvedTables.addModifiedTable(changeColumn.getTableName());
  }


  @Override
  public void visit(RemoveColumn removeColumn) {
    resolvedTables.addModifiedTable(removeColumn.getTableName());
  }


  @Override
  public void visit(RemoveIndex removeIndex) {
    resolvedTables.addModifiedTable(removeIndex.getTableName());
  }


  @Override
  public void visit(ChangeIndex changeIndex) {
    resolvedTables.addModifiedTable(changeIndex.getTableName());
  }


  @Override
  public void visit(RenameIndex renameIndex) {
    resolvedTables.addModifiedTable(renameIndex.getTableName());
  }


  @Override
  public void visit(RenameTable renameTable) {
    resolvedTables.addModifiedTable(renameTable.getOldTableName());
    resolvedTables.addModifiedTable(renameTable.getNewTableName());
  }


  @Override
  public void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
    resolvedTables.addModifiedTable(changePrimaryKeyColumns.getTableName());
  }


  @Override
  public void visit(AddTableFrom addTableFrom) {
    resolvedTables.addModifiedTable(addTableFrom.getTable().getName());
  }


  @Override
  public void visit(AnalyseTable analyseTable) {
    resolvedTables.addModifiedTable(analyseTable.getTableName());
  }


  @Override
  public void visit(SelectFirstStatement selectFirstStatement) {
    if(selectFirstStatement.getTable() != null) {
      resolvedTables.addReadTable(selectFirstStatement.getTable().getName());
    }
  }


  @Override
  public void visit(SelectStatement selectStatement) {
    if(selectStatement.getTable() != null) {
      resolvedTables.addReadTable(selectStatement.getTable().getName());
    }
  }


  @Override
  public void visit(DeleteStatement deleteStatement) {
    resolvedTables.addModifiedTable(deleteStatement.getTable().getName());
  }


  @Override
  public void visit(InsertStatement insertStatement) {
    resolvedTables.addModifiedTable(insertStatement.getTable().getName());
    if(insertStatement.getFromTable() != null) {
      resolvedTables.addReadTable(insertStatement.getFromTable().getName());
    }
  }


  @Override
  public void visit(MergeStatement mergeStatement) {
    resolvedTables.addModifiedTable(mergeStatement.getTable().getName());
  }


  @Override
  public void visit(PortableSqlStatement portableSqlStatement) {
    resolvedTables.portableSqlStatementUsed();
  }


  @Override
  public void visit(TruncateStatement truncateStatement) {
    resolvedTables.addModifiedTable(truncateStatement.getTable().getName());
  }


  @Override
  public void visit(UpdateStatement updateStatement) {
    resolvedTables.addModifiedTable(updateStatement.getTable().getName());
  }


  @Override
  public void visit(FieldReference fieldReference) {
    if(fieldReference.getTable() != null) {
      resolvedTables.addReadTable(fieldReference.getTable().getName());
    }
  }


  @Override
  public void visit(Join join) {
    if(join.getTable() != null) {
      resolvedTables.addReadTable(join.getTable().getName());
    }
  }
}
