package org.alfasoftware.morf.upgrade;

import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;

public class HumanReadableStatementSchemaEditor implements SchemaEditor {
  private final HumanReadableStatementConsumer consumer;

  HumanReadableStatementSchemaEditor(HumanReadableStatementConsumer consumer) {
    this.consumer = consumer;
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
   **/
  @Override
  public void addColumn(String tableName, Column definition, FieldLiteral columnDefault) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddColumnString(tableName, definition, columnDefault));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
   */
  @Override
  public void addColumn(String tableName, Column definition) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddColumnString(tableName, definition));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#addIndex(java.lang.String, org.alfasoftware.morf.metadata.Index)
   **/
  @Override
  public void addIndex(String tableName, Index index) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddIndexString(tableName, index));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#addTable(org.alfasoftware.morf.metadata.Table)
   **/
  @Override
  public void addTable(Table definition) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddTableString(definition));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#changeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
   **/
  @Override
  public void changeColumn(String tableName, Column fromDefinition, Column toDefinition) {
    consumer.schemaChange(HumanReadableStatementHelper.generateChangeColumnString(tableName, fromDefinition, toDefinition));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#changeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index, org.alfasoftware.morf.metadata.Index)
   **/
  @Override
  public void changeIndex(String tableName, Index fromIndex, Index toIndex) {
    consumer.schemaChange(HumanReadableStatementHelper.generateChangeIndexString(tableName, fromIndex, toIndex));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
   **/
  @Override
  public void removeColumn(String tableName, Column definition) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRemoveColumnString(tableName, definition));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumns(java.lang.String, org.alfasoftware.morf.metadata.Column[])
   */
  @Override
  public void removeColumns(String tableName, Column... definitions) {
    for (Column definition : definitions) {
      removeColumn(tableName, definition);
    }
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index)
   **/
  @Override
  public void removeIndex(String tableName, Index index) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRemoveIndexString(tableName, index));
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#renameIndex(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void renameIndex(String tableName, String fromIndexName, String toIndexName) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRenameIndexString(tableName, fromIndexName, toIndexName));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeTable(org.alfasoftware.morf.metadata.Table)
   **/
  @Override
  public void removeTable(Table table) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRemoveTableString(table));
  }

  @Override
  public void renameTable(String fromTableName, String toTableName) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRenameTableString(fromTableName, toTableName));
  }

  @Override
  public void changePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    consumer.schemaChange(HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, oldPrimaryKeyColumns, newPrimaryKeyColumns));
  }


  @Override
  public void correctPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns) {
    consumer.schemaChange(HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, newPrimaryKeyColumns));
  }

  @Override
  public void addTableFrom(Table table, SelectStatement select) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddTableFromString(table, select));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#analyseTable(String)
   **/
  @Override
  public void analyseTable(String tableName) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAnalyseTableFromString(tableName));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#addSequence(org.alfasoftware.morf.metadata.Sequence)
   **/
  @Override
  public void addSequence(org.alfasoftware.morf.metadata.Sequence sequence) {
    consumer.schemaChange(HumanReadableStatementHelper.generateAddSequenceString(sequence));
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeSequence(org.alfasoftware.morf.metadata.Sequence)
   **/
  @Override
  public void removeSequence(Sequence sequence) {
    consumer.schemaChange(HumanReadableStatementHelper.generateRemoveSequenceString(sequence));
  }
}

