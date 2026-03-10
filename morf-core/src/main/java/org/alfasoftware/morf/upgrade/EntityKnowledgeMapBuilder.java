package org.alfasoftware.morf.upgrade;

import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.FieldLiteral;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class EntityKnowledgeMapBuilder implements HumanReadableStatementConsumer, SchemaEditor, DataEditor {
  private final String preferredSQLDialect;
  private EntityKnowledgeMapUpgradeStep currentUpgradeStep;

  // tree map so entities are stored in alphabetical order
  private final Map<String, Map<EntityKnowledgeMapUpgradeStep, List<String>>> knowledgeMap = Maps.newTreeMap(String::compareTo);


  public EntityKnowledgeMapBuilder(String preferredSQLDialect) {
    this.preferredSQLDialect = preferredSQLDialect;
  }

  public Map<String, Map<EntityKnowledgeMapUpgradeStep, List<String>>> getKnowledgeMultimap() {
    return knowledgeMap;
  }

    private void putInMap(String tableName, String schemaChangeDescription) {
      // linked hashmap so we maintain insert order of upgrade steps
      knowledgeMap.computeIfAbsent(tableName, k -> Maps.newLinkedHashMap())
        .computeIfAbsent(currentUpgradeStep, k -> Lists.newArrayList())
        .add(schemaChangeDescription);
  }

  //----------------------------------------------------------------
  //HumanReadableStatementConsumer Overrides
  //----------------------------------------------------------------
  /**
   * @see HumanReadableStatementConsumer#versionStart(String) 
   */
  @Override
  public void versionStart(String versionNumber) {
    //nothing to write
  }

  /**
   * @see HumanReadableStatementConsumer#upgradeStepStart(String, String, String) 
   */
  @Override
  public void upgradeStepStart(String name, String description, String jiraId) {
    currentUpgradeStep = new EntityKnowledgeMapUpgradeStep(name, description, jiraId);
  }

  /**
   * @see HumanReadableStatementConsumer#schemaChange(String)
   */
  @Override
  public void schemaChange(String description) {
    //nothing to write
  }

  /**
   * @see HumanReadableStatementConsumer#upgradeStepEnd(String)
   */
  @Override
  public void upgradeStepEnd(String name) {
    //nothing to write
  }

  /**
   * @see HumanReadableStatementConsumer#versionEnd(String)
   */
  @Override
  public void versionEnd(String versionNumber) {
    //nothing to write
  }

  /**
   * @see HumanReadableStatementConsumer#dataChange(String)
   */
  @Override
  public void dataChange(String description) {
    //nothing to write
  }

  //----------------------------------------------------------------
  //SchemaEditor Overrides
  //----------------------------------------------------------------
  /**
   * @see SchemaEditor#addColumn(String, Column)
   **/
  @Override
  public void addColumn(String tableName, Column definition, FieldLiteral columnDefault) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateAddColumnString(tableName, definition, columnDefault));
    }
  }



  /**
   * @see SchemaEditor#addColumn(String, Column)
   */
  @Override
  public void addColumn(String tableName, Column definition) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateAddColumnString(tableName, definition));
    }

  }

  /**
   * @see SchemaEditor#addIndex(String, Index)
   **/
  @Override
  public void addIndex(String tableName, Index index) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateAddIndexString(tableName, index));
    }

  }

  /**
   * @see SchemaEditor#addTable(Table)
   **/
  @Override
  public void addTable(Table definition) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(definition.getName(), HumanReadableStatementHelper.generateAddTableString(definition));
    }

  }

  /**
   * @see SchemaEditor#changeColumn(String, Column, Column)
   **/
  @Override
  public void changeColumn(String tableName, Column fromDefinition, Column toDefinition) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateChangeColumnString(tableName, fromDefinition, toDefinition));
    }

  }

  /**
   * @see SchemaEditor#changeIndex(String, Index, Index)
   **/
  @Override
  public void changeIndex(String tableName, Index fromIndex, Index toIndex) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateChangeIndexString(tableName, fromIndex, toIndex));
    }

  }

  /**
   * @see SchemaEditor#removeColumn(String, Column)
   **/
  @Override
  public void removeColumn(String tableName, Column definition) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateRemoveColumnString(tableName, definition));
    }

  }

  /**
   * @see SchemaEditor#removeColumns(String, Column[])
   */
  @Override
  public void removeColumns(String tableName, Column... definitions) {
    for (Column definition : definitions) {
      removeColumn(tableName, definition);
    }
  }

  /**
   * @see SchemaEditor#removeIndex(String, Index)
   **/
  @Override
  public void removeIndex(String tableName, Index index) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateRemoveIndexString(tableName, index));
    }

  }


  /**
   * @see SchemaEditor#renameIndex(String, String, String)
   */
  @Override
  public void renameIndex(String tableName, String fromIndexName, String toIndexName) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateRenameIndexString(tableName, fromIndexName, toIndexName));
    }

  }

  /**
   * @see SchemaEditor#removeTable(Table)
   **/
  @Override
  public void removeTable(Table table) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(table.getName(), HumanReadableStatementHelper.generateRemoveTableString(table));
    }

  }

  @Override
  public void renameTable(String fromTableName, String toTableName) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(fromTableName, HumanReadableStatementHelper.generateRenameTableString(fromTableName, toTableName));
    }

  }

  @Override
  public void changePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, oldPrimaryKeyColumns, newPrimaryKeyColumns));
    }

  }


  @Override
  public void correctPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, newPrimaryKeyColumns));
    }

  }

  @Override
  public void addTableFrom(Table table, SelectStatement select) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(table.getName(), HumanReadableStatementHelper.generateAddTableFromString(table, select));
    }
  }

  /**
   * @see SchemaEditor#analyseTable(String)
   **/
  @Override
  public void analyseTable(String tableName) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(tableName, HumanReadableStatementHelper.generateAnalyseTableFromString(tableName));
    }
  }

  /**
   * @see SchemaEditor#addSequence(Sequence)
   **/
  @Override
  public void addSequence(Sequence sequence) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(sequence.getName(), HumanReadableStatementHelper.generateAddSequenceString(sequence));
    }
  }

  /**
   * @see SchemaEditor#removeSequence(Sequence)
   **/
  @Override
  public void removeSequence(Sequence sequence) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(sequence.getName(), HumanReadableStatementHelper.generateRemoveSequenceString(sequence));
    }
  }

  //----------------------------------------------------------------
  //DataEditor Overrides
  //----------------------------------------------------------------

  /**
   * Causes execute statement schema change to be added to the change sequence.
   *
   * @param statement the {@link Statement} to execute
   */
  @Override
  public void executeStatement(Statement statement) {
    if(currentUpgradeStep.isPopulated()) {
      putInMap(HumanReadableStatementHelper.dataUpgradeTableName(statement),
          HumanReadableStatementHelper.generateDataUpgradeString(statement, preferredSQLDialect));
    }
  }

}
