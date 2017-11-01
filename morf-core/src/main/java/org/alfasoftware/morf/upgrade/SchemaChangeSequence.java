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

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils.ColumnBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.FieldLiteral;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Tracks a sequence of {@link SchemaChange}s as various {@link SchemaEditor}
 * methods are called to specify the database schema changes required for an
 * upgrade.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SchemaChangeSequence {

  private final List<UpgradeStep>            upgradeSteps;

  private final Set<String> tableAdditions = new HashSet<>();

  private final List<UpgradeStepWithChanges> allChanges     = Lists.newArrayList();


  /**
   * @param steps the upgrade steps
   */
  public SchemaChangeSequence(List<UpgradeStep> steps) {
    upgradeSteps = steps;

    for (UpgradeStep step : steps) {
      InternalVisitor internalVisitor = new InternalVisitor();
      Editor editor = new Editor(internalVisitor);
      // For historical reasons, we need to pass the editor in twice
      step.execute(editor, editor);
      allChanges.add(new UpgradeStepWithChanges(step, internalVisitor.getChanges()));
    }
  }


  /**
   * Applies the changes to the given schema.
   *
   * @param initialSchema The schema to apply changes to.
   * @return the resulting schema after applying changes in this sequence
   */
  public Schema applyToSchema(Schema initialSchema) {
    Schema currentSchema = initialSchema;
    for (UpgradeStepWithChanges changesForStep : allChanges) {
      for (SchemaChange change : changesForStep.getChanges()) {
        try {
          currentSchema = change.apply(currentSchema);
        } catch (RuntimeException rte) {
          throw new RuntimeException("Failed to apply change [" + change + "] from upgrade step " + changesForStep.getUpgradeClass(), rte);
        }
      }
    }
    return currentSchema;
  }


  /**
   * Applies the change reversals to the given schema.
   *
   * @param initialSchema The schema to apply changes to.
   * @return the resulting schema after applying reverse changes in this sequence
   */
  public Schema applyInReverseToSchema(Schema initialSchema) {
    Schema currentSchema = initialSchema;

    // we need to reverse the order of the changes inside the step before we try to reverse-execute them
    for (UpgradeStepWithChanges changesForStep : Lists.reverse(allChanges)) {
      for (SchemaChange change : Lists.reverse(changesForStep.getChanges())) {
        try {
          currentSchema = change.reverse(currentSchema);
        } catch (RuntimeException rte) {
          throw new RuntimeException("Failed to reverse-apply change [" + change + "] from upgrade step " + changesForStep.getUpgradeClass(), rte);
        }
      }
    }

    return currentSchema;
  }


  /**
   * @return the upgradeSteps
   */
  public List<UpgradeStep> getUpgradeSteps() {
    return ImmutableList.copyOf(upgradeSteps);
  }


  /**
   * @param visitor The schema change visitor against which to write the changes.
   */
  public void applyTo(SchemaChangeVisitor visitor) {
    for (UpgradeStepWithChanges changesForStep : allChanges) {
      try {
        visitor.startStep(changesForStep.getUpgradeClass());
        for (SchemaChange change : changesForStep.getChanges()) {
          change.accept(visitor);
        }
        visitor.addAuditRecord(changesForStep.getUUID(), changesForStep.getDescription());
      } catch (Exception e) {
        throw new RuntimeException("Failed to apply step: [" + changesForStep.getUpgradeClass() + "]", e);
      }
    }
  }


  /**
   * @return The set of all table which are added by this sequence.
   */
  public Set<String> tableAdditions() {
    return tableAdditions;
  }


  /**
   * The editor implementation which is used by upgrade steps
   */
  private class Editor implements SchemaEditor, DataEditor {

    private final SchemaChangeVisitor visitor;

    /**
     * @param visitor The visitor to pass the changes to.
     */
    Editor(SchemaChangeVisitor visitor) {
      super();
      this.visitor = visitor;
    }


    /**
     * @see org.alfasoftware.morf.upgrade.DataEditor#executeStatement(org.alfasoftware.morf.sql.Statement)
     */
    @Override
    public void executeStatement(Statement statement) {
      visitor.visit(new ExecuteStatement(statement));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
     */
    @Override
    public void addColumn(String tableName, Column definition, FieldLiteral defaultValue) {
      // create a new Column with the default and add this first
      ColumnBuilder temporaryDefinitionWithDefault = column(definition.getName(), definition.getType(), definition.getWidth(), definition.getScale()).defaultValue(defaultValue.getValue());
      temporaryDefinitionWithDefault = definition.isNullable() ? temporaryDefinitionWithDefault.nullable() : temporaryDefinitionWithDefault;
      temporaryDefinitionWithDefault = definition.isPrimaryKey() ? temporaryDefinitionWithDefault.primaryKey() : temporaryDefinitionWithDefault;
      addColumn(tableName, temporaryDefinitionWithDefault);

      // now move to the final column definition, which may not have the default value.
      changeColumn(tableName, temporaryDefinitionWithDefault, definition);
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
     */
    @Override
    public void addColumn(String tableName, Column definition) {
      visitor.visit(new AddColumn(tableName, definition));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#addTable(org.alfasoftware.morf.metadata.Table)
     */
    @Override
    public void addTable(Table definition) {
      // track added tables...
      tableAdditions.add(definition.getName());

      visitor.visit(new AddTable(definition));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeTable(org.alfasoftware.morf.metadata.Table)
     */
    @Override
    public void removeTable(Table table) {
      visitor.visit(new RemoveTable(table));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#changeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column)
     */
    @Override
    public void changeColumn(String tableName, Column fromDefinition, Column toDefinition) {
      visitor.visit(new ChangeColumn(tableName, fromDefinition, toDefinition));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
     */
    @Override
    public void removeColumn(String tableName, Column definition) {
      visitor.visit(new RemoveColumn(tableName, definition));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumns(java.lang.String, org.alfasoftware.morf.metadata.Column[])
     */
    @Override
    public void removeColumns(String tableName, Column... definitions) {
      // simple redirect for now, but a future optimisation could re-implement this to be more efficient
      for (Column definition : definitions) {
        removeColumn(tableName, definition);
      }
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#addIndex(java.lang.String, org.alfasoftware.morf.metadata.Index)
     */
    @Override
    public void addIndex(String tableName, Index index) {
      visitor.visit(new AddIndex(tableName, index));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index)
     */
    @Override
    public void removeIndex(String tableName, Index index) {
      visitor.visit(new RemoveIndex(tableName, index));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#changeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index, org.alfasoftware.morf.metadata.Index)
     */
    @Override
    public void changeIndex(String tableName, Index fromIndex, Index toIndex) {
      visitor.visit(new ChangeIndex(tableName, fromIndex, toIndex));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#renameIndex(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void renameIndex(String tableName, String fromIndexName, String toIndexName) {
      visitor.visit(new RenameIndex(tableName, fromIndexName, toIndexName));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#renameTable(java.lang.String, java.lang.String)
     */
    @Override
    public void renameTable(String fromTableName, String toTableName) {
      tableAdditions.add(toTableName);
      visitor.visit(new RenameTable(fromTableName, toTableName));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#changePrimaryKeyColumns(java.lang.String, java.util.List, java.util.List)
     */
    @Override
    public void changePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
      visitor.visit(new ChangePrimaryKeyColumns(tableName, oldPrimaryKeyColumns, newPrimaryKeyColumns));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#correctPrimaryKeyColumns(java.lang.String, java.util.List)
     * @deprecated This change step should never be required, use {@link #changePrimaryKeyColumns(String, List, List)}
     *  instead. This method will be removed when upgrade steps before 5.2.14 are removed.
     */
    @Override
    @Deprecated
    public void correctPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns) {
      visitor.visit(new CorrectPrimaryKeyColumns(tableName, newPrimaryKeyColumns));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#addTableFrom(org.alfasoftware.morf.metadata.Table, org.alfasoftware.morf.sql.SelectStatement)
     */
    @Override
    public void addTableFrom(Table table, SelectStatement select) {
      // track added tables...
      tableAdditions.add(table.getName());

      visitor.visit(new AddTableFrom(table, select));
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaEditor#analyseTable(org.alfasoftware.morf.metadata.Table)
     */
    @Override
    public void analyseTable(String tableName) {
      visitor.visit(new AnalyseTable(tableName));
    }
  }


  /**
   * Encapsulates an {@link UpgradeStep} and the list of {@link SchemaChange} it generates.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  private static class UpgradeStepWithChanges {
    private final UpgradeStep delegate;
    private final List<SchemaChange> changes;


    /**
     * @param delegate
     * @param changes
     */
    UpgradeStepWithChanges(UpgradeStep delegate, List<SchemaChange> changes) {
      super();
      this.delegate = delegate;
      this.changes = changes;
    }

    public Class<? extends UpgradeStep> getUpgradeClass() {
      return delegate.getClass();
    }


    public String getDescription() {
      return delegate.getDescription();
    }


    /**
     * @return the changes
     */
    public List<SchemaChange> getChanges() {
      return changes;
    }


    public java.util.UUID getUUID() {
      return java.util.UUID.fromString(delegate.getClass().getAnnotation(UUID.class).value());
    }
  }


  /**
   * SchemaChangeVisitor which redirects the calls onto the apply() method.
   */
  private static class InternalVisitor implements SchemaChangeVisitor {

    private final List<SchemaChange> changes = Lists.newArrayList();


    /**
     * @return the changes
     */
    public List<SchemaChange> getChanges() {
      return changes;
    }


    @Override
    public void visit(ExecuteStatement executeStatement) {
      changes.add(executeStatement);
    }


    @Override
    public void visit(ChangeIndex changeIndex) {
      changes.add(changeIndex);
    }


    @Override
    public void visit(RemoveIndex removeIndex) {
      changes.add(removeIndex);
    }


    @Override
    public void visit(RemoveColumn removeColumn) {
      changes.add(removeColumn);
    }


    @Override
    public void visit(ChangeColumn changeColumn) {
      changes.add(changeColumn);
    }


    @Override
    public void visit(AddIndex addIndex) {
      changes.add(addIndex);
    }


    @Override
    public void visit(RemoveTable removeTable) {
      changes.add(removeTable);
    }


    @Override
    public void visit(AddTable addTable) {
      changes.add(addTable);
    }


    @Override
    public void visit(AddColumn addColumn) {
      changes.add(addColumn);
    }


    @Override
    public void addAuditRecord(java.util.UUID uuid, String description) {
      // no-op here. We don't need to record the UUIDs until we actually apply the changes.
    }


    @Override
    public void startStep(Class<? extends UpgradeStep> upgradeClass) {
      // no-op here. We don't care what step is running
    }


    @Override
    public void visit(RenameTable renameTable) {
      changes.add(renameTable);
    }


    @Override
    public void visit(ChangePrimaryKeyColumns changePrimaryKeyColumns) {
      changes.add(changePrimaryKeyColumns);
    }

    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.RenameIndex)
     */
    @Override
    public void visit(RenameIndex renameIndex) {
      changes.add(renameIndex);
    }


    /**
     * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#visit(org.alfasoftware.morf.upgrade.AddTableFrom)
     */
    @Override
    public void visit(AddTableFrom addTableFrom) {
      changes.add(addTableFrom);
    }


    @Override
    public void visit(AnalyseTable analyseTable) {
      changes.add(analyseTable);
    }
  }
}
