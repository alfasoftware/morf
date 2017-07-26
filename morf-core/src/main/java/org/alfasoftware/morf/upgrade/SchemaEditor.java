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

import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;

/**
 * API available to {@link UpgradeStep} implementors to effect database schema
 * change.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface SchemaEditor {

  /**
   * Causes an add column schema change to be added to the change sequence.
   * <p>
   * This default does <em>not</em> end up on the database column definition,
   * unlike {@link Column#getDefaultValue()}. Use this in preference to
   * having a separate {@link UpdateStatement}, if a single constant value will
   * be used for all rows.
   * </p>
   *
   * @param tableName name of table to add column to.
   * @param definition {@link Column} definition.
   * @param valueForExistingRows The value to set for the new column on
   *          existing rows.
   */
  public void addColumn(String tableName, Column definition, FieldLiteral valueForExistingRows);


  /**
   * Causes an add column schema change to be added to the change sequence.
   * <p>
   * Note that no default value will be set on existing rows using this method,
   * this will be assumed to be null. If an upgrade to existing rows is
   * required, {@link #addColumn(String, Column, FieldLiteral)} should be used
   * in preference to an additional {@link UpdateStatement}, if a single
   * constant value will be used for all rows or the column is non-nullable so a
   * default value is required prior to a more complex update.
   * </p>
   *
   * @param tableName name of table to add column to.
   * @param definition {@link Column} definition.
   */
  public void addColumn(String tableName, Column definition);


  /**
   * Causes an add table schema change to be added to the change sequence.
   *
   * @param definition {@link Table} definition to be added.
   */
  public void addTable(Table definition);


  /**
   * Causes an add table schema change to be added and the resulting table to be populated from the given {@link SelectStatement} to the change sequence.
   *
   * <p>It may be necessary to add casts to the fields of the select statement in order for the output table field definitions to be correct.</p>
   *
   * <p>The table being added must have no indexes when it is added here. They can be added subsequently.</p>
   *
   * @param table {@link Table} to be added.
   * @param select {@link SelectStatement} to populate the table.
   */
  public void addTableFrom(Table table, SelectStatement select);


  /**
   * Causes a remove table schema change to be added to the change sequence.
   *
   * @param table {@link Table} to be removed.
   */
  public void removeTable(Table table);


  /**
   * Causes change column schema change to be added to the change sequence.
   *
   * @param tableName name of table on which the column exists
   * @param fromDefinition the definition of the column to change
   * @param toDefinition the new {@link Column} definition.
   */
  public void changeColumn(String tableName, Column fromDefinition, Column toDefinition);


  /**
   * Causes a remove column schema change to be added to the change sequence.
   *
   * <p>If you have more than one column to remove, prefer: {@link #removeColumns(String, Column...)}.</p>
   *
   * @param tableName name of table on which the column exists
   * @param definition the definition of the column to remove
   */
  public void removeColumn(String tableName, Column definition);


  /**
   * Causes columns to be removed from the schema.
   *
   * <p>If you have more than one column to remove, this method gives the underlying implementations the chance to combine and optimise the removals.</p>
   *
   * @param tableName name of table on which the column exists
   * @param definitions the definition of the columns to remove
   */
  public void removeColumns(String tableName, Column... definitions);


  /**
   * Causes an add index schema change to be added to the change sequence.
   *
   * @param tableName name of table to add index to
   * @param index {@link Index} to be added
   */
  public void addIndex(String tableName, Index index);


  /**
   * Causes a remove index schema change to be added to the change sequence.
   *
   * @param tableName name of table to remove index from
   * @param index {@link Index} to be removed
   */
  public void removeIndex(String tableName, Index index);


  /**
   * Causes change index schema change to be added to the change sequence.
   *
   * @param tableName name of table on which the index exists
   * @param fromIndex the index to change
   * @param toIndex the new {@link Index} to change to
   */
  public void changeIndex(String tableName, Index fromIndex, Index toIndex);


  /**
   * Causes a rename index schema change to be added to the change sequence.
   *
   * @param tableName name of table on which the index exists
   * @param fromIndexName the name of the existing index which will be renamed
   * @param toIndexName the new name for the specified index
   */
  public void renameIndex(String tableName, String fromIndexName, String toIndexName);


  /**
   * Renames a table in the schema.
   * <p>
   * User defined indexes in domain classes will <em>not</em> be renamed by this
   * method. Any such indexes should be modified using
   * {@link #changeIndex(String, Index, Index)} to recreate each index.
   * </p>
   *
   * @param fromTableName The original table name
   * @param toTableName The new table name
   */
  public void renameTable(String fromTableName, String toTableName);


  /**
   * Change the primary key of a table.
   *
   * @param tableName The original table name
   * @param oldPrimaryKeyColumns The current/old primary key columns for the table.
   * @param newPrimaryKeyColumns The new primary key columns for the table.
   */
  public void changePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns);


  /**
   * This method is only required for one upgrade step under WEB-22563 to correct a primary key order.
   *
   * The problem causing the required correction is also fixed under the same issue so a correction of this nature will
   * no longer be required.
   *
   * @param tableName The original table name
   * @param newPrimaryKeyColumns The new primary key columns for the table.
   *
   * @deprecated This change step should never be required, use {@link #changePrimaryKeyColumns(String, List, List)}
   *             instead. This method will be removed when upgrade steps before 5.2.14 are removed.
   */
  @Deprecated
  public void correctPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns);
}
