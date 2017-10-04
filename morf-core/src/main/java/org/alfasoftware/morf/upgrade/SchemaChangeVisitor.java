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


/**
 * Interface for any upgrade / downgrade strategy which handles all the
 * defined {@link SchemaChange} implementations.
 *
 * <p>For instance different upgrade / downgrade strategies (in place / copy to new DB...),
 * different DB implementations must all handle add column, add table and the
 * other possible schema migrations.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public interface SchemaChangeVisitor {

  /**
   * Perform visit operation on an {@link AddColumn} instance.
   *
   * @param addColumn instance of {@link AddColumn} to visit.
   */
  public void visit(AddColumn addColumn);


  /**
   * Perform visit operation on an {@link AddTable} instance.
   *
   * @param addTable instance of {@link AddTable} to visit.
   */
  public void visit(AddTable addTable);


  /**
   * Perform visit operation on an {@link RemoveTable} instance.
   *
   * @param removeTable instance of {@link RemoveTable} to visit.
   */
  public void visit(RemoveTable removeTable);


  /**
   * Perform visit operation on an {@link AddIndex} instance.
   *
   * @param addIndex instance of {@link AddIndex} to visit.
   */
  public void visit(AddIndex addIndex);


  /**
   * Perform visit operation on an {@link ChangeColumn} instance.
   *
   * @param changeColumn instance of {@link ChangeColumn} to visit.
   */
  public void visit(ChangeColumn changeColumn);


  /**
   * Perform visit operation on a {@link RemoveColumn} instance.
   *
   * @param removeColumn instance of {@link RemoveColumn} to visit.
   */
  public void visit(RemoveColumn removeColumn);


  /**
   * Perform visit operation on a {@link RemoveIndex} instance.
   *
   * @param removeIndex instance of {@link RemoveIndex} to visit.
   */
  public void visit(RemoveIndex removeIndex);


  /**
   * Perform visit operation on a {@link ChangeIndex} instance.
   *
   * @param changeIndex instance of {@link ChangeIndex} to visit.
   */
  public void visit(ChangeIndex changeIndex);


  /**
   * Perform visit operation on a {@link RenameIndex} instance.
   *
   * @param renameIndex instance of {@link RenameIndex} to visit.
   */
  public void visit(RenameIndex renameIndex);



  /**
   * Perform visit operation on a {@link ExecuteStatement} instance.
   *
   * @param executeStatement instance of {@link ExecuteStatement} to visit.
   */
  public void visit(ExecuteStatement executeStatement);


  /**
   * Perform visit operation on a {@link RenameTable} instance.
   *
   * @param renameTable instance of {@link RenameTable} to visit.
   */
  public void visit(RenameTable renameTable);

  /**
   * Perform visit operation on a {@link ChangePrimaryKeyColumns} instance.
   *
   * @param renameTable instance of {@link ChangePrimaryKeyColumns} to visit.
   */
  public void visit(ChangePrimaryKeyColumns renameTable);


  /**
   * Perform visit operation on a {@link AddTableFrom} instance.
   *
   * @param addTableFrom instance of {@link AddTableFrom} to visit.
   */
  public void visit(AddTableFrom addTableFrom);


  /**
   * Perform visit operation on a {@link AnalyseTable} instance.
   * 
   * @param analyseTable instance of {@link AnalyseTable} to visit.
   */
  public void visit(AnalyseTable analyseTable);


  /**
   * Add the UUID audit record.
   *
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   */
  public void addAuditRecord(java.util.UUID uuid, String description);


  /**
   * Invoked before the changes from each upgrade step are applied.
   *
   * @param upgradeClass The upgrade step being started.
   */
  public void startStep(Class<? extends UpgradeStep> upgradeClass);
}
