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


import java.util.UUID;

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
  void visit(AddColumn addColumn);


  /**
   * Perform visit operation on an {@link AddTable} instance.
   *
   * @param addTable instance of {@link AddTable} to visit.
   */
  void visit(AddTable addTable);


  /**
   * Perform visit operation on an {@link RemoveTable} instance.
   *
   * @param removeTable instance of {@link RemoveTable} to visit.
   */
  void visit(RemoveTable removeTable);


  /**
   * Perform visit operation on an {@link AddIndex} instance.
   *
   * @param addIndex instance of {@link AddIndex} to visit.
   */
  void visit(AddIndex addIndex);


  /**
   * Perform visit operation on an {@link ChangeColumn} instance.
   *
   * @param changeColumn instance of {@link ChangeColumn} to visit.
   */
  void visit(ChangeColumn changeColumn);


  /**
   * Perform visit operation on a {@link RemoveColumn} instance.
   *
   * @param removeColumn instance of {@link RemoveColumn} to visit.
   */
  void visit(RemoveColumn removeColumn);


  /**
   * Perform visit operation on a {@link RemoveIndex} instance.
   *
   * @param removeIndex instance of {@link RemoveIndex} to visit.
   */
  void visit(RemoveIndex removeIndex);


  /**
   * Perform visit operation on a {@link ChangeIndex} instance.
   *
   * @param changeIndex instance of {@link ChangeIndex} to visit.
   */
  void visit(ChangeIndex changeIndex);


  /**
   * Perform visit operation on a {@link RenameIndex} instance.
   *
   * @param renameIndex instance of {@link RenameIndex} to visit.
   */
  void visit(RenameIndex renameIndex);



  /**
   * Perform visit operation on a {@link ExecuteStatement} instance.
   *
   * @param executeStatement instance of {@link ExecuteStatement} to visit.
   */
  void visit(ExecuteStatement executeStatement);


  /**
   * Perform visit operation on a {@link RenameTable} instance.
   *
   * @param renameTable instance of {@link RenameTable} to visit.
   */
  void visit(RenameTable renameTable);

  /**
   * Perform visit operation on a {@link ChangePrimaryKeyColumns} instance.
   *
   * @param renameTable instance of {@link ChangePrimaryKeyColumns} to visit.
   */
  void visit(ChangePrimaryKeyColumns renameTable);


  /**
   * Perform visit operation on a {@link AddTableFrom} instance.
   *
   * @param addTableFrom instance of {@link AddTableFrom} to visit.
   */
  void visit(AddTableFrom addTableFrom);


  /**
   * Perform visit operation on a {@link AnalyseTable} instance.
   * 
   * @param analyseTable instance of {@link AnalyseTable} to visit.
   */
  void visit(AnalyseTable analyseTable);


  /**
   * Add the UUID audit record in {@link UpgradeStepStatus#SCHEDULED} status
   *
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   */
  void addAuditRecord(java.util.UUID uuid, String description);


  /**
   * Update an existing audit record to {@link UpgradeStepStatus#STARTED} status
   *
   * @param uuid The UUID of the step which has been applied
   */
  void updateStartedAuditRecord(UUID uuid);

  /**
   * Update the UUID audit record to {@link UpgradeStepStatus#SCHEDULED} status
   *
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   */
  void updateFinishedAuditRecord(UUID uuid, String description);


  /**
   * Invoked before the changes from each upgrade step are applied.
   *
   * @param upgradeClass The upgrade step being started.
   */
  void startStep(Class<? extends UpgradeStep> upgradeClass);
}
