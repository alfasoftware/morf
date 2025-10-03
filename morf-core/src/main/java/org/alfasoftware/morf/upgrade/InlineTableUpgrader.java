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

/**
 * Schema change visitor which doesn't use transitional tables.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class InlineTableUpgrader extends AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  private final SqlStatementWriter sqlStatementWriter;


  /**
   * Default constructor.
   *
   * @param startSchema schema prior to upgrade step.
   * @param upgradeConfigAndContext upgrade config
   * @param sqlDialect Dialect to generate statements for the target database.
   * @param sqlStatementWriter recipient for all upgrade SQL statements.
   * @param idTable table for id generation.
   */
  public InlineTableUpgrader(Schema startSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect, SqlStatementWriter sqlStatementWriter, Table idTable) {
    super(startSchema, upgradeConfigAndContext, sqlDialect, idTable);
    this.currentSchema = startSchema;
    this.sqlDialect = sqlDialect;
    this.sqlStatementWriter = sqlStatementWriter;
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
   * Write out SQL
   */
  @Override
  protected void writeStatements(Collection<String> statements) {
    sqlStatementWriter.writeSql(statements);
  }


  /**
   * Write out SQL
   */
  @Override
  protected void writeStatement(String statement) {
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



}