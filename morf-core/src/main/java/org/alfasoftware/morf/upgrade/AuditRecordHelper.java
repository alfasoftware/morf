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

import static org.alfasoftware.morf.sql.element.Function.dateToYyyyMMddHHmmss;
import static org.alfasoftware.morf.sql.element.Function.now;

import java.util.UUID;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;

/**
 *  A helper class to add audit records.
 */
public class AuditRecordHelper {

  /**
   * Add the audit record, writing out the SQL for the insert.
   *
   * @see org.alfasoftware.morf.upgrade.SchemaChangeVisitor#addAuditRecord(java.util.UUID, java.lang.String)
   *
   * @param visitor The schema change visitor adding the audit record.
   * @param schema The schema to add the audit record to.
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   */
  public static void addAuditRecord(SchemaChangeVisitor visitor, Schema schema, UUID uuid, String description) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists("UpgradeAudit"))
      return;

    InsertStatement auditRecord = createAuditInsertStatement(uuid, description);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


  /**
   * Returns an {@link InsertStatement} used to be added to the upgrade audit table.
   *
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   * @return The insert statement
   */
  public static InsertStatement createAuditInsertStatement(UUID uuid, String description) {
    InsertStatement auditRecord = new InsertStatement().into(
      new TableReference("UpgradeAudit")).values(
        new FieldLiteral(uuid.toString()).as("upgradeUUID"),
        new FieldLiteral(description).as("description"),
        dateToYyyyMMddHHmmss(now()).as("appliedTime")
      );
    return auditRecord;
  }
}
