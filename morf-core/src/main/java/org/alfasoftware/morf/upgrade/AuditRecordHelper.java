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

import static org.alfasoftware.morf.sql.SqlUtils.cast;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Function.addDays;
import static org.alfasoftware.morf.sql.element.Function.dateToYyyyMMddHHmmss;
import static org.alfasoftware.morf.sql.element.Function.now;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.UPGRADE_STEP_DESCRIPTION_LENGTH;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 *  A helper class to add audit records.
 */
public class AuditRecordHelper {


  private static String serverName;

  // variable to track whether the status, server and processingTimeMs columns have been added
  private static boolean extendedUpgradeAuditTableRowsPresent;

  /*
    //TODO tidy this into proper JavaDocs.


UpgradeAudit Table Schema:

          column("upgradeUUID",      DataType.STRING, 100).primaryKey(),
          column("description",      DataType.STRING, 200).nullable(),
          column("appliedTime",      DataType.DECIMAL, 14).nullable(),
          column("status",           DataType.STRING,  10).nullable(),
          column("server",           DataType.STRING,  100).nullable(),
          column("processingTimeMs", DataType.DECIMAL, 14).nullable()

          Initial insert sets upgradeUUID, description, server, appliedTime, status = SCHEDULED

          Update appliedTime, server status = RUNNING when start running

          On result set STATUS = COMPLETED/FAILED and set processingTimeMs

   */

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
  @Deprecated
  public static void addAuditRecordNoStatus(SchemaChangeVisitor visitor, Schema schema, UUID uuid, String description) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists("UpgradeAudit"))
      return;

    InsertStatement auditRecord = createAuditInsertStatement(uuid, description);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


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

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 6. If we have more than 5 columns
      // we can assume that we have the extended fields
      if(schema.getTable("UpgradeAudit").columns().size() > 5) {
        extendedUpgradeAuditTableRowsPresent = true;
      } else {
        // If we don't have the expanded fields we can no-op as UpgradeAudit records are only inserted after we
        // have run the upgrade step
        return;
      }
    }



    InsertStatement auditRecord = createExtendedAuditInsertStatement(uuid, description);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


  public static void updateRunningAuditRecord(SchemaChangeVisitor visitor, Schema schema, UUID uuid) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists("UpgradeAudit"))
      return;

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 6. If we have more than 5 columns
      // we can assume that we have the extended fields
      if(schema.getTable("UpgradeAudit").columns().size() > 5) {
        extendedUpgradeAuditTableRowsPresent = true;
      } else {
        // If we don't have the expanded fields we can no-op as UpgradeAudit records are only inserted after we
        // have run the upgrade step
        return;
      }
    }

    UpdateStatement auditRecord = createRunningAuditUpdateStatement(uuid);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


  public static void updateFinishedAuditRecord(SchemaChangeVisitor visitor, Schema schema, UUID uuid, long processingTimeMs, boolean success, String description) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists("UpgradeAudit"))
      return;

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 6. If we have more than 5 columns
      // we can assume that we have the extended fields
      if(schema.getTable("UpgradeAudit").columns().size() > 5) {
        extendedUpgradeAuditTableRowsPresent = true;
        // Just extended so will not have a UpgradeAudit table record. So we need to add one, so it can be upgraded afterwards
        addAuditRecord(visitor, schema, uuid, description);
      } else {
        addAuditRecordNoStatus(visitor, schema, uuid, description);
        return;
      }
    }

    UpdateStatement auditRecord = createFinishedAuditUpdateStatement(uuid, processingTimeMs, success);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


  /**
   * Utility method to get a name for the server that this class is executing on. Value is cached after the first execution
   *
   * @return host name
   */
  private static String getServerName() {
    if (serverName == null) {
      try {
        serverName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        serverName = "unknown server";
      }
    }

    return serverName;
  }




  //          Initial insert sets upgradeUUID, description, server, appliedTime, status = SCHEDULED
  public static InsertStatement createExtendedAuditInsertStatement(UUID uuid, String description) {
    InsertStatement auditRecord = new InsertStatement().into(
            tableRef("UpgradeAudit")).values(
            literal(uuid.toString()).as("upgradeUUID"),
            literal(description.length() > UPGRADE_STEP_DESCRIPTION_LENGTH ? description.substring(0, UPGRADE_STEP_DESCRIPTION_LENGTH) : description).as("description"),
            cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as("appliedTime"),
            literal(UpgradeStepStatus.SCHEDULED.name()).as("status"),
            new FieldLiteral(getServerName()).as("server")
    );
    return auditRecord;
  }

  public static UpdateStatement createRunningAuditUpdateStatement(UUID uuid) {
    UpdateStatement auditRecord = new UpdateStatement(new TableReference(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME))
            .set(cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as("appliedTime"))
            .set(literal(UpgradeStepStatus.RUNNING.name()).as("status"))
            .set(literal(getServerName()).as("server"))
            .where(field("upgradeUUID").eq(uuid.toString()));
    return auditRecord;
  }

  public static UpdateStatement createFinishedAuditUpdateStatement(UUID uuid, long processingTimeMs, boolean success) {
    UpdateStatement auditRecord = new UpdateStatement(new TableReference(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME))
            .set(literal(processingTimeMs).as("processingTimeMs"))
            .set(literal(success ? UpgradeStepStatus.COMPLETED.name() : UpgradeStepStatus.FAILED.name()).as("status"))
            .where(field("upgradeUUID").eq(uuid.toString()));
    return auditRecord;
  }

//  column("status",           DataType.STRING,  10).nullable(),
//  column("server",           DataType.STRING,  100).nullable(),
//  column("processingTimeMs", DataType.DECIMAL, 14).nullable()



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
        new FieldLiteral(description.length() > UPGRADE_STEP_DESCRIPTION_LENGTH ? description.substring(0, UPGRADE_STEP_DESCRIPTION_LENGTH) : description).as("description"),
        cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as("appliedTime")
      );
    return auditRecord;
  }
}
