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
import static org.alfasoftware.morf.sql.element.Function.clientHost;
import static org.alfasoftware.morf.sql.element.Function.dateToYyyyMMddHHmmss;
import static org.alfasoftware.morf.sql.element.Function.now;
import static org.alfasoftware.morf.sql.element.Function.currentUnixTimeMilliseconds;
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

  private static final String UPGRADE_AUDIT_TABLE_NAME = "UpgradeAudit";
  private static final String UPGRADE_UUID_COLUMN_NAME = "upgradeUUID";
  private static final String APPLIED_TIME_COLUMN_NAME = "appliedTime";
  private static final String STATUS_COLUMN_NAME = "status";
  private static final String SERVER_COLUMN_NAME = "server";

  private static String serverName;

  // variable to track whether the status, server and processingTimeMs columns have been added
  private static boolean extendedUpgradeAuditTableRowsPresent;

  /**
   * Add the audit record, writing out the SQL for the insert.
   * @deprecated
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
    if (!schema.tableExists(UPGRADE_AUDIT_TABLE_NAME))
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
    if (!schema.tableExists(UPGRADE_AUDIT_TABLE_NAME))
      return;

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 6. If we have more than 5 columns
      // we can assume that we have the extended fields
      if(schema.getTable(UPGRADE_AUDIT_TABLE_NAME).columns().size() > 6) {
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


  public static void updateStartedAuditRecord(SchemaChangeVisitor visitor, Schema schema, UUID uuid) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists(UPGRADE_AUDIT_TABLE_NAME))
      return;

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 7. If we have more than 6 columns
      // we can assume that we have the extended fields
      if(schema.getTable(UPGRADE_AUDIT_TABLE_NAME).columns().size() > 6) {
        extendedUpgradeAuditTableRowsPresent = true;
      } else {
        // If we don't have the expanded fields we can no-op as UpgradeAudit records are only inserted after we
        // have run the upgrade step
        return;
      }
    }

    UpdateStatement auditRecord = createStartedAuditUpdateStatement(uuid);

    visitor.visit(new ExecuteStatement(auditRecord));
  }


  public static void updateFinishedAuditRecord(SchemaChangeVisitor visitor, Schema schema, UUID uuid, String description) {
    // There's no point adding an UpgradeAudit row if the table isn't there.
    if (!schema.tableExists(UPGRADE_AUDIT_TABLE_NAME))
      return;

    if ( ! extendedUpgradeAuditTableRowsPresent ) {
      // adding the extended tables expanded the table from 3 columns to 7. If we have more than 6 columns
      // we can assume that we have the extended fields
      if(schema.getTable(UPGRADE_AUDIT_TABLE_NAME).columns().size() > 6) {
        extendedUpgradeAuditTableRowsPresent = true;
        // Just extended so will not have a UpgradeAudit table record. So we need to add one, so it can be upgraded afterwards
        addAuditRecord(visitor, schema, uuid, description);
      } else {
        addAuditRecordNoStatus(visitor, schema, uuid, description);
        return;
      }
    }

    UpdateStatement auditRecord = createFinishedAuditUpdateStatement(uuid);

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
    return new InsertStatement().into(
            tableRef(UPGRADE_AUDIT_TABLE_NAME)).values(
            literal(uuid.toString()).as(UPGRADE_UUID_COLUMN_NAME),
            literal(description.length() > UPGRADE_STEP_DESCRIPTION_LENGTH ? description.substring(0, UPGRADE_STEP_DESCRIPTION_LENGTH) : description).as("description"),
            cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as(APPLIED_TIME_COLUMN_NAME),
            literal(UpgradeStepStatus.SCHEDULED.name()).as(STATUS_COLUMN_NAME),
            new FieldLiteral(getServerName()).as(SERVER_COLUMN_NAME)
    );
  }

  public static UpdateStatement createStartedAuditUpdateStatement(UUID uuid) {
    return new UpdateStatement(new TableReference(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME))
            .set(cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as(APPLIED_TIME_COLUMN_NAME))
            .set(literal(UpgradeStepStatus.STARTED.name()).as(STATUS_COLUMN_NAME))
            .set(clientHost().as(SERVER_COLUMN_NAME))
            .set(currentUnixTimeMilliseconds().as("startTimeMs"))
            .where(field(UPGRADE_UUID_COLUMN_NAME).eq(uuid.toString()));
  }

  public static UpdateStatement createFinishedAuditUpdateStatement(UUID uuid) {
    return new UpdateStatement(new TableReference(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME))
            .set(currentUnixTimeMilliseconds().minus(field("startTimeMs")).as("processingTimeMs"))
            .set(literal( UpgradeStepStatus.COMPLETED.name()).as(STATUS_COLUMN_NAME))
            .where(field(UPGRADE_UUID_COLUMN_NAME).eq(uuid.toString()));
  }


  /**
   * Returns an {@link InsertStatement} used to be added to the upgrade audit table.
   *
   * @param uuid The UUID of the step which has been applied
   * @param description The description of the step.
   * @return The insert statement
   */
  public static InsertStatement createAuditInsertStatement(UUID uuid, String description) {

    return new InsertStatement().into(
      new TableReference(UPGRADE_AUDIT_TABLE_NAME)).values(
        new FieldLiteral(uuid.toString()).as(UPGRADE_UUID_COLUMN_NAME),
        new FieldLiteral(description.length() > UPGRADE_STEP_DESCRIPTION_LENGTH ? description.substring(0, UPGRADE_STEP_DESCRIPTION_LENGTH) : description).as("description"),
        cast(dateToYyyyMMddHHmmss(now())).asType(DataType.DECIMAL, 14).as(APPLIED_TIME_COLUMN_NAME)
      );
  }
}
