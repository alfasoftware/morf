package org.alfasoftware.morf.upgrade.upgrade;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.UpgradeStepStatus;
import org.alfasoftware.morf.upgrade.Version;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.element.Criterion.isNull;

@Version("2.5.2")
@Sequence(1686844860)
@UUID("47832d23-f1e1-422f-b6de-b76e57517334")
public class AddExtraLoggingToUpgradeAuditTable implements UpgradeStep {

    private final String statusColumn = "status";
    private final String serverColumn = "server";
    private final String processingTimeMsColumn = "processingTimeMs";


    @Override
    public String getJiraId() {
        return "MORF-72";
    }

    @Override
    public String getDescription() {
        return "Add extra logging columns to the UpgradeAudit table";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {

        schema.addColumn(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME,
                column(statusColumn, DataType.STRING, 10).nullable());
        schema.addColumn(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME,
                column(serverColumn, DataType.STRING, 100).nullable());
        schema.addColumn(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME,
                column(processingTimeMsColumn, DataType.DECIMAL, 14).nullable());

        data.executeStatement(
                new UpdateStatement(new TableReference(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME))
                        .set(new FieldLiteral(UpgradeStepStatus.COMPLETED.name()).as(statusColumn))
                        .where(isNull(field(statusColumn)))
        );

    }
}
