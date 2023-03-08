package org.alfasoftware.morf.upgrade.upgrade;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

@Sequence(1678215071)
@UUID("baf6d6fd-9c47-4fb4-8e3f-11c81ef800be")
class AddRunTimeMsToUpgradeAuditTable implements UpgradeStep {

    @Override
    public String getJiraId() {
        //TODO put reference in here
        return "MORF-XXX";
    }

    @Override
    public String getDescription() {
        return "Adds runTimeMs column to the upgrade audit table to log the time taken to run the upgrade step";
    }

    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
        schema.addColumn("UpgradeAudit", column("runTimeMs", DataType.DECIMAL, 19).nullable(), new FieldLiteral("1", DataType.DECIMAL));
    }
}
