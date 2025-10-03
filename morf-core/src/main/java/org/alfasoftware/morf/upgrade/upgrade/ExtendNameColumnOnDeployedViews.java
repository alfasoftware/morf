package org.alfasoftware.morf.upgrade.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;

@Version("2.17.0")
@Sequence(1718032504)
@UUID("618eb92e-d3f7-4807-92ae-aa6eeeccdcee")
class ExtendNameColumnOnDeployedViews implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "MORF-98";
  }


  @Override
  public String getDescription() {
    return "Extends the name column to allow for longer view names.";
  }


  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.changeColumn("DeployedViews", column("name", DataType.STRING, 30).primaryKey(), column("name", DataType.STRING, 100).primaryKey());
  }
}
