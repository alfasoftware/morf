package org.alfasoftware.morf.upgrade.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.alfasoftware.morf.upgrade.Version;

@Version("1.4.0")
@Sequence(1625753324)
@UUID("188df754-1526-4200-9e69-69d6a95c1879")
class AddDeployedViewsSqlDefinition implements UpgradeStep {

  @Override
  public String getJiraId() {
    return "WEB-122709";
  }


  @Override
  public String getDescription() {
    return "Add view definitions to deployed views table";
  }


  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addColumn("DeployedViews",
      column("sqlDefinition", DataType.CLOB).nullable());
  }
}
