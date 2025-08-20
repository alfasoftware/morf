package org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("1ade56c0-b1d7-11e2-9e96-080020011112")
public class AddIndex extends AbstractTestUpgradeStep {
  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
    schema.addIndex("BasicTableWithIndex", index("BasicTableWithIndex_1").columns("decimalTenZeroCol"));
  }
}
