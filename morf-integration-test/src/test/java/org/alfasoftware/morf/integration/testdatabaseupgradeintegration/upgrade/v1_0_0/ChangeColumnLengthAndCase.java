package org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(1)
@UUID("17e17988-6f8d-41e0-8f56-db6c6876bf3c")
public class ChangeColumnLengthAndCase extends AbstractTestUpgradeStep  {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      schema.changeColumn("BasicTable",
        column("decimalNineFiveCol", DataType.DECIMAL, 9, 5),
        column("decimalninefivecol", DataType.DECIMAL, 10, 6));
    }
  }