package org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0;

import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.UpdateStatement.update;

import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

@Sequence(5)
@UUID("00000000-0000-0000-0000-012345678902")
public class UpdateField extends AbstractTestUpgradeStep {

  public static boolean testingUseCtasDuringUpgrade = false;

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {

    UpdateStatement updateStatement = update(tableRef("WithDefaultValue"))
      .set(literal("NEW").as("anotherValue"))
      .useCtasDuringUpgrade(testingUseCtasDuringUpgrade)
      .build();

    data.executeStatement(updateStatement);
  }
}