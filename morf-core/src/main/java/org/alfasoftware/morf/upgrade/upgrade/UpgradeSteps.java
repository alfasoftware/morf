package org.alfasoftware.morf.upgrade.upgrade;

import java.util.List;

import org.alfasoftware.morf.upgrade.UpgradeStep;

import com.google.common.collect.ImmutableList;

public class UpgradeSteps {

  public static final List<Class<? extends UpgradeStep>> LIST = ImmutableList.of(
    AddDeployedViewsSqlDefinition.class,
    AddRunTimeMsToUpgradeAuditTable.class
  );
}
