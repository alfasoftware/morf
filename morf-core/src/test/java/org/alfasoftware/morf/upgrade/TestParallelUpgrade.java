package org.alfasoftware.morf.upgrade;

import java.util.List;

import org.alfasoftware.morf.upgrade.ParallelUpgrade.UpgradeNode;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestParallelUpgrade {

  @Test
  public void testGraphBuilding() {
    //given
    ParallelUpgrade parallelUpgrade = new ParallelUpgrade(null, null, null, null);
    List<UpgradeStep> upgradesToApply = Lists.newArrayList(
      new UpgradeStep10(),
      new UpgradeStep13(),
      new UpgradeStep12(),
      new UpgradeStep11(),
      new UpgradeStep1(),
      new UpgradeStep2(),
      new UpgradeStep5(),
      new UpgradeStep4(),
      new UpgradeStep3(),
      new UpgradeStep9(),
      new UpgradeStep7(),
      new UpgradeStep8(),
      new UpgradeStep6());

    //when
    List<UpgradeNode> nodes = parallelUpgrade.produceNodes(upgradesToApply);
    parallelUpgrade.prepareGraph(nodes);

    //then


  }
}

@Sequence(1)
@UpgradeModifies("t1")
class UpgradeStep1 implements UpgradeStep {

  @Override
  public String getJiraId() {
    return null;
  }

  @Override
  public String getDescription() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {
  }
}

@Sequence(2)
@UpgradeModifies("t1")
class UpgradeStep2 extends UpgradeStep1{}

@Sequence(3)
@UpgradeReads("t1")
class UpgradeStep3 extends UpgradeStep1{}

@Sequence(4)
@UpgradeReads("t1")
class UpgradeStep4 extends UpgradeStep1{}

@Sequence(5)
@UpgradeModifies("t2")
class UpgradeStep5 extends UpgradeStep1{}

@Sequence(6)
@UpgradeModifies({"t1", "t2"})
class UpgradeStep6 extends UpgradeStep1{}

@Sequence(7)
@UpgradeReads({"t1", "t2"})
class UpgradeStep7 extends UpgradeStep1{}

@Sequence(8)
@UpgradeReads("t2")
class UpgradeStep8 extends UpgradeStep1{}

@Sequence(9)
@UpgradeModifies("t1")
@UpgradeReads("t2")
class UpgradeStep9 extends UpgradeStep1{}

@Sequence(10)
class UpgradeStep10 extends UpgradeStep1{}

@Sequence(11)
class UpgradeStep11 extends UpgradeStep1{}

@Sequence(12)
@UpgradeModifies("t3")
class UpgradeStep12 extends UpgradeStep1{}

@Sequence(13)
@UpgradeReads("t4")
class UpgradeStep13 extends UpgradeStep1{}
