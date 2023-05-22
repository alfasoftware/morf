package org.alfasoftware.morf.upgrade;


import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link UpgradeScriptAdditionsProvider}'s default and NoOp implementations.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
public class TestUpgradeScriptAdditionsProvider {

  /**
   * Tests that no filtering is applied by default.
   */
  @Test
  public void testNoFiltering() {
      //Given
      Set<UpgradeScriptAddition> upgradeScriptAdditions = allScriptAdditions();

      //When
      UpgradeScriptAdditionsProvider upgradeScriptAdditionsProvider = new UpgradeScriptAdditionsProvider.DefaultScriptAdditions(
              upgradeScriptAdditions);

      //Then
      Set<UpgradeScriptAddition> filteredResult =  upgradeScriptAdditionsProvider.getUpgradeScriptAdditions();
      assertEquals("Expecting all 3 elements", 3, filteredResult.size());
  }

  /**
   * Tests that NoOp implementation returns and empty set.
   */
  @Test
  public void testNoOp() {
      //Given

      //When
      UpgradeScriptAdditionsProvider upgradeScriptAdditionsProvider = new UpgradeScriptAdditionsProvider.NoOpScriptAdditions();

      //Then
      Set<UpgradeScriptAddition> result =  upgradeScriptAdditionsProvider.getUpgradeScriptAdditions();
      assertEquals("Expecting empty set", 0, result.size());
  }


  private Set<UpgradeScriptAddition> allScriptAdditions() {
      Set<UpgradeScriptAddition> upgradeScriptAdditions = new HashSet<>();
      upgradeScriptAdditions.add(new ScriptAddition1());
      upgradeScriptAdditions.add(new ScriptAddition2());
      upgradeScriptAdditions.add(new ScriptAddition3());
      return upgradeScriptAdditions;
  }


  public static class ScriptAddition1 implements UpgradeScriptAddition {
    @Override
    public Iterable<String> sql(ConnectionResources connectionResources) {
          return null;
      }
  }

  public static class ScriptAddition2 implements UpgradeScriptAddition {

    @Override
    public Iterable<String> sql(ConnectionResources connectionResources) {
            return null;
        }
  }

  public static class ScriptAddition3 implements UpgradeScriptAddition {

    @Override
    public Iterable<String> sql(ConnectionResources connectionResources) {
            return null;
        }
  }
}