package org.alfasoftware.morf.upgrade;


import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link UpgradeScriptAdditionsProvider}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
public class TestUpgradeScriptAdditionsProvider {

  /**
   * Tests that all script additions are returned when filtering is disabled.
   */
  @Test
  public void testNoFiltering() {
      //Given
      Set<UpgradeScriptAddition> upgradeScriptAdditions = allScriptAdditions();

      //When
      UpgradeScriptAdditionsProvider upgradeScriptAdditionsProvider = new UpgradeScriptAdditionsProvider.FilteredScriptAdditions(upgradeScriptAdditions);

      //Then
      Set<UpgradeScriptAddition> filteredResult =  upgradeScriptAdditionsProvider.getUpgradeScriptAdditions();
      assertEquals(3, filteredResult.size());
  }

    @Test
    public void testFilterOneAnnotation() {
        //Given
        Set<UpgradeScriptAddition> upgradeScriptAdditions = allScriptAdditions();

        //When
        UpgradeScriptAdditionsProvider upgradeScriptAdditionsProvider = new UpgradeScriptAdditionsProvider.FilteredScriptAdditions(upgradeScriptAdditions);
        List<Class<? extends Annotation>> annotationsForExclusion = new ArrayList<>();
        annotationsForExclusion.add(DoNotInclude.class);
        upgradeScriptAdditionsProvider.excludeAnnotatedBy(annotationsForExclusion);

        //Then
        Set<UpgradeScriptAddition> filteredResult =  upgradeScriptAdditionsProvider.getUpgradeScriptAdditions();
        assertEquals(2, filteredResult.size());
    }


    @Test
    public void testFilterByTwoAnnotations() {
        //Given
        Set<UpgradeScriptAddition> upgradeScriptAdditions = allScriptAdditions();

        //When
        UpgradeScriptAdditionsProvider upgradeScriptAdditionsProvider = new UpgradeScriptAdditionsProvider.FilteredScriptAdditions(upgradeScriptAdditions);
        List<Class<? extends Annotation>> annotationsForExclusion = new ArrayList<>();
        annotationsForExclusion.add(DoNotIncludeAgain.class);
        annotationsForExclusion.add(DoNotInclude.class);
        upgradeScriptAdditionsProvider.excludeAnnotatedBy(annotationsForExclusion);

        //Then
        Set<UpgradeScriptAddition> filteredResult =  upgradeScriptAdditionsProvider.getUpgradeScriptAdditions();
        assertEquals(1, filteredResult.size());
        UpgradeScriptAddition scriptAddition = filteredResult.iterator().next();
        assertEquals(scriptAddition.getClass(), ScriptAddition2.class);
    }



  private Set<UpgradeScriptAddition> allScriptAdditions() {
      Set<UpgradeScriptAddition> upgradeScriptAdditions = new HashSet<>();
      upgradeScriptAdditions.add(new ScriptAddition1());
      upgradeScriptAdditions.add(new ScriptAddition2());
      upgradeScriptAdditions.add(new ScriptAddition3());
      return upgradeScriptAdditions;
  }

  @DoNotInclude
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

  @DoNotIncludeAgain
  public static class ScriptAddition3 implements UpgradeScriptAddition {

    @Override
    public Iterable<String> sql(ConnectionResources connectionResources) {
            return null;
        }
  }

  @Retention(RetentionPolicy.RUNTIME)
   public @interface DoNotInclude {
  }

 @Retention(RetentionPolicy.RUNTIME)
   public @interface DoNotIncludeAgain {
 }
}