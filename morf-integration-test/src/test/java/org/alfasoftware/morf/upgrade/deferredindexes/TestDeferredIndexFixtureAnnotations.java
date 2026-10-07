/* Copyright 2026 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.upgrade.deferredindexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.alfasoftware.morf.upgrade.UUID;
import org.alfasoftware.morf.upgrade.UpgradeGraph;
import org.alfasoftware.morf.upgrade.UpgradeStep;
import org.junit.Test;

import com.google.common.reflect.ClassPath;

/**
 * Guards the deferred-index integration fixtures against duplicate {@code @Sequence}
 * and {@code @UUID} values.
 *
 * <p>A duplicate is invisible until someone writes a test that happens to combine the
 * two clashing steps in one {@code performUpgradeSteps(...)} call. It then surfaces as
 * an {@code IllegalStateException} from {@link UpgradeGraph} that reads like a product
 * defect rather than a fixture-authoring mistake, in a test that has nothing to do with
 * either step. Catching it here keeps the failure where the cause is.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class TestDeferredIndexFixtureAnnotations {

  private static final String FIXTURE_PACKAGE =
      "org.alfasoftware.morf.upgrade.deferredindexes.upgrade";


  /**
   * Every fixture must be usable alongside every other. {@link UpgradeGraph} is Morf's
   * own validator for that, so building one over the whole fixture set is the same check
   * a real upgrade performs.
   */
  @Test
  public void testAllFixturesCanBeCombinedInOneUpgradeGraph() throws IOException {
    // given
    List<Class<? extends UpgradeStep>> fixtures = fixtureSteps();
    assertFalse("Fixture scan found nothing -- the package name is probably stale: "
        + FIXTURE_PACKAGE, fixtures.isEmpty());

    // when / then
    try {
      new UpgradeGraph(fixtures);
    } catch (IllegalStateException e) {
      fail("The deferred-index fixtures cannot all be used in one upgrade. Any test "
          + "combining the clashing steps will fail with a message that looks like a "
          + "product bug. Underlying error: " + e.getMessage());
    }
  }


  /** No two fixtures may declare the same {@code @UUID}. */
  @Test
  public void testAllFixtureUuidsAreUnique() throws IOException {
    // given
    List<Class<? extends UpgradeStep>> fixtures = fixtureSteps();

    // when
    Map<String, List<String>> byUuid = fixtures.stream().collect(Collectors.groupingBy(
        c -> c.getAnnotation(UUID.class).value(),
        Collectors.mapping(Class::getSimpleName, Collectors.toList())));

    // then
    List<String> clashes = byUuid.entrySet().stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> e.getKey() + " -> " + e.getValue())
        .collect(Collectors.toList());
    assertEquals("Fixtures sharing a @UUID: " + clashes, List.of(), clashes);
  }


  /**
   * Collects every concrete {@link UpgradeStep} under the fixture package. Scans rather
   * than hardcoding a list so a newly-added fixture is covered without anyone
   * remembering to register it here.
   */
  private List<Class<? extends UpgradeStep>> fixtureSteps() throws IOException {
    ClassLoader loader = getClass().getClassLoader();
    List<Class<? extends UpgradeStep>> steps = new ArrayList<>();
    for (ClassPath.ClassInfo info :
        ClassPath.from(loader).getTopLevelClassesRecursive(FIXTURE_PACKAGE)) {
      Class<?> c = info.load();
      if (UpgradeStep.class.isAssignableFrom(c)
          && !Modifier.isAbstract(c.getModifiers())
          && !c.isInterface()) {
        steps.add(c.asSubclass(UpgradeStep.class));
      }
    }
    steps.sort(java.util.Comparator.comparing((Function<Class<?>, String>) Class::getSimpleName));
    return steps;
  }
}
