/* Copyright 2017 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.SelectStatement;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

/**
 * Test the dependency analysis done by {@link ViewChanges}
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TestViewChanges {

  private static final Function<View, String> TO_NAME = new Function<View, String> () {
    @Override public String apply(View view) {
      return view.getName();
    }
  };


  /**
   * Test DSL correctly specifies dependencies on generated views.
   */
  @Test
  public void testSchemaUtils() {
    assertEquals(ImmutableSet.of("B", "C"), ImmutableSet.copyOf(view("A", "B", "C").getDependencies()));
  }


  /**
   * Test a simple configuration.
   */
  @Test
  public void testSimple() {
    ViewChanges c = new ViewChanges(
      ImmutableSet.of(
        view("A", "B", "C"),
        view("B", "C", "D"),
        view("C"),
        view("D")
      ),
      ImmutableSet.of(
        view("B")
      ),
      ImmutableSet.of(
        view("A"),
        view("B")
      ));

    assertEquals("Views to drop mismatch", ImmutableSet.of("B", "A"), nameSet(c.getViewsToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("A", "B"), nameSet(c.getViewsToDeploy()));

    Ordering<String> dropOrder = Ordering.explicit(nameList(c.getViewsToDrop()));
    assertTrue("Must drop A before B", dropOrder.compare("A", "B") < 0);

    Ordering<String> deployOrder = Ordering.explicit(nameList(c.getViewsToDeploy()));
    assertTrue("Must deploy B before A", deployOrder.compare("B", "A") < 0);
  }


  /**
   * Test we're still prepared to drop views that we know nothing of.
   */
  @Test
  public void testObsoleted() {
    ViewChanges c = new ViewChanges(
      ImmutableSet.of(
        view("A", "B"),
        view("B", "C"),
        view("C")
      ),
      ImmutableSet.of(
        view("B"),
        view("X")
      ),
      ImmutableSet.of(
        view("A"),
        view("B")
      ));

    assertEquals("Views to drop mismatch", ImmutableSet.of("A", "B", "X"), nameSet(c.getViewsToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("A", "B"), nameSet(c.getViewsToDeploy()));

    Ordering<String> dropOrder = Ordering.explicit(nameList(c.getViewsToDrop()));
    assertTrue("Must drop A before B", dropOrder.compare("A", "B") < 0);

    Ordering<String> deployOrder = Ordering.explicit(nameList(c.getViewsToDeploy()));
    assertTrue("Must deploy B before A", deployOrder.compare("B", "A") < 0);
  }


  /**
   * Test cycles are detected.
   */
  @Test(expected=IllegalStateException.class)
  public void testCycle() {
    new ViewChanges(
      ImmutableSet.of(
        view("A", "B"),
        view("B", "A")
      ),
      ImmutableSet.of(
        view("B")
      ),
      ImmutableSet.of(
        view("A")
      ));
  }


  /**
   * Ensure that case-infidelity of discovered views doesn't break us.
   */
  @Test
  public void testCaseCorrectionOfDropSet() {
    ViewChanges c = new ViewChanges(
      ImmutableSet.of(
        view("A", "B"),
        view("B")
      ),
      ImmutableSet.of(
        view("b")
      ),
      ImmutableSet.of(
        view("A"),
        view("B")
      ));

    assertEquals("Views to drop mismatch", ImmutableSet.of("A", "B"), nameSet(c.getViewsToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("A", "B"), nameSet(c.getViewsToDeploy()));

    Ordering<String> dropOrder = Ordering.explicit(nameList(c.getViewsToDrop()));
    assertTrue("Must drop A before B", dropOrder.compare("A", "B") < 0);

    Ordering<String> deployOrder = Ordering.explicit(nameList(c.getViewsToDeploy()));
    assertTrue("Must deploy B before A", deployOrder.compare("B", "A") < 0);
  }


  /**
   * Check we can expand the set of drops and deploys.
   */
  @Test
  public void testDroppingAlsoDeployingAlso() {
    ViewChanges c = new ViewChanges(
      ImmutableSet.of(
        view("A", "B"),
        view("B"),
        view("D")
      ),
      ImmutableSet.of(
        view("B")
      ),
      ImmutableSet.of(
        view("A"),
        view("B")
      )).droppingAlso(ImmutableSet.of(view("C"))).deployingAlso(ImmutableSet.of(view("D")));

    assertEquals("Views to drop mismatch", ImmutableSet.of("A", "B", "C"), nameSet(c.getViewsToDrop()));
    assertEquals("Views to deploy mismatch", ImmutableSet.of("A", "B", "D"), nameSet(c.getViewsToDeploy()));

    Ordering<String> dropOrder = Ordering.explicit(nameList(c.getViewsToDrop()));
    assertTrue("Must drop A before B", dropOrder.compare("A", "B") < 0);

    Ordering<String> deployOrder = Ordering.explicit(nameList(c.getViewsToDeploy()));
    assertTrue("Must deploy B before A", deployOrder.compare("B", "A") < 0);
  }


  /**
   * Create a view with dependencies.
   *
   * @param name view name.
   * @param deps view dependencies.
   * @return named view with dependencies.
   */
  private View view(String name, String... deps) {
    return SchemaUtils.view(name, new SelectStatement(), deps);
  }


  /**
   * Convenience function - view collection to set of names.
   * @param views collection of views.
   * @return set of names of views.
   */
  private Set<String> nameSet(Collection<View> views) {
    return ImmutableSet.copyOf(Collections2.transform(views, TO_NAME));
  }


  /**
   * Convenience function - view collection to list of names (preserving order).
   * @param views collection of views (assumed to be ordered)
   * @return list of names of views.
   */
  private List<String> nameList(Collection<View> views) {
    return ImmutableList.copyOf(Collections2.transform(views, TO_NAME));
  }
}
