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

package org.alfasoftware.morf.guicesupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.mockito.internal.progress.ThreadSafeMockingProgress;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * JUnit {@link MethodRule} which creates a Guice injector with the supplied modules.
 *
 * <p>If the test class using this rule implements {@link Module} it too will be used to create the injector.</p>
 *
 * <p>{@link Injector#injectMembers(Object)} will be called on the test itself, so the test may inject
 * dependencies.</p>
 *
 * <pre><code>public class TestMyClass {
 *
 *   &#064;Rule
 *   public final InjectMembersRule setupInjector = new InjectMembersRule(new MyModule());
 *
 *   &#064;Inject
 *   private MyTestDependency dependency;
 *
 *
 *   &#064;Test
 *   public void testMyMethod() {
 *     ...
 *   }
 * }</code></pre>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class InjectMembersRule implements MethodRule {

  /**
   * The modules for the guice injector.
   */
  private final Module[] modules;

  /**
   * @param modules modules for the guice injector.
   */
  public InjectMembersRule(final Module... modules) {
    this.modules = modules;
  }


  /**
   * @see org.junit.rules.MethodRule#apply(org.junit.runners.model.Statement, org.junit.runners.model.FrameworkMethod, java.lang.Object)
   */
  @Override
  public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        final List<Module> moduleWithTarget = new ArrayList<>(Arrays.asList(modules));
        if (target instanceof Module) {
          moduleWithTarget.add((Module) target);
        }
        Guice.createInjector(moduleWithTarget).injectMembers(target);
        try {
          base.evaluate();
        } finally {
          new ThreadSafeMockingProgress().reset();
        }
      }
    };
  }
}
