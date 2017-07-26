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

package org.alfasoftware.morf.jdbc;

import java.util.Set;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.collect.ImmutableSet;

/**
 * JUnit {@link TestRule} which allows for a specified list of tests to be
 * ignored.
 *
 * <p>This rule is required to allow database dialects to be partially supported,
 * e.g. DB2/400 while still allowing projects to develop and add support for
 * such databases where appropriate.</p>
 *
 * <p><strong>THIS SHOULD NOT BE USED</strong> in any core supported dialect. The
 * only reason this is used by the DB2/400 dialect is that there is some
 * (incomplete) support which should be tested where it exists.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class UnsupportedDatabaseTestRule implements TestRule {

  private final Set<String> unsupportedTests;


  /**
   * Creates a new {@link UnsupportedDatabaseTestRule} based on the specified
   * named test methods.
   *
   * @param unsupportedTests The test method names
   */
  public UnsupportedDatabaseTestRule(final Set<String> unsupportedTests) {
    super();
    this.unsupportedTests = unsupportedTests;
  }


  /**
   * Creates a new {@link UnsupportedDatabaseTestRule} based on the specified
   * named test methods.
   * @param unsupportedTests The test method names
   */
  public UnsupportedDatabaseTestRule(String... unsupportedTests) {
    this(ImmutableSet.copyOf(unsupportedTests));
  }


  /**
   * @see org.junit.rules.TestRule#apply(org.junit.runners.model.Statement,
   *      org.junit.runner.Description)
   */
  @Override
  public Statement apply(Statement base, Description description) {
    return unsupportedTests.contains(description.getMethodName()) ? new UnsupportedDatabaseTestStatement(description) : base;
  }
}
