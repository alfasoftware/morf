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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assume;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;


/**
 * {@link Statement} implementation which allows tests to be ignored if they
 * are not supported by a particular database platform.
 *
 * <p>This will use {@link Assume} to mark tests as passed.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
class UnsupportedDatabaseTestStatement extends Statement {

  private static final Log  log = LogFactory.getLog(UnsupportedDatabaseTestStatement.class);

  private final Description description;


  /**
   * Create a new {@link UnsupportedDatabaseTestStatement} which will prevent
   * the test specified in the {@link Description} provided from running.
   *
   * @param description The {@link Description} of the test.
   */
  UnsupportedDatabaseTestStatement(Description description) {
    super();
    this.description = description;
  }


  /**
   * @see org.junit.runners.model.Statement#evaluate()
   */
  @Override
  public void evaluate() {
    String message = String.format("Test %s ignored by %s as it is marked as unsupported", description.getMethodName(),
      description.getTestClass().getSimpleName());
    log.info(message);
    Assume.assumeTrue(message, false);
  }
}
