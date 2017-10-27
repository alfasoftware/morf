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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor;

/**
 * Use to log notifications received about SQL execution.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class LoggingSqlScriptVisitor implements SqlScriptVisitor {
  private final Logger log = LoggerFactory.getLogger(LoggingSqlScriptVisitor.class);

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionStart()
   */
  @Override
  public void executionStart() {
    log.info("Starting SQL Script");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#beforeExecute(java.lang.String)
   */
  @Override
  public void beforeExecute(String sql) {
    log.info("Executing [" + sql + "]");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#afterExecute(java.lang.String, long)
   */
  @Override
  public void afterExecute(String sql, long numberOfRowsUpdated) {
    log.info("Completed [" + sql + "] with [" + numberOfRowsUpdated + "] rows updated");
  }

  /**
   * {@inheritDoc}
   * @see org.alfasoftware.morf.jdbc.SqlScriptExecutor.SqlScriptVisitor#executionEnd()
   */
  @Override
  public void executionEnd() {
    log.info("SQL Script Complete");
  }

}